package master

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/slothhcommits/Hive-FS/internal/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type logRec = *messages.Master_OperationRec

func formatHandle(h uint64) string { return strconv.FormatUint(h, 10) }

func cloneHandles(h []uint64) []uint64 {
	return slices.Clone(h)
}

func newLogRec(typ operationType, args ...string) logRec {
	return &messages.Master_OperationRec{OpType: uint32(typ), Args: args}
}

func logRecs(r ...logRec) []logRec { return r }

func joinOps(parts ...[]logRec) []logRec {
	var out []logRec
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func appendChunkOps(path string, currLen int, handles ...uint64) []logRec {
	out := make([]logRec, 0, len(handles))
	for i, h := range handles {
		out = append(out, newLogRec(APPEND_CHUNK, path, formatHandle(h), strconv.FormatUint(uint64(currLen + i), 10)))
	}
	return out
}

// CREATE_FILE followed by one APPEND_CHUNK per handle
func createFileOps(path string, handles ...uint64) []logRec {
	return joinOps(logRecs(newLogRec(CREATE_FILE, path)), appendChunkOps(path, 0, handles...))
}

func snapshotOfFiles(m map[string][]uint64) stateSnapshot {
	ns := map[string]nodeSnap{}
	for p, h := range m {
		ns[p] = nodeSnap{chunkHandles: h}
	}
	return stateSnapshot{namespace: ns}
}

// raw copy of the persisted state; slice headers are shared with the live state,
// exactly like snapshotState does
func rawSnapshot(s *persistentState) stateSnapshot {

	raw := stateSnapshot{
		chunkHandle: s.chunkHandle.Load(),
		namespace:   map[string]nodeSnap{},
		deleted:     map[string]deletedSnap{},
		chunkTable:  map[uint64]chunkSnap{},
	}

	s.namespace.mu.RLock()
	for k, n := range s.namespace.entries {
		raw.namespace[k] = nodeSnap{dir: n.dir, chunkHandles: n.chunkHandles}
	}
	s.namespace.mu.RUnlock()

	s.deletedMu.Lock()
	for k, d := range s.namespace.deleted {
		ds := deletedSnap{deletedAt: d.deletedAt}
		if d.node != nil {
			ds.dir = d.node.dir
			ds.chunkHandles = d.node.chunkHandles
		}
		raw.deleted[k] = ds
	}
	s.deletedMu.Unlock()

	s.chunkTable.mu.RLock()
	for k, c := range s.chunkTable.entries {
		raw.chunkTable[k] = chunkSnap{ref: c.ref, version: c.version, repFactor: c.repFactor}
	}
	s.chunkTable.mu.RUnlock()

	return raw
}

// deep copy with no shared slices; nil and empty slices are treated the same
func normalizeSnapshot(s stateSnapshot) stateSnapshot {

	out := stateSnapshot{
		chunkHandle: s.chunkHandle,
		namespace:   map[string]nodeSnap{},
		deleted:     map[string]deletedSnap{},
		chunkTable:  map[uint64]chunkSnap{},
	}
	for k, n := range s.namespace {
		out.namespace[k] = nodeSnap{dir: n.dir, chunkHandles: cloneHandles(n.chunkHandles)}
	}
	for k, d := range s.deleted {
		out.deleted[k] = deletedSnap{deletedAt: d.deletedAt, dir: d.dir, chunkHandles: cloneHandles(d.chunkHandles)}
	}
	for k, c := range s.chunkTable {
		out.chunkTable[k] = c
	}
	return out
}

func captureState(s *persistentState) stateSnapshot { return normalizeSnapshot(rawSnapshot(s)) }

// builds a state whose slices are the snapshot's own slices (no copy)
func stateFromSnapshot(snap stateSnapshot, dir string) *persistentState {

	s := newPersistentState(dir)
	s.chunkHandle.Store(snap.chunkHandle)

	for k, n := range snap.namespace {
		s.namespace.entries[k] = &node{dir: n.dir, metadata: metadata{chunkHandles: n.chunkHandles}}
	}
	for k, d := range snap.deleted {
		s.namespace.deleted[k] = &deletedNode{
			deletedAt: d.deletedAt,
			node:      &node{dir: d.dir, metadata: metadata{chunkHandles: d.chunkHandles}},
		}
	}
	for k, c := range snap.chunkTable {
		s.chunkTable.entries[k] = &chunk{
			id:        k,
			ref:       c.ref,
			version:   c.version,
			repFactor: c.repFactor,
			locations: make(map[string]time.Time),
		}
	}
	return s
}

func requireStateEqual(t *testing.T, label string, want, got stateSnapshot) {
	t.Helper()
	require.Equal(t, normalizeSnapshot(want), normalizeSnapshot(got), "%s: state mismatch", label)
}

func encodeLogFrame(ops ...logRec) []byte {

	payload, err := proto.Marshal(&messages.Master_Log{Recs: ops})
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, crc32.Checksum(payload, castagnoliTable))
	binary.Write(&buf, binary.BigEndian, uint32(len(payload)))
	buf.Write(payload)
	return buf.Bytes()
}

func logFrameSize(ops []logRec) int64 {
	return int64(8 + proto.Size(&messages.Master_Log{Recs: ops}))
}

func writeTestFile(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0644))
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, 10*time.Second, 5*time.Millisecond, "timed out waiting for %s", path)

	// the flush goroutine still syncs the directory after the rename
	time.Sleep(100 * time.Millisecond)
}

func flushCheckpoint(t *testing.T, dir string, handle uint64, snap stateSnapshot) string {
	t.Helper()
	src := newPersistentState(dir)
	src.flushState(strconv.FormatUint(handle, 10), &snap)
	return filepath.Join(dir, formatHandle(handle)+".cp")
}

func loadCheckpointFile(t *testing.T, path string) *persistentState {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)

	s := newPersistentState(t.TempDir())
	s.loadCheckpoint(f)
	require.NoError(t, f.Close())
	return s
}

// commits ops as one transaction; exec applies the same ops to the live state
func commitOps(s *persistentState, ops ...logRec) {
	tx := newTransaction()
	for _, o := range ops {
		tx.addOperation(&operation{t: operationType(o.OpType), args: o.Args})
		tx.addFunc(func() { s.applyOp(o) })
	}
	s.commitTransaction(tx)
}

// writes ops to the log only, never applies them
func logOpsOnly(s *persistentState, ops ...logRec) {
	tx := newTransaction()
	for _, o := range ops {
		tx.addOperation(&operation{t: operationType(o.OpType), args: o.Args})
	}
	s.logger.log(tx)
}

func newStateWithOpenLog(t *testing.T) (*persistentState, string) {
	t.Helper()
	dir := t.TempDir()
	s := newPersistentState(dir)
	s.logger.open()
	t.Cleanup(func() { s.logger.logFile.Close() })
	return s, dir
}

func recoverState(dir string) *persistentState {
	s := newPersistentState(dir)
	s.buildState()
	return s
}

func sampleSnapshot() stateSnapshot {
	return stateSnapshot{
		chunkHandle: 42,
		namespace: map[string]nodeSnap{
			"/dir":    {dir: true},
			"/dir/f1": {chunkHandles: []uint64{1, 2, 3}},
			"/f2":     {},
		},
		deleted: map[string]deletedSnap{
			"/old/100":    {deletedAt: 100, chunkHandles: []uint64{7, 8}},
			"/olddir/200": {deletedAt: 200, dir: true},
		},
		chunkTable: map[uint64]chunkSnap{
			1: {ref: 1, version: 2, repFactor: 3},
			2: {ref: 2, version: 5, repFactor: 3},
		},
	}
}

// ---------------------------------------------------------------------------
// 2. loadCheckpoint
// ---------------------------------------------------------------------------

func TestLoadCheckpoint(t *testing.T) {

	snap := sampleSnapshot()
	path := flushCheckpoint(t, t.TempDir(), 7, snap)

	got := loadCheckpointFile(t, path)

	requireStateEqual(t, "loaded checkpoint", snap, captureState(got))
	assert.Equal(t, snap.chunkHandle, got.chunkHandle.Load(), "chunk handle")
}

// ---------------------------------------------------------------------------
// 4. flushState
// ---------------------------------------------------------------------------

func TestFlushState(t *testing.T) {

	withoutNamespace := sampleSnapshot()
	withoutNamespace.namespace = map[string]nodeSnap{}

	withoutDeleted := sampleSnapshot()
	withoutDeleted.deleted = map[string]deletedSnap{}

	withoutChunkTable := sampleSnapshot()
	withoutChunkTable.chunkTable = map[uint64]chunkSnap{}

	cases := []struct {
		name string
		snap stateSnapshot
	}{
		{"full", sampleSnapshot()},
		{"empty namespace", withoutNamespace},
		{"empty deleted", withoutDeleted},
		{"empty chunktable", withoutChunkTable},
		{"all empty", stateSnapshot{chunkHandle: 5}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := flushCheckpoint(t, t.TempDir(), 3, tc.snap)
			got := loadCheckpointFile(t, path)
			requireStateEqual(t, "flushed and loaded", tc.snap, captureState(got))
		})
	}
}

// ---------------------------------------------------------------------------
// 5. opLog.log()
// ---------------------------------------------------------------------------

func TestOpLogLog(t *testing.T) {

	dir := t.TempDir()
	l := newOpLog(dir)
	l.open()
	t.Cleanup(func() { l.logFile.Close() })

	batches := [][]*operation{
		{
			{t: CREATE_FILE, args: []string{"/a"}},
			{t: APPEND_CHUNK, args: []string{"/a", "1"}},
		},
		{
			{t: MOVE, args: []string{"/a", "/b"}},
		},
		{
			{t: REMOVE, args: []string{"/b", "100"}},
			{t: CREATE_DIR, args: []string{"/d"}},
			{t: SNAPSHOT, args: []string{"/x", "/y"}},
		},
	}

	for _, b := range batches {
		tx := newTransaction()
		for _, o := range b {
			tx.addOperation(o)
		}
		l.log(tx)
	}

	data, err := os.ReadFile(filepath.Join(dir, "1.log"))
	require.NoError(t, err)

	off := 0
	for i, b := range batches {

		require.GreaterOrEqual(t, len(data)-off, 8, "batch %d: header truncated", i)

		sum := binary.BigEndian.Uint32(data[off : off+4])
		size := int(binary.BigEndian.Uint32(data[off+4 : off+8]))
		require.GreaterOrEqual(t, len(data)-off-8, size, "batch %d: payload truncated", i)

		payload := data[off+8 : off+8+size]
		require.Equal(t, crc32.Checksum(payload, castagnoliTable), sum, "batch %d: bad checksum", i)

		var got messages.Master_Log
		require.NoError(t, proto.Unmarshal(payload, &got), "batch %d", i)
		require.Len(t, got.Recs, len(b), "batch %d: rec count", i)

		for j, o := range b {
			require.Equal(t, uint32(o.t), got.Recs[j].OpType, "batch %d rec %d: op type", i, j)
			require.Equal(t, o.args, got.Recs[j].Args, "batch %d rec %d: args", i, j)
		}

		off += 8 + size
	}

	assert.Equal(t, len(data), off, "trailing bytes in log")
}

// ---------------------------------------------------------------------------
// 6. commitTransaction
// ---------------------------------------------------------------------------

func TestCommitTransaction_ConcurrentBatches(t *testing.T) {

	oldMax := maxLogSize
	maxLogSize = 8 * 1024
	t.Cleanup(func() { maxLogSize = oldMax })

	s, dir := newStateWithOpenLog(t)

	// initial state X
	want := snapshotOfFiles(map[string][]uint64{})
	written := int64(0)
	for i := 0; i < 5; i++ {
		p := fmt.Sprintf("/x%d", i)
		ops := createFileOps(p, uint64(100+i))
		commitOps(s, ops...)
		written += logFrameSize(ops)
		want.namespace[p] = nodeSnap{chunkHandles: []uint64{uint64(100 + i)}}
	}

	// disjoint batches: each creates its own file; the first five also append to their own x file
	buildBatch := func(i int) (ops []logRec, path string, hs []uint64) {
		path = fmt.Sprintf("/n%d", i)
		hs = make([]uint64, 8)
		for j := range hs {
			hs[j] = uint64(1000 + i*10 + j)
		}
		ops = createFileOps(path, hs...)
		if i < 5 {
			ops = append(ops, appendChunkOps(fmt.Sprintf("/x%d", i), len(want.namespace[fmt.Sprintf("/x%d", i)].chunkHandles), uint64(500+i))...)
		}
		return
	}

	sample, _, _ := buildBatch(100)
	unit := logFrameSize(sample)
	// one wave is about half a log, so a wave rotates at most once
	batchesPerWave := max(1, int(maxLogSize/2/unit))

	next := 0
	for written < 3*maxLogSize {

		h0 := s.logger.handle.Load()

		var wg sync.WaitGroup
		for k := 0; k < batchesPerWave; k++ {

			ops, p, hs := buildBatch(next)
			want.namespace[p] = nodeSnap{chunkHandles: hs}
			if next < 5 {
				want.namespace[fmt.Sprintf("/x%d", next)] = nodeSnap{chunkHandles: []uint64{uint64(100 + next), uint64(500 + next)}}
			}
			written += logFrameSize(ops)
			next++

			wg.Add(1)
			go func() {
				defer wg.Done()
				commitOps(s, ops...)
			}()
		}
		wg.Wait()

		// checkpoint flushes all share temp.cp, so let each finish before the next wave
		h1 := s.logger.handle.Load()
		for h := h0 + 1; h <= h1; h++ {
			waitForFile(t, filepath.Join(dir, formatHandle(h)+".cp"))
		}
	}

	require.GreaterOrEqual(t, s.logger.handle.Load(), uint64(3), "expected at least 2 log rotations")

	requireStateEqual(t, "live state", want, captureState(s))
	requireStateEqual(t, "state rebuilt from disk", want, captureState(recoverState(dir)))
}

var (
	batchA = createFileOps("/a", 1, 2)
	batchB = createFileOps("/b", 3)
	batchC = createFileOps("/c", 4, 5)
)

func wantStateAB() stateSnapshot {
	return snapshotOfFiles(map[string][]uint64{"/a": {1, 2}, "/b": {3}})
}

func wantStateABC() stateSnapshot {
	return snapshotOfFiles(map[string][]uint64{"/a": {1, 2}, "/b": {3}, "/c": {4, 5}})
}

func TestCommitTransaction_Breakpoint_MidTransaction(t *testing.T) {

	t.Run("logged but not applied", func(t *testing.T) {
		s, dir := newStateWithOpenLog(t)
		commitOps(s, batchA...)
		commitOps(s, batchB...)
		logOpsOnly(s, batchC...)

		requireStateEqual(t, "live state", wantStateAB(), captureState(s))
		requireStateEqual(t, "recovered", wantStateABC(), captureState(recoverState(dir)))
	})

	t.Run("record partially written", func(t *testing.T) {
		s, dir := newStateWithOpenLog(t)
		commitOps(s, batchA...)
		commitOps(s, batchB...)

		path := filepath.Join(dir, "1.log")
		st, err := os.Stat(path)
		require.NoError(t, err)
		validLen := st.Size()

		torn := encodeLogFrame(batchC...)
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		_, err = f.Write(torn[:len(torn)-3])
		require.NoError(t, err)
		require.NoError(t, f.Close())

		requireStateEqual(t, "recovered", wantStateAB(), captureState(recoverState(dir)))

		st, err = os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, validLen, st.Size(), "log not truncated to last valid record")
	})
}

func TestCommitTransaction_Breakpoint_MidRotation(t *testing.T) {

	s, dir := newStateWithOpenLog(t)
	commitOps(s, batchA...)
	commitOps(s, batchB...)

	// new log exists, checkpoint was never written
	h := s.logger.newHandle()
	s.logger.rotateLog(h)
	commitOps(s, batchC...)

	requireStateEqual(t, "live state", wantStateABC(), captureState(s))
	requireStateEqual(t, "recovered", wantStateABC(), captureState(recoverState(dir)))
}

func TestCommitTransaction_Breakpoint_MidCheckpoint(t *testing.T) {

	t.Run("checkpoint complete, old log not yet cleaned", func(t *testing.T) {
		s, dir := newStateWithOpenLog(t)
		commitOps(s, batchA...)
		commitOps(s, batchB...)

		h := s.logger.newHandle()
		s.flushState(strconv.FormatUint(h, 10), s.snapshotState())
		waitForFile(t, filepath.Join(dir, formatHandle(h)+".cp"))
		s.logger.rotateLog(h)
		commitOps(s, batchC...)

		requireStateEqual(t, "recovered", wantStateABC(), captureState(recoverState(dir)))

		assert.NoFileExists(t, filepath.Join(dir, "1.log"), "log older than the checkpoint should be removed")
	})

	t.Run("checkpoint flush died partway", func(t *testing.T) {
		s, dir := newStateWithOpenLog(t)
		commitOps(s, batchA...)
		commitOps(s, batchB...)

		h := s.logger.newHandle()
		s.logger.rotateLog(h)
		commitOps(s, batchC...)

		full := flushCheckpoint(t, t.TempDir(), h, rawSnapshot(s))
		data, err := os.ReadFile(full)
		require.NoError(t, err)
		writeTestFile(t, dir, "temp.cp", data[:len(data)/2])

		requireStateEqual(t, "recovered", wantStateABC(), captureState(recoverState(dir)))
	})
}

// ---------------------------------------------------------------------------
// 7. buildState
// ---------------------------------------------------------------------------

func TestBuildState(t *testing.T) {

	torn := encodeLogFrame(newLogRec(CREATE_FILE, "/torn"))

	badCRC := slices.Clone(torn)
	badCRC[0] ^= 0xFF

	cases := []struct {
		name string
		tail []byte
	}{
		{"torn inside length field", torn[:6]},
		{"torn inside payload", torn[:8+(len(torn)-8)/2]},
		{"bad crc record", badCRC},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			dir := t.TempDir()
			src := newPersistentState(dir)

			// checkpoints: 5 holds /old, 9 holds /new
			src.flushState(strconv.FormatUint(5, 10), &stateSnapshot{chunkHandle: 11, namespace: map[string]nodeSnap{"/old": {}}})
			src.flushState(strconv.FormatUint(9, 10), &stateSnapshot{chunkHandle: 42, namespace: map[string]nodeSnap{"/new": {}}})

			// logs 5 to 8 are before the cutoff, 9 and 10 are at or after it
			stale := encodeLogFrame(newLogRec(CREATE_FILE, "/stale"))
			for n := uint64(5); n <= 8; n++ {
				writeTestFile(t, dir, formatHandle(n)+".log", stale)
			}
			fresh := encodeLogFrame(newLogRec(CREATE_FILE, "/fresh"))
			writeTestFile(t, dir, "9.log", fresh)
			writeTestFile(t, dir, "10.log", append(slices.Clone(fresh), tc.tail...))

			// garbage files whose content would add /garbage if wrongly read
			garbageLog := encodeLogFrame(newLogRec(CREATE_FILE, "/garbage"))
			for _, name := range []string{"abc.log", "-1.log", "10.logx", "9.log.bak", "7.txt"} {
				writeTestFile(t, dir, name, garbageLog)
			}
			garbageCheckpoint := stateSnapshot{chunkHandle: 999, namespace: map[string]nodeSnap{"/garbage": {}}}
			for i, name := range []string{"xyz.cp", "-3.cp", "temp.cp"} {
				h := uint64(70 + i)
				src.flushState(strconv.FormatUint(h, 10), &garbageCheckpoint)
				require.NoError(t, os.Rename(filepath.Join(dir, formatHandle(h)+".cp"), filepath.Join(dir, name)))
			}

			got := recoverState(dir)

			want := stateSnapshot{
				chunkHandle: 42,
				namespace:   map[string]nodeSnap{"/new": {}, "/fresh": {}},
			}
			requireStateEqual(t, "built state", want, captureState(got))

			for _, name := range []string{"5.cp", "5.log", "6.log", "7.log", "8.log"} {
				assert.NoFileExists(t, filepath.Join(dir, name), "%s should have been removed", name)
			}

			st, err := os.Stat(filepath.Join(dir, "10.log"))
			require.NoError(t, err)
			assert.Equal(t, int64(len(fresh)), st.Size(), "last log not truncated to last valid record")
		})
	}
}
