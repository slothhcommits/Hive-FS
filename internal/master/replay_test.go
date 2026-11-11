package master

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nsTestEntry struct {
	path string
	node *node
}

type deletedTestEntry struct {
	node *deletedNode
	path string
}

type chunkTestEntry struct {
	id    uint64
	chunk *chunk
}

func newChunkTestEntry(t *testing.T, id uint64, ref uint32, version uint32, rep uint32) *chunkTestEntry {
	t.Helper()
	return &chunkTestEntry{
		id: id,
		chunk: &chunk{
			id:        id,
			version:   version,
			ref:       ref,
			repFactor: rep,
		},
	}
}

func seedNamespace(t *testing.T, state *persistentState, nsEntries []*nsTestEntry, deletedEntries []*deletedTestEntry) {
	t.Helper()
	state.namespace.mu.Lock()
	for _, e := range nsEntries {
		state.namespace.entries[e.path] = e.node
	}
	state.namespace.mu.Unlock()

	state.namespace.deletedMu.Lock()
	for _, e := range deletedEntries {
		state.namespace.deleted[state.namespace.makeDeletedPath(e.path, e.node.deletedAt)] = e.node
	}
	state.namespace.deletedMu.Unlock()
}

func seedChunkTable(t *testing.T, state *persistentState, entries []*chunkTestEntry) {
	t.Helper()
	state.chunkTable.mu.Lock()
	for _, e := range entries {
		state.chunkTable.entries[e.id] = e.chunk
	}
	state.chunkTable.mu.Unlock()
}

func sampleChunkHandles(t *testing.T, seed uint64, n int) []uint64 {

	t.Helper()

	handles := make([]uint64, n)
	for i := range handles {
		handles[i] = uint64(100+i) + seed
	}

	return handles
}

func sampleState(t *testing.T, dir string, chunkHandle uint64) *persistentState {

	t.Helper()

	extraHandles := sampleChunkHandles(t, 0, 100)

	nsEntries := []*nsTestEntry{
		// directories
		{path: "/", node: &node{dir: true}},
		{path: "/a", node: &node{dir: true}},
		{path: "/a/b", node: &node{dir: true}},
		{path: "/x", node: &node{dir: true}},
		{path: "/x/y", node: &node{dir: true}},
		{path: "/x/y/z", node: &node{dir: true}},
		{path: "/x/empty", node: &node{dir: true}},

		// files
		{path: "/a/b/c", node: &node{dir: false}}, // no handles
		{path: "/a/b/d", node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{64, 65, 66, 67, 68}}}},
		{path: "/a/e", node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{69}}}},                // single handle
		{path: "/a/f", node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{64, 65}}}},            // shares handles with /a/b/d
		{path: "/x/y/z/deep", node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{70, 71, 72}}}}, // deep path
		{path: "/top", node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{73}}}},                // directly under root
		{path: "/big", node: &node{dir: false, metadata: metadata{chunkHandles: extraHandles}}},                // many handles
	}

	deletedEntries := []*deletedTestEntry{
		{path: "/a/b", node: &deletedNode{deletedAt: 1000, node: &node{dir: true}}},
		{path: "/a/b/c", node: &deletedNode{deletedAt: 1000, node: &node{dir: false}}},
		{path: "/a/b/d", node: &deletedNode{deletedAt: 1000, node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{64, 65, 66, 67, 68}}}}}, // same handles as the live /a/b/d
		{path: "/old/dir", node: &deletedNode{deletedAt: 2000, node: &node{dir: true}}},
		{path: "/old/dir/f", node: &deletedNode{deletedAt: 2000, node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{80, 81}}}}},
		{path: "/a/b/d", node: &deletedNode{deletedAt: 3000, node: &node{dir: false, metadata: metadata{chunkHandles: []uint64{90}}}}}, // same path deleted twice
		{path: "/gone", node: &deletedNode{deletedAt: 4000, node: &node{dir: false}}},
	}

	chunkEntries := []*chunkTestEntry{
		newChunkTestEntry(t, 64, 3, 4, 3), // live /a/b/d, live /a/f, deleted /a/b/d
		newChunkTestEntry(t, 65, 3, 4, 3),
		newChunkTestEntry(t, 66, 2, 2, 3),
		newChunkTestEntry(t, 67, 2, 2, 3),
		newChunkTestEntry(t, 68, 2, 1, 3),
		newChunkTestEntry(t, 69, 1, 7, 5), // different replication factor
		newChunkTestEntry(t, 70, 1, 1, 3),
		newChunkTestEntry(t, 71, 1, 1, 3),
		newChunkTestEntry(t, 72, 1, 1, 1),
		newChunkTestEntry(t, 73, 1, 9, 3),
		newChunkTestEntry(t, 80, 1, 1, 3), // only referenced by a deleted file
		newChunkTestEntry(t, 81, 1, 1, 3),
		newChunkTestEntry(t, 90, 1, 2, 3),
		newChunkTestEntry(t, 95, 0, 1, 3), // unreferenced, waiting for the garbage collector
	}
	for _, h := range extraHandles {
		chunkEntries = append(chunkEntries, newChunkTestEntry(t, h, 1, 1, 3))
	}

	state := newPersistentState(dir)

	state.chunkHandle.Store(chunkHandle)

	seedNamespace(t, state, nsEntries, deletedEntries)
	seedChunkTable(t, state, chunkEntries)

	return state
}

func applyTx(logger *opLog, tx *transaction) {
	logger.log(tx)
	for _, ex := range tx.exec {
		ex()
	}
}

func step(t *testing.T, state *persistentState, f func(tx *transaction), steps *[]string) {
	t.Helper()
	tx := newTransaction()
	f(tx)
	applyTx(state.logger, tx)
	name := fmt.Sprintf("step%02d", len(*steps))
	state.flushState(name, state.snapshotState())
	*steps = append(*steps, name)
}

func fileNode(t *testing.T, state *persistentState, path string) *node {
	t.Helper()
	n, ok := state.namespace.entries[path]
	require.True(t, ok, path)
	return n
}

func create(t *testing.T, state *persistentState, path string) func(tx *transaction) {
	t.Helper()
	return func(tx *transaction) { require.NoError(t, state.namespace.create(tx, path)) }
}

func appendAll(t *testing.T, state *persistentState, path string, hs ...uint64) func(tx *transaction) {

	for _, h := range hs[:len(hs)-1] {
		tx := newTransaction()
		fileNode(t, state, path).appendChunkHandle(tx, path, h)
		applyTx(state.logger, tx)
	}

	return func(tx *transaction) {
		fileNode(t, state, path).appendChunkHandle(tx, path, hs[len(hs)-1])
	}
}

func move(t *testing.T, state *persistentState, src, dst string) func(tx *transaction) {
	t.Helper()
	return func(tx *transaction) { require.NoError(t, state.namespace.move(tx, src, dst)) }
}

func remove(t *testing.T, state *persistentState, path string) func(tx *transaction) {
	t.Helper()
	return func(tx *transaction) { require.NoError(t, state.namespace.delete(tx, path)) }
}

func snapshot(t *testing.T, state *persistentState, src, dst string) func(tx *transaction) {
	t.Helper()
	return func(tx *transaction) {
		_, err := state.namespace.snapshot(tx, src, dst)
		require.NoError(t, err)
	}
}

func deleteBatch(t *testing.T, state *persistentState, path string, processed int) func(tx *transaction) {
	t.Helper()
	return func(tx *transaction) {

		d := state.namespace.deleted[path]
		length := len(d.chunkHandles)

		tx.addOperation(&operation{
			t:    DELETE,
			args: []string{path, strconv.Itoa(length), strconv.Itoa(processed)},
		})

		tx.addFunc(func() {
			if processed >= len(d.chunkHandles) {
				delete(state.namespace.deleted, path)
			} else {
				d.chunkHandles = d.chunkHandles[processed:]
			}
		})
	}
}

func addChunks(chunks []*chunkTestEntry, state *persistentState) func(tx *transaction) {
	return func(tx *transaction) {
		for _, e := range chunks {
			state.chunkTable.addChunk(tx, e.chunk)
		}
	}
}

func replayAssertCheckpoint(t *testing.T, cpFile *os.File, expected *persistentState) {

	t.Helper()

	cp := newPersistentState(t.TempDir())
	cp.loadCheckpoint(cpFile)

	_, err := expected.logger.logFile.Seek(0, io.SeekStart)
	require.NoError(t, err)

	cp.replayLog(expected.logger.logFile)

	assert.Equal(t, expected.namespace.entries, cp.namespace.entries, "replayed namespace does not match expected namespace")
	assert.Equal(t, expected.namespace.deleted, cp.namespace.deleted, "replayed deleted entry in namespace does not match expected namespace")
	assert.Equal(t, len(expected.chunkTable.entries), len(cp.chunkTable.entries))

	for h, c := range expected.chunkTable.entries {
		replayedC, ok := cp.chunkTable.entries[h]
		require.True(t, ok)
		assert.Equal(t, replayedC.id, c.id)
		assert.Equal(t, replayedC.ref, c.ref)
		assert.Equal(t, replayedC.repFactor, c.repFactor)
		assert.Equal(t, replayedC.version, c.version)
	}
}

func TestMySnapshotState(t *testing.T) {

	dir := t.TempDir()
	testHandle := uint64(007)
	state := sampleState(t, dir, testHandle)

	snapshot := state.snapshotState()

	assert.Equal(t, state.chunkHandle.Load(), snapshot.chunkHandle)

	assert.Equal(t, len(state.namespace.entries), len(snapshot.namespace))
	assert.Equal(t, len(state.namespace.deleted), len(snapshot.deleted))
	assert.Equal(t, len(state.chunkTable.entries), len(snapshot.chunkTable))

	for k, n := range state.namespace.entries {
		nodeSnap, ok := snapshot.namespace[k]
		require.True(t, ok, "namespace entry : %s not found in snapshot\n", k)
		assert.Equal(t, nodeSnap.dir, n.dir)
		assert.Equal(t, nodeSnap.chunkHandles, n.chunkHandles)

		beforeMut := cloneHandles(nodeSnap.chunkHandles)

		tx := newTransaction()

		if !n.dir && len(n.chunkHandles) != 0 {
			n.replaceChunkHandle(tx, "/a/b/d", 0, n.chunkHandles[0], 700)
		}

		n.appendChunkHandle(tx, "/a/b/d", testHandle)

		for _, fn := range tx.exec {
			fn()
		}

		assert.Equal(t, nodeSnap.chunkHandles, beforeMut)

		n.chunkHandles = n.chunkHandles[:len(n.chunkHandles)-1] // revert append
	}

	for k, n := range state.namespace.deleted {
		deletedSnap, ok := snapshot.deleted[k]
		require.True(t, ok, "deleted entry : %s not found in snapshot\n", k)
		assert.Equal(t, deletedSnap.dir, n.dir)
		assert.Equal(t, deletedSnap.deletedAt, n.deletedAt)
		assert.Equal(t, deletedSnap.chunkHandles, n.chunkHandles)
	}

	for handle, chunk := range state.chunkTable.entries {
		chunkSnap, ok := snapshot.chunkTable[handle]
		require.True(t, ok, "chunk handle entry : %d not found in snapshot\n", handle)
		assert.Equal(t, chunkSnap.ref, chunk.ref)
		assert.Equal(t, chunkSnap.version, chunk.version)
		assert.Equal(t, chunkSnap.repFactor, chunk.repFactor)
	}
}

func TestLogReplay_PointerBased(t *testing.T) {

	t.Run("create-snapshot-append", func(t *testing.T) {

		dir := t.TempDir()
		base := sampleState(t, dir, 1008)
		base.logger.open()

		testHandle := uint64(108)

		filePath := "/test/file"
		snapPath := "/test/snap"

		steps := []string{}

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		// append /test/file testHandle+1
		step(t, base, appendAll(t, base, filePath, testHandle+1), &steps)

		// snapshot /test/file /test/snap
		step(t, base, snapshot(t, base, filePath, snapPath), &steps)

		// append /test/file testhandle
		step(t, base, appendAll(t, base, filePath, testHandle), &steps)

		require.Equal(t, 4, len(steps))

		for _, name := range steps {
			f, err := os.Open(filepath.Join(dir, name+".cp"))
			require.NoError(t, err)
			replayAssertCheckpoint(t, f, base)
			require.NoError(t, f.Close())
		}
	})

	t.Run("create-snapshot-replace", func(t *testing.T) {

		dir := t.TempDir()

		base := sampleState(t, dir, 1008)
		base.logger.open()

		testHandle := uint64(108)

		filePath := "/test/file"
		snapPath := "/test/snap"

		var steps []string

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		// append /test/file testHandle, testHandle+1
		step(t, base, appendAll(t, base, filePath, testHandle, testHandle+1), &steps)

		// snapshot /test/file /test/snap
		step(t, base, snapshot(t, base, filePath, snapPath), &steps)

		// replace /test/file 1 testhandle+1 testhandle+2
		step(t, base, func(tx *transaction) {
			fileNode(t, base, filePath).replaceChunkHandle(tx, filePath, 1, testHandle+1, testHandle+2)
		}, &steps)

		require.Equal(t, 4, len(steps))

		for _, name := range steps {
			f, err := os.Open(filepath.Join(dir, name+".cp"))
			require.NoError(t, err)
			replayAssertCheckpoint(t, f, base)
			require.NoError(t, f.Close())
		}
	})

	t.Run("create-snapshot-remove-create", func(t *testing.T) {

		dir := t.TempDir()

		base := sampleState(t, dir, 1008)
		base.logger.open()

		testHandle := uint64(108)

		filePath := "/test/file"
		snapPath := "/test/snap"

		steps := []string{}

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		// append /test/file testHandle
		step(t, base, appendAll(t, base, filePath, testHandle, testHandle+1), &steps)

		// snapshot /test/file /test/snap
		step(t, base, snapshot(t, base, filePath, snapPath), &steps)

		// remove /test/file
		step(t, base, remove(t, base, filePath), &steps)

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		require.Equal(t, 5, len(steps))

		for _, name := range steps {
			f, err := os.Open(filepath.Join(dir, name+".cp"))
			require.NoError(t, err)
			replayAssertCheckpoint(t, f, base)
			require.NoError(t, f.Close())
		}
	})

	t.Run("create-move-create", func(t *testing.T) {

		dir := t.TempDir()

		base := sampleState(t, dir, 1008)
		base.logger.open()

		testHandle := uint64(108)

		filePath := "/test/file"
		movedPath := "/test/moved"

		steps := []string{}

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		// append /test/file testHandle
		step(t, base, appendAll(t, base, filePath, testHandle), &steps)

		// move /test/file /test/moved
		step(t, base, move(t, base, filePath, movedPath), &steps)

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		require.Equal(t, 4, len(steps))

		for _, name := range steps {
			f, err := os.Open(filepath.Join(dir, name+".cp"))
			require.NoError(t, err)
			replayAssertCheckpoint(t, f, base)
			require.NoError(t, f.Close())
		}
	})

	t.Run("create-remove-create", func(t *testing.T) {

		dir := t.TempDir()

		base := sampleState(t, dir, 1008)
		base.logger.open()

		testHandle := uint64(108)

		filePath := "/test/file"

		steps := []string{}

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		// append /test/file testHandle
		step(t, base, appendAll(t, base, filePath, testHandle, testHandle), &steps)

		// remove /test/file
		step(t, base, remove(t, base, filePath), &steps)

		// create /test/file
		step(t, base, create(t, base, filePath), &steps)

		require.Equal(t, 4, len(steps))

		for _, name := range steps {
			f, err := os.Open(filepath.Join(dir, name+".cp"))
			require.NoError(t, err)
			replayAssertCheckpoint(t, f, base)
			require.NoError(t, f.Close())
		}
	})

	t.Run("append-replace-on-existing-file", func(t *testing.T) {

		dir := t.TempDir()

		base := sampleState(t, dir, 1008)
		base.logger.open()

		filePath := "/a/e" // seeded with [69]
		testHandle := uint64(108)

		steps := []string{}

		// append /test/file testHandle
		step(t, base, appendAll(t, base, filePath, testHandle), &steps)

		// replace /test/file 1 testHandle testHandle+1
		step(t, base, func(tx *transaction) {
			fileNode(t, base, filePath).replaceChunkHandle(tx, filePath, 1, testHandle, testHandle+1)
		}, &steps)

		require.Equal(t, 2, len(steps))

		for _, name := range steps {
			f, err := os.Open(filepath.Join(dir, name+".cp"))
			require.NoError(t, err)
			replayAssertCheckpoint(t, f, base)
			require.NoError(t, f.Close())
		}
	})
}

func TestLogReplay_EveryOp(t *testing.T) {

	dir := t.TempDir()
	base := newPersistentState(dir)
	base.logger.open()

	steps := []string{}

	bigHandles := sampleChunkHandles(t, 0, 100)

	// starting point: empty state
	step(t, base, func(tx *transaction) {}, &steps)

	// CREATE_FILE (also creates /s/a and /s/a/b)
	for _, p := range []string{"/s/a/b/c", "/s/a/b/d", "/s/e", "/s/f", "/s/top", "/s/big"} {
		step(t, base, create(t, base, p), &steps)
	}

	// APPEND_CHUNK
	step(t, base, appendAll(t, base, "/s/a/b/d", 64, 65, 66, 67, 68), &steps)
	step(t, base, appendAll(t, base, "/s/e", 69), &steps)
	step(t, base, appendAll(t, base, "/s/f", 64, 65), &steps)
	step(t, base, appendAll(t, base, "/s/top", 73), &steps)
	step(t, base, appendAll(t, base, "/s/big", bigHandles...), &steps)

	// CREATE_CHUNK
	chunks := []*chunkTestEntry{
		newChunkTestEntry(t, 64, 3, 4, 3),
		newChunkTestEntry(t, 65, 3, 4, 3),
		newChunkTestEntry(t, 66, 2, 2, 3),
		newChunkTestEntry(t, 67, 2, 2, 3),
		newChunkTestEntry(t, 68, 2, 1, 3),
		newChunkTestEntry(t, 69, 1, 7, 5),
		newChunkTestEntry(t, 73, 1, 9, 3),
		newChunkTestEntry(t, 80, 1, 1, 3),
		newChunkTestEntry(t, 95, 0, 1, 3),
	}
	for _, h := range bigHandles {
		chunks = append(chunks, newChunkTestEntry(t, h, 1, 1, 3))
	}

	step(t, base, addChunks(chunks, base), &steps)

	// UPDATE_REF, UPDATE_VERSION, UPDATE_REP
	c64 := base.chunkTable.entries[64]
	step(t, base, func(tx *transaction) { c64.updateRef(tx, 5) }, &steps)
	step(t, base, func(tx *transaction) { c64.updateVersion(tx, 6) }, &steps)
	step(t, base, func(tx *transaction) { c64.updateRep(tx, 4) }, &steps)

	// REPLACE_CHUNK
	step(t, base, func(tx *transaction) {
		fileNode(t, base, "/s/a/b/d").replaceChunkHandle(tx, "/s/a/b/d", 1, 65, 650)
	}, &steps)

	// SNAPSHOT of a file and of a dir, then mutate the sources
	step(t, base, snapshot(t, base, "/s/a/b/d", "/s/snapfile"), &steps)
	step(t, base, snapshot(t, base, "/s/a", "/s/a2"), &steps)
	step(t, base, appendAll(t, base, "/s/a/b/d", 74), &steps)
	step(t, base, func(tx *transaction) {
		fileNode(t, base, "/s/a/b/d").replaceChunkHandle(tx, "/s/a/b/d", 0, 64, 640)
	}, &steps)

	// MOVE: file into dir, rename file, rename dir
	step(t, base, move(t, base, "/s/f", "/s/empty"), &steps)
	step(t, base, move(t, base, "/s/e", "/s/e2"), &steps)
	step(t, base, move(t, base, "/s/a2", "/s/a3"), &steps)

	// REMOVE: file, create it again, whole dir, big file
	step(t, base, remove(t, base, "/s/top"), &steps)
	step(t, base, create(t, base, "/s/top"), &steps)
	step(t, base, remove(t, base, "/s/a3"), &steps)
	step(t, base, remove(t, base, "/s/big"), &steps)

	// DELETE in batches on the removed big file
	key := ""
	for k := range base.namespace.deleted {
		if strings.HasPrefix(k, "/s/big/") {
			key = k
		}
	}
	require.NotEmpty(t, key)

	step(t, base, deleteBatch(t, base, key, 40), &steps)
	step(t, base, deleteBatch(t, base, key, 40), &steps)
	step(t, base, deleteBatch(t, base, key, 20), &steps)

	// DELETE_CHUNK
	step(t, base, func(tx *transaction) {
		base.chunkTable.remove(tx, 95)
		base.chunkTable.remove(tx, 80)
	}, &steps)

	// replay the whole log on every checkpoint
	for _, name := range steps {
		f, err := os.Open(filepath.Join(dir, name+".cp"))
		require.NoError(t, err)
		replayAssertCheckpoint(t, f, base)
		require.NoError(t, f.Close())
	}
}

