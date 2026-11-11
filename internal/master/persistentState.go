package master

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/slothhcommits/Hive-FS/internal/messages"
	"google.golang.org/protobuf/proto"
)

type operationType uint8

const (

	// Namespace mutations
	CREATE_FILE operationType = iota // CREATE_FILE "filename"
	CREATE_DIR                       // CREATE_DIR "dirname"
	MOVE                             // MOVE "src" "dst"
	SNAPSHOT                         // SNAPSHOT "src" "dst"
	REMOVE                           // REMOVE "location" "time"
	DELETE                           // DELETE "deletedKey" "chunkHandlesLength" "processedChunkHandles"

	// Node metadata mutations
	APPEND_CHUNK  // APPEND_CHUNK "path" "chunkhandle" "idx"
	REPLACE_CHUNK // REPLACE_CHUNK "path" "idx" "oldChunkhandle" "newChunkhandle"

	// chunk table mutations
	CREATE_CHUNK // CREATE_CHUNK "chunkhandle" "ref" "version" "rep"
	DELETE_CHUNK // DELETE_CHUNK "chunkhandle"

	// chunk metadata mutations
	UPDATE_REF     // UPDATE_REF "chunkhandle" "ref"
	UPDATE_VERSION // UPDATE_VERSION "chunkhandle" "version"
	UPDATE_REP     // UPDATE_REP "chunkhandle" "rep"
)

type operation struct {
	t    operationType
	args []string
}

type transaction struct {
	operations []*operation
	exec       []func()
	rollbacks  []func()
}

func newTransaction() *transaction {
	return &transaction{
		operations: make([]*operation, 0),
		exec:       make([]func(), 0),
		rollbacks:  make([]func(), 0),
	}
}

func (tx *transaction) addOperation(op *operation) {
	tx.operations = append(tx.operations, op)
}

func (tx *transaction) addFunc(fn func()) {
	tx.exec = append(tx.exec, fn)
}

func (tx *transaction) addRB(fn func()) {
	tx.rollbacks = append(tx.rollbacks, fn)
}

// be careful about the order
func (tx *transaction) rollback() {
	for _, rb := range tx.rollbacks {
		rb()
	}
}

const (
	checkpointDelim uint32 = 0
)

// slice headers are shared with live state; safe because exec only appends or replaces the slice
type nodeSnap struct {
	dir          bool
	chunkHandles []uint64
}

type deletedSnap struct {
	deletedAt    int64
	dir          bool
	chunkHandles []uint64
}

type chunkSnap struct {
	ref       uint32
	version   uint32
	repFactor uint32
}

type stateSnapshot struct {
	chunkHandle uint64
	namespace   map[string]nodeSnap
	deleted     map[string]deletedSnap
	chunkTable  map[uint64]chunkSnap
}

type persistentState struct {
	*namespace
	*chunkTable

	logger      *opLog
	chunkHandle atomic.Uint64

	crc32Table *crc32.Table
}

func newPersistentState(dir string) *persistentState {

	return &persistentState{

		namespace:  newNamespace(),
		chunkTable: newChunkTable(),

		logger: newOpLog(dir),

		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}

}

func (s *persistentState) newChunkHandle() uint64 {
	return s.chunkHandle.Add(1)
}

func (s *persistentState) buildState() {

	hivePath := s.logger.dir

	files, err := os.ReadDir(hivePath)
	if err != nil {
		panic(err)
	}

	var logs []uint64
	var checkpoint uint64 = 0

	var junk []string

	currLogHandle := s.logger.handle.Load()
	for _, file := range files {

		if name, ok := strings.CutSuffix(file.Name(), ".log"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil && n != currLogHandle {
				logs = append(logs, n)
			}
		}

		if name, ok := strings.CutSuffix(file.Name(), ".cp"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil {
				if checkpoint == 0 || checkpoint < n {
					if checkpoint != 0 {
						junk = append(junk, strconv.FormatUint(checkpoint, 10)+".cp")
					}
					checkpoint = n
				} else {
					junk = append(junk, strconv.FormatUint(n, 10)+".cp")
				}
			}
		}
	}

	slices.Sort(logs)

	if checkpoint != 0 {

		filename := strconv.FormatUint(checkpoint, 10) + ".cp"
		cpFile, err := os.Open(filepath.Join(hivePath, filename))
		if err != nil {
			panic(err)
		}

		s.loadCheckpoint(cpFile)
		err = cpFile.Close()
		if err != nil {
			panic(err)
		}

		trimIdx := len(logs)
		for i := range logs {
			if logs[i] >= checkpoint {
				trimIdx = i
				break
			} else {
				junk = append(junk, strconv.FormatUint(logs[i], 10)+".log")
			}
		}
		logs = logs[trimIdx:]
	}

	for i, log := range logs {

		filename := strconv.FormatUint(log, 10) + ".log"
		file, err := os.Open(filepath.Join(hivePath, filename))
		if err != nil {
			panic(err)
		}

		off := s.replayLog(file)
		file.Close()

		if i == len(logs)-1 {
			if err := os.Truncate(filepath.Join(hivePath, filename), off); err != nil {
				panic(err)
			}
		}
	}

	for _, name := range junk {
		os.Remove(filepath.Join(hivePath, name))
	}
}

func (s *persistentState) loadCheckpoint(file *os.File) {

	var handleBuf [8]byte
	_, err := io.ReadFull(file, handleBuf[:8])
	if err != nil && !errors.Is(err, io.EOF) {
		panic(err)
	}

	s.chunkHandle.Store(max(binary.BigEndian.Uint64(handleBuf[:]), s.chunkHandle.Load()))

	s.namespace.mu.Lock()
	for {

		_, buf, err := readFileSegment(file)
		if err != nil {
			panic(err)
		}

		if len(buf) == 0 {
			break
		}

		var rec messages.Master_NamespaceRec
		err = proto.Unmarshal(buf, &rec)
		if err != nil {
			panic(err)
		}

		node := node{}
		node.dir = rec.Dir

		if rec.Metadata != nil {
			node.metadata.chunkHandles = rec.Metadata.ChunkHandles
		}
		s.namespace.entries[rec.Path] = &node

	}
	s.namespace.mu.Unlock()

	s.deletedMu.Lock()
	for {

		_, buf, err := readFileSegment(file)
		if err != nil {
			panic(err)
		}

		if len(buf) == 0 {
			break
		}

		var rec messages.Master_DeletedRec
		err = proto.Unmarshal(buf, &rec)
		if err != nil {
			panic(err)
		}

		s.namespace.deleted[rec.Key] = &deletedNode{
			deletedAt: rec.DeletedAt,
			node: &node{
				dir:      rec.Dir,
				metadata: metadata{chunkHandles: rec.ChunkHandles},
			},
		}

	}
	s.deletedMu.Unlock()

	s.chunkTable.mu.Lock()
	defer s.chunkTable.mu.Unlock()

	for {

		_, buf, err := readFileSegment(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			panic(err)
		}

		var rec messages.Master_ChunktableRec
		err = proto.Unmarshal(buf, &rec)
		if err != nil {
			panic(err)
		}

		if rec.Chunk != nil {
			s.chunkTable.entries[rec.Handle] = &chunk{
				id:        rec.Handle,
				ref:       rec.Chunk.Ref,
				version:   rec.Chunk.Version,
				repFactor: rec.Chunk.RepFactor,
				locations: make(map[string]time.Time),
			}
		}
	}
}

func (s *persistentState) applyOp(op *messages.Master_OperationRec) {

	switch operationType(op.OpType) {

	// namespace
	case CREATE_FILE:
		s.namespace.mu.Lock()
		s.namespace.entries[op.Args[0]] = &node{dir: false}
		s.namespace.mu.Unlock()

	case CREATE_DIR:
		s.namespace.mu.Lock()
		s.namespace.entries[op.Args[0]] = &node{dir: true}
		s.namespace.mu.Unlock()

	case REMOVE:
		s.namespace.mu.Lock()
		s.deletedMu.Lock()
		if s.namespace.entries[op.Args[0]] != nil {
			delTime, err := strconv.ParseInt(op.Args[1], 10, 64)
			if err != nil {
				panic(err)
			}
			s.deleted[op.Args[0]+"/"+op.Args[1]] = &deletedNode{
				deletedAt: delTime,
				node:      s.namespace.entries[op.Args[0]],
			}
			delete(s.namespace.entries, op.Args[0])
		}
		s.deletedMu.Unlock()
		s.namespace.mu.Unlock()

	case DELETE:
		length, err := strconv.Atoi(op.Args[1])
		if err != nil {
			panic(err)
		}
		processed, err := strconv.Atoi(op.Args[2])
		if err != nil {
			panic(err)
		}
		s.deletedMu.Lock()
		// length mismatch means this DELETE was already applied
		if d, ok := s.deleted[op.Args[0]]; ok && len(d.chunkHandles) == length {
			if processed >= len(d.chunkHandles) {
				delete(s.deleted, op.Args[0])
			} else {
				d.chunkHandles = d.chunkHandles[processed:]
			}
		}
		s.deletedMu.Unlock()

	case MOVE:
		s.namespace.mu.Lock()
		if node, ok := s.namespace.entries[op.Args[0]]; ok {
			delete(s.namespace.entries, op.Args[0])
			s.namespace.entries[op.Args[1]] = node
		}
		s.namespace.mu.Unlock()

	case SNAPSHOT:
		s.namespace.mu.Lock()
		if srcNode, ok := s.namespace.entries[op.Args[0]]; ok {
			if _, ok := s.namespace.entries[op.Args[1]]; !ok {
				srcNode.RLock()
				s.namespace.entries[op.Args[1]] = &node{
					dir: srcNode.dir,
					metadata: metadata{
						chunkHandles: slices.Clone(srcNode.chunkHandles),
					},
				}
				srcNode.RUnlock()
			}
		}
		s.namespace.mu.Unlock()

	// node metadata
	case APPEND_CHUNK:
		s.namespace.mu.RLock()
		if node, ok := s.namespace.entries[op.Args[0]]; ok {
			node.Lock()
			handle := parseUint64(op.Args[1])
			idx, err := strconv.Atoi(op.Args[2])
			if err != nil {
				panic(err)
			}
			// len > idx: already applied (checkpoint is ahead), skip
			if len(node.chunkHandles) == idx {
				node.chunkHandles = append(node.chunkHandles, handle)
			}
			// else {
			// 	log.Println("path : ", op.Args[0], "dropped : ", handle, "index : ", idx, " length : ", len(node.chunkHandles))
			// }
			node.Unlock()
		}
		s.namespace.mu.RUnlock()

	case REPLACE_CHUNK:
		s.namespace.mu.RLock()
		if node, ok := s.namespace.entries[op.Args[0]]; ok {

			idx, err := strconv.Atoi(op.Args[1])
			if err != nil {
				panic(err)
			}

			node.Lock()
			if idx < len(node.chunkHandles) {
				oldHandle := parseUint64(op.Args[2])
				newHandle := parseUint64(op.Args[3])
				if node.chunkHandles[idx] == oldHandle {
					node.chunkHandles[idx] = newHandle
				}
			}
			node.Unlock()
		}
		s.namespace.mu.RUnlock()

	// chunktable
	case CREATE_CHUNK:

		handle := parseUint64(op.Args[0])
		ref := parseUint32(op.Args[1])
		version := parseUint32(op.Args[2])
		rep := parseUint32(op.Args[3])

		s.chunkHandle.Store(max(handle, s.chunkHandle.Load()))

		c := chunk{
			id:        handle,
			ref:       ref,
			version:   version,
			repFactor: rep,
			locations: make(map[string]time.Time),
		}
		s.chunkTable.mu.Lock()
		s.chunkTable.entries[handle] = &c
		s.chunkTable.mu.Unlock()

	case DELETE_CHUNK:
		s.chunkTable.mu.Lock()
		delete(s.chunkTable.entries, parseUint64(op.Args[0]))
		s.chunkTable.mu.Unlock()

	// chunk
	case UPDATE_REF:
		handle := parseUint64(op.Args[0])
		ref := parseUint32(op.Args[1])
		s.chunkTable.mu.Lock()
		if c, ok := s.chunkTable.entries[handle]; ok {
			c.Lock()
			c.ref = ref
			c.Unlock()
		}
		s.chunkTable.mu.Unlock()

	case UPDATE_VERSION:
		handle := parseUint64(op.Args[0])
		version := parseUint32(op.Args[1])
		s.chunkTable.mu.Lock()
		if c, ok := s.chunkTable.entries[handle]; ok {
			c.Lock()
			c.version = version
			c.Unlock()
		}
		s.chunkTable.mu.Unlock()

	case UPDATE_REP:
		handle := parseUint64(op.Args[0])
		rep := parseUint32(op.Args[1])
		s.chunkTable.mu.Lock()
		if c, ok := s.chunkTable.entries[handle]; ok {
			c.Lock()
			c.repFactor = rep
			c.Unlock()
		}
		s.chunkTable.mu.Unlock()
	}
}

func (s *persistentState) replayLog(file *os.File) int64 {

	var offset int64
	for {

		var checksumBuf [4]byte
		_, err := io.ReadFull(file, checksumBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset
			}
			panic(err)
		}

		n, buf, err := readFileSegment(file)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset
			}
			panic(err)
		}

		checksum := binary.BigEndian.Uint32(checksumBuf[:4])

		if checksum != s.checkSum(buf) {
			return offset // corrupted write
		}

		var log messages.Master_Log
		err = proto.Unmarshal(buf, &log)
		if err != nil {
			panic(err)
		}

		for _, rec := range log.Recs {
			s.applyOp(rec)
		}

		offset += int64(n + 4)
	}
}

func (s *persistentState) commitTransaction(tx *transaction) {

	s.logger.mu.Lock()
	defer s.logger.mu.Unlock()

	if s.logger.isFull() {
		newLoghandle := s.logger.newHandle()
		snap := s.snapshotState()
		go s.flushState(strconv.FormatUint(newLoghandle, 10), snap)
		s.logger.rotateLog(newLoghandle)
	}

	s.logger.log(tx)

	for _, fn := range tx.exec {
		fn()
	}
}

func (s *persistentState) snapshotState() *stateSnapshot {

	// no node or chunk locks: persisted fields are only mutated in exec, under the log lock held by the caller
	snap := &stateSnapshot{chunkHandle: s.chunkHandle.Load()}

	s.namespace.mu.RLock()
	snap.namespace = make(map[string]nodeSnap, len(s.namespace.entries))
	for k, n := range s.namespace.entries {
		snap.namespace[k] = nodeSnap{dir: n.dir, chunkHandles: n.chunkHandles}
	}
	s.namespace.mu.RUnlock()

	s.deletedMu.Lock()
	snap.deleted = make(map[string]deletedSnap, len(s.namespace.deleted))
	for k, d := range s.namespace.deleted {
		snap.deleted[k] = deletedSnap{deletedAt: d.deletedAt, dir: d.dir, chunkHandles: d.chunkHandles}
	}
	s.deletedMu.Unlock()

	s.chunkTable.mu.RLock()
	snap.chunkTable = make(map[uint64]chunkSnap, len(s.chunkTable.entries))
	for k, c := range s.chunkTable.entries {
		snap.chunkTable[k] = chunkSnap{ref: c.ref, version: c.version, repFactor: c.repFactor}
	}
	s.chunkTable.mu.RUnlock()

	return snap
}

func (s *persistentState) flushState(name string, snap *stateSnapshot) {

	hivePath := s.logger.dir

	file, err := os.Create(filepath.Join(hivePath, "temp.cp"))
	if err != nil {
		panic(err)
	}

	// TODO: checkpoint records aren't checksummed; convert to a streamed protobuf with a CRC trailer
	const maxMem = 1024 * 1024 * 100

	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian, snap.chunkHandle)
	if err != nil {
		panic(err)
	}

	for k, n := range snap.namespace {

		rec, err := proto.Marshal(&messages.Master_NamespaceRec{
			Path: k,
			Dir:  n.dir,
			Metadata: &messages.Master_NamespaceRec_Metadata{
				ChunkHandles: n.chunkHandles,
			},
		})
		if err != nil {
			panic(err)
		}

		err = binary.Write(&buf, binary.BigEndian, uint32(len(rec)))
		if err != nil {
			panic(err)
		}

		_, err = buf.Write(rec)
		if err != nil {
			panic(err)
		}

		if buf.Len() >= maxMem {
			writeFile(buf.Bytes(), file)
			buf.Reset()
		}
	}

	err = binary.Write(&buf, binary.BigEndian, checkpointDelim)
	if err != nil {
		panic(err)
	}

	writeFile(buf.Bytes(), file)
	buf.Reset()

	for k, d := range snap.deleted {

		rec, err := proto.Marshal(&messages.Master_DeletedRec{
			Key:          k,
			DeletedAt:    d.deletedAt,
			Dir:          d.dir,
			ChunkHandles: d.chunkHandles,
		})
		if err != nil {
			panic(err)
		}

		err = binary.Write(&buf, binary.BigEndian, uint32(len(rec)))
		if err != nil {
			panic(err)
		}

		_, err = buf.Write(rec)
		if err != nil {
			panic(err)
		}

		if buf.Len() >= maxMem {
			writeFile(buf.Bytes(), file)
			buf.Reset()
		}
	}

	err = binary.Write(&buf, binary.BigEndian, checkpointDelim)
	if err != nil {
		panic(err)
	}

	writeFile(buf.Bytes(), file)
	buf.Reset()

	for k, c := range snap.chunkTable {

		rec, err := proto.Marshal(&messages.Master_ChunktableRec{
			Handle: k,
			Chunk: &messages.Master_ChunktableRec_Chunk{
				Ref:       c.ref,
				Version:   c.version,
				RepFactor: c.repFactor,
			},
		})
		if err != nil {
			panic(err)
		}

		err = binary.Write(&buf, binary.BigEndian, uint32(len(rec)))
		if err != nil {
			panic(err)
		}

		_, err = buf.Write(rec)
		if err != nil {
			panic(err)
		}

		if buf.Len() >= maxMem {
			writeFile(buf.Bytes(), file)
			buf.Reset()
		}
	}

	writeFile(buf.Bytes(), file)
	buf.Reset()

	if err := file.Sync(); err != nil {
		panic(err)
	}

	if err := file.Close(); err != nil {
		panic(err)
	}

	err = os.Rename(
		file.Name(),
		filepath.Join(hivePath, name+".cp"),
	)
	if err != nil {
		panic(err)
	}

	dir, err := os.Open(hivePath)
	if err != nil {
		panic(err)
	}
	if err := dir.Sync(); err != nil {
		panic(err)
	}
	dir.Close()
}

func (s *persistentState) checkSum(data []byte) uint32 {
	return crc32.Checksum(data, s.crc32Table)
}

func readFileSegment(f *os.File) (int, []byte, error) {

	var lenBuf [4]byte

	var bytesRead int
	n, err := io.ReadFull(f, lenBuf[:4])
	bytesRead += n
	if err != nil {
		return bytesRead, nil, err
	}

	len := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, len)

	n, err = io.ReadFull(f, buf)
	bytesRead += n
	if err != nil {
		return bytesRead, nil, err
	}

	return bytesRead, buf, nil
}

func writeFile(buf []byte, f *os.File) {

	if _, err := f.Write(buf); err != nil {
		panic(err)
	}

	if err := f.Sync(); err != nil {
		panic(err)
	}
}

func parseUint64(s string) uint64 {
	num, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return num
}

func parseUint32(s string) uint32 {
	num, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(num)
}
