package master

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PraveenC2002/gfs/internal/messages"
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
	DELETE                           // DELETE "deletedKey" "processedChunkHandles"

	// Node metadata mutations
	APPEND_CHUNK  // APPEND_CHUNK "path" "chunkhandle"
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
	log       *opLog
	ops       []*operation
	exec      []func()
	rollbacks []func()
}

func (tx *transaction) addOp(op *operation) {
	tx.ops = append(tx.ops, op)
}

func (tx *transaction) addFunc(fn func()) {
	tx.exec = append(tx.exec, fn)
}

func (tx *transaction) addRB(fn func()) {
	tx.rollbacks = append(tx.rollbacks, fn)
}

// be careful about the order
func (tx *transaction) commit() {
	tx.log.logAndApply(tx.ops, tx.exec)
}

// be careful about the order
func (tx *transaction) rollback() {
	for _, rb := range tx.rollbacks {
		rb()
	}
}

const (
	checkpointDelim uint32 = 0
	maxLogSize      int64  = 1024 * 1024 * 10
)

type syncState struct {
	sync.Mutex
	checkpointSig chan uint64
}

type opLog struct {
	*syncState

	dir        string
	handle     atomic.Uint64
	crc32Table *crc32.Table
	logFile    *os.File
}

func newOpLog(dir string) *opLog {
	return &opLog{
		syncState: &syncState{
			checkpointSig: make(chan uint64, 1),
		},
		dir:        dir,
		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}
}

func (l *opLog) newHandle() uint64 {
	return l.handle.Add(1)
}

// open creates the log directory and starts a fresh log after the highest existing log or checkpoint handle
func (l *opLog) open() {

	if err := os.MkdirAll(l.dir, 0755); err != nil {
		panic(err)
	}

	files, err := os.ReadDir(l.dir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {

		if name, ok := strings.CutSuffix(file.Name(), ".log"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil {
				l.handle.Store(max(l.handle.Load(), n))
			}
		}

		if name, ok := strings.CutSuffix(file.Name(), ".cp"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil {
				l.handle.Store(max(l.handle.Load(), n))
			}
		}
	}

	newLogName := strconv.FormatUint(l.newHandle(), 10) + ".log"
	newLog, err := os.Create(filepath.Join(l.dir, newLogName))
	if err != nil {
		panic(err)
	}

	l.logFile = newLog
}

func (l *opLog) checkSum(data []byte) uint32 {
	return crc32.Checksum(data, l.crc32Table)
}

func (l *opLog) logAndApply(operations []*operation, exec []func()) {

	log := messages.Master_Log{Recs: []*messages.Master_OperationRec{}}
	for _, op := range operations {
		rec := messages.Master_OperationRec{
			OpType: uint32(op.t),
			Args:   op.args,
		}
		log.Recs = append(log.Recs, &rec)
	}

	l.Lock()
	if len(log.Recs) > 0 {

		var buf bytes.Buffer

		logBuf, err := proto.Marshal(&log)
		if err != nil {
			panic(err)
		}

		binary.Write(&buf, binary.BigEndian, l.checkSum(logBuf))
		binary.Write(&buf, binary.BigEndian, uint32(len(logBuf)))
		_, err = buf.Write(logBuf)
		if err != nil {
			panic(err)
		}

		info, err := l.logFile.Stat()
		if err != nil {
			panic(err)
		}
		if info.Size() > maxLogSize {
			handle := l.newHandle()
			l.checkpointSig <- handle
			l.rotateLog(handle)
		}

		writeFile(buf.Bytes(), l.logFile)

	}
	
	for _, fn := range exec {
		fn()
	}

	l.Unlock()
}

func (l *opLog) rotateLog(handle uint64) {

	err := l.logFile.Close()
	if err != nil {
		panic(err)
	}

	newLogName := strconv.FormatUint(handle, 10) + ".log"
	newLog, err := os.Create(filepath.Join(l.dir, newLogName))
	if err != nil {
		panic(err)
	}

	l.logFile = newLog
}

type persistentState struct {
	*namespace
	*chunkTable

	log         *opLog
	chunkHandle atomic.Uint64

	crc32Table *crc32.Table
}

func newPersistentState(dir string) *persistentState {

	return &persistentState{

		namespace:  newNamespace(),
		chunkTable: newChunkTable(),

		log: newOpLog(dir),

		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}

}

func (s *persistentState) newChunkHandle() uint64 {
	return s.chunkHandle.Add(1)
}

func (s *persistentState) newTx() *transaction {
	return &transaction{log: s.log}
}

func (s *persistentState) buildState() {

	hivePath := s.log.dir

	files, err := os.ReadDir(hivePath)
	if err != nil {
		panic(err)
	}

	var logs []uint64
	var checkpoint uint64 = 0

	var junk []string

	currLogHandle := s.log.handle.Load()
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

func (s *persistentState) listenCheckpointSig() {

	for handle := range s.log.checkpointSig {
		s.flushState(handle)
	}
}

func (s *persistentState) flushState(handle uint64) {

	hivePath := s.log.dir

	file, err := os.Create(filepath.Join(hivePath, "temp.cp"))
	if err != nil {
		panic(err)
	}

	s.namespace.mu.RLock()
	namespace := maps.Clone(s.namespace.entries)
	s.namespace.mu.RUnlock()

	s.deletedMu.Lock()
	deleted := maps.Clone(s.namespace.deleted)
	s.deletedMu.Unlock()

	s.chunkTable.mu.RLock()
	chunkTable := maps.Clone(s.chunkTable.entries)
	s.chunkTable.mu.RUnlock()

	// TODO: checkpoint records aren't checksummed; convert to a streamed protobuf with a CRC trailer
	const maxMem = 1024 * 1024 * 100

	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian, s.chunkHandle.Load())
	if err != nil {
		panic(err)
	}

	for k, n := range namespace {

		n.RLock()

		chunkHandles := []uint64{}
		for _, h := range n.metadata.chunkHandles {
			chunkHandles = append(chunkHandles, h)
		}

		rec, err := proto.Marshal(&messages.Master_NamespaceRec{
			Path: k,
			Dir:  n.dir,
			Metadata: &messages.Master_NamespaceRec_Metadata{
				ChunkHandles: chunkHandles,
			},
		})
		if err != nil {
			panic(err)
		}
		n.RUnlock()

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

	for k, d := range deleted {

		d.RLock()

		chunkHandles := []uint64{}
		chunkHandles = append(chunkHandles, d.metadata.chunkHandles...)

		rec, err := proto.Marshal(&messages.Master_DeletedRec{
			Key:          k,
			DeletedAt:    d.deletedAt,
			Dir:          d.dir,
			ChunkHandles: chunkHandles,
		})
		if err != nil {
			panic(err)
		}
		d.RUnlock()

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

	for k, c := range chunkTable {

		c.Lock()

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
		c.Unlock()

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
	if err := file.Close(); err != nil {
		panic(err)
	}

	err = os.Rename(
		file.Name(),
		filepath.Join(hivePath, strconv.FormatUint(handle, 10)+".cp"),
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
		processed, err := strconv.Atoi(op.Args[1])
		if err != nil {
			panic(err)
		}
		s.deletedMu.Lock()
		if d, ok := s.deleted[op.Args[0]]; ok {
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
			if !slices.Contains(node.chunkHandles, handle) {
				node.chunkHandles = append(node.chunkHandles, handle)
			}
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

func (s *persistentState) checkSum(data []byte) uint32 {
	return crc32.Checksum(data, s.crc32Table)
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
