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

var hivePath string

var logHandle atomic.Uint64

func newLogHandle() uint64 {
	return logHandle.Add(1)
}

type operationType uint8

const (

	// Namespace mutations
	CREATE_FILE operationType = iota // CREATE_FILE "filename"
	CREATE_DIR                       // CREATE_DIR "dirname"
	REMOVE                           // REMOVE "location"
	MOVE                             // MOVE "src" "dst"
	SNAPSHOT                         // SNAPSHOT "src" "dst"

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
	logger.log(tx.ops)
	defer stateStatus.doneMutating()
	for _, fn := range tx.exec {
		fn()
	}
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
	sync.RWMutex
	checkpointSig chan uint64
}

func (s *syncState) isMutating() {
	s.RLock()
}

func (s *syncState) doneMutating() {
	s.RUnlock()
}

func (s *syncState) isReplicating() {
	s.Lock()
}

func (s *syncState) doneReplicating() {
	s.Unlock()
}

var stateStatus *syncState

type opLog struct {
	crc32Table *crc32.Table

	logMu   sync.Mutex
	logFile *os.File
}

func (l *opLog) checkSum(data []byte) uint32 {
	return crc32.Checksum(data, l.crc32Table)
}

/*

Operation flow :

0. expand the path
1. acquire locks
2. check exist
exist ? file or dir?
does not exist ? make or err
3. collect ops.
4. log ops
5. apply ops

*/

/*

Here : defer doneMutating in caller,
call-once meaning two consecutive calls to log() should have one doneMutating() call.
Ops should be batched and logged

*/

func (l *opLog) log(operations []*operation) {

	log := messages.Log{Recs: []*messages.OperationRec{}}
	for _, op := range operations {
		rec := messages.OperationRec{
			OpType: uint32(op.t),
			Args:   op.args,
		}
		log.Recs = append(log.Recs, &rec)
	}

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

	l.logMu.Lock()

	info, err := l.logFile.Stat()
	if err != nil {
		panic(err)
	}

	if info.Size() > maxLogSize {
		l.rotateLog()
	}

	writeFile(buf.Bytes(), l.logFile)

	stateStatus.isMutating()
	l.logMu.Unlock()
}

func (l *opLog) rotateLog() {

	err := l.logFile.Close()
	if err != nil {
		panic(err)
	}

	handle := newLogHandle()

	newLogName := strconv.FormatUint(handle, 10) + ".log"
	newLog, err := os.Create(filepath.Join(hivePath, newLogName))
	if err != nil {
		panic(err)
	}

	l.logFile = newLog

	select {
	case stateStatus.checkpointSig <- handle:
	default:
	}

}

var logger *opLog

type persistentState struct {
	*namespace
	*chunkTable

	crc32Table *crc32.Table
}

func newPersistentState() *persistentState {

	return &persistentState{

		namespace:  newNamesapce(),
		chunkTable: newChunkTable(),

		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}

}

func (s *persistentState) buildState() {

	files, err := os.ReadDir(hivePath)
	if err != nil {
		panic(err)
	}

	var logs []uint64
	var checkpoint uint64 = 0

	currLogHandle := logHandle.Load()
	for _, file := range files {

		if name, ok := strings.CutSuffix(file.Name(), ".log"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil && n != currLogHandle {
				logs = append(logs, n)
			}
		}

		if name, ok := strings.CutSuffix(file.Name(), ".cp"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil {
				if checkpoint == 0 || checkpoint < n {
					checkpoint = n
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
}

func (s *persistentState) listenCheckpointSig() {

	for handle := range stateStatus.checkpointSig {
		s.flushState(handle)
	}
}

func (s *persistentState) flushState(handle uint64) {

	file, err := os.Create(filepath.Join(hivePath, "temp.cp"))
	if err != nil {
		panic(err)
	}

	stateStatus.isReplicating()

	s.namespace.mu.RLock()
	namespace := maps.Clone(s.namespace.entries)
	s.namespace.mu.RUnlock()

	s.chunkTable.mu.RLock()
	chunkTable := maps.Clone(s.chunkTable.entries)
	s.chunkTable.mu.RUnlock()

	stateStatus.doneReplicating()

	const maxMem = 1024 * 1024 * 100

	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian, chunkHandle.Load())
	if err != nil {
		panic(err)
	}

	for k, n := range namespace {

		n.RLock()

		chunkHandles := []uint64{}
		for _, h := range n.metadata.chunkHandles {
			chunkHandles = append(chunkHandles, h)
		}

		rec, err := proto.Marshal(&messages.NamespaceRec{
			Path: k,
			Dir:  n.dir,
			Metadata: &messages.NamespaceRec_Metadata{
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

	for k, c := range chunkTable {

		c.RLock()

		rec, err := proto.Marshal(&messages.ChunktableRec{
			Handle: k,
			Chunk: &messages.ChunktableRec_Chunk{
				Ref:       c.ref,
				Version:   c.version,
				RepFactor: c.repFactor,
			},
		})
		if err != nil {
			panic(err)
		}
		c.RUnlock()

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

func (s *persistentState) applyOp(op *messages.OperationRec) {

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
		delete(s.namespace.entries, op.Args[0])
		s.namespace.mu.Unlock()

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

		chunkHandle.Store(max(handle, chunkHandle.Load()))

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

		var log messages.Log
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

	chunkHandle.Store(max(binary.BigEndian.Uint64(handleBuf[:]), chunkHandle.Load()))

	s.namespace.mu.Lock()
	for {

		_, buf, err := readFileSegment(file)
		if err != nil {
			panic(err)
		}

		if len(buf) == 0 {
			break
		}

		var rec messages.NamespaceRec
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

		var rec messages.ChunktableRec
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

func init() {

	hivePath = func() string {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}

		hivePath := filepath.Join(home, ".hiveFS")
		if err := os.MkdirAll(hivePath, 0755); err != nil {
			panic(err)
		}

		return hivePath
	}()

	stateStatus = &syncState{
		checkpointSig: make(chan uint64, 1),
	}

	logger = &opLog{
		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}

	func() {

		files, err := os.ReadDir(hivePath)
		if err != nil {
			panic(err)
		}

		for _, file := range files {

			if name, ok := strings.CutSuffix(file.Name(), ".log"); ok {
				if n, err := strconv.ParseUint(name, 10, 64); err == nil {
					logHandle.Store(max(logHandle.Load(), n))
				}
			}

			if name, ok := strings.CutSuffix(file.Name(), ".cp"); ok {
				if n, err := strconv.ParseUint(name, 10, 64); err == nil {
					logHandle.Store(max(logHandle.Load(), n))
				}
			}
		}

		newLogName := strconv.FormatUint(newLogHandle(), 10) + ".log"
		newLog, err := os.Create(filepath.Join(hivePath, newLogName))
		if err != nil {
			panic(err)
		}

		logger.logMu = sync.Mutex{}
		logger.logFile = newLog

	}()

}
