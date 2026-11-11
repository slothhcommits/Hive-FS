package master

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/slothhcommits/Hive-FS/internal/messages"
	"google.golang.org/protobuf/proto"
)

var (
	maxLogSize int64 = 1024 * 1024 * 10
)

type opLog struct {
	mu sync.Mutex

	dir        string
	handle     atomic.Uint64
	crc32Table *crc32.Table
	logFile    *os.File
}

func newOpLog(dir string) *opLog {
	return &opLog{
		dir:        dir,
		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}
}

func (oplog *opLog) newHandle() uint64 {
	return oplog.handle.Add(1)
}

// open creates the log directory and
// starts a fresh log after the highest existing log or checkpoint handle

func (oplog *opLog) open() {

	if err := os.MkdirAll(oplog.dir, 0755); err != nil {
		panic(err)
	}

	files, err := os.ReadDir(oplog.dir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {

		if name, ok := strings.CutSuffix(file.Name(), ".log"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil {
				oplog.handle.Store(max(oplog.handle.Load(), n))
			}
		}

		if name, ok := strings.CutSuffix(file.Name(), ".cp"); ok {
			if n, err := strconv.ParseUint(name, 10, 64); err == nil {
				oplog.handle.Store(max(oplog.handle.Load(), n))
			}
		}
	}

	newLogName := strconv.FormatUint(oplog.newHandle(), 10) + ".log"
	newLog, err := os.Create(filepath.Join(oplog.dir, newLogName))
	if err != nil {
		panic(err)
	}

	oplog.logFile = newLog
}

func (oplog *opLog) checkSum(data []byte) uint32 {
	return crc32.Checksum(data, oplog.crc32Table)
}

func (oplog *opLog) isFull() bool {

	info, err := oplog.logFile.Stat()
	if err != nil {
		panic(err)
	}

	return info.Size() > maxLogSize
}

func (oplog *opLog) log(tx *transaction) {

	log := messages.Master_Log{Recs: []*messages.Master_OperationRec{}}

	for _, op := range tx.operations {
		rec := messages.Master_OperationRec{
			OpType: uint32(op.t),
			Args:   op.args,
		}
		log.Recs = append(log.Recs, &rec)
	}

	if len(log.Recs) > 0 {

		var buf bytes.Buffer

		logBuf, err := proto.Marshal(&log)
		if err != nil {
			panic(err)
		}

		binary.Write(&buf, binary.BigEndian, oplog.checkSum(logBuf))
		binary.Write(&buf, binary.BigEndian, uint32(len(logBuf)))
		_, err = buf.Write(logBuf)
		if err != nil {
			panic(err)
		}

		writeFile(buf.Bytes(), oplog.logFile)
	}
}

func (oplog *opLog) rotateLog(handle uint64) {

	err := oplog.logFile.Close()
	if err != nil {
		panic(err)
	}

	newLogName := strconv.FormatUint(handle, 10) + ".log"
	newLog, err := os.Create(filepath.Join(oplog.dir, newLogName))
	if err != nil {
		panic(err)
	}

	oplog.logFile = newLog
}
