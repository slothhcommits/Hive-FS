package chunkserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/slothhcommits/Hive-FS/internal"
)

// 64mb -> 64kb blocks = 1024 checksums --> 4bytes each = 4kb

const (
	blockSize      = 64 << 10
	maxChunkSize   = 64 << 20
	blocksPerChunk = maxChunkSize / blockSize // 1024
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type checksums struct {
	handle uint64
	table  []uint32
}

func newChecksums(handle uint64) *checksums {
	return &checksums{
		handle: handle,
		table:  make([]uint32, 0, 1024),
	}
}

// caller's responsibility to close the file
// args : chunkfile, chunkfile offset, chunkfile length
func (checksums *checksums) verify(chunkFile *os.File, offset int64, length uint32) error {

	if offset < 0 {
		return internal.ErrBadRequest
	}

	if length == 0 {
		return nil
	}

	idx := int(offset / blockSize)
	n := ((int(offset) + int(length) - 1) / blockSize) - idx + 1

	if len(checksums.table) < idx+n {
		return internal.ErrBadRequest
	}

	cfi, err := chunkFile.Stat()
	if err != nil {
		return err
	}

	nChunks := cfi.Size() / blockSize

	if cfi.Size()%blockSize != 0 {
		nChunks++
	}

	if idx+n > int(nChunks) {
		return internal.ErrFileCorrupt
	}

	_, err = chunkFile.Seek(int64(idx)*blockSize, io.SeekStart)
	if err != nil {
		return err
	}

	block := make([]byte, blockSize)

	for i := idx; i < idx+n; i++ {

		nRead, err := io.ReadFull(chunkFile, block)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			if errors.Is(err, io.EOF) {
				return internal.ErrFileCorrupt
			}
			return err
		}

		if checksums.table[i] != crc32.Checksum(block[:nRead], castagnoli) {
			return internal.ErrFileCorrupt
		}
	}

	return nil
}

func (checksums *checksums) update(chunkFile *os.File, offset int64, length uint32) error {

	if offset < 0 {
		return internal.ErrBadRequest
	}

	if length == 0 {
		return nil
	}

	idx := int(offset / blockSize)
	n := ((int(offset) + int(length) - 1) / blockSize) - idx + 1

	if idx < 0 {
		panic(fmt.Errorf("negative index"))
	}

	if idx+n > blocksPerChunk {
		return internal.ErrBadRequest
	}

	// a write that extends the chunk covers blocks the table has no entry for yet.
	for len(checksums.table) < idx+n {
		checksums.table = append(checksums.table, 0)
	}

	if _, err := chunkFile.Seek(int64(idx)*blockSize, io.SeekStart); err != nil {
		return err
	}

	block := make([]byte, blockSize)

	for i := idx; i < idx+n; i++ {

		nRead, err := io.ReadFull(chunkFile, block)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}

		checksums.table[i] = crc32.Checksum(block[:nRead], castagnoli)

		if err != nil {
			break
		}
	}

	return nil
}

// Caller's responsibility to close the file
func (checksums *checksums) flush(checksumfile *os.File) error {

	if len(checksums.table) == 0 {
		return nil
	}

	buf := make([]byte, len(checksums.table)*4)

	for i, checksum := range checksums.table {
		binary.BigEndian.PutUint32(buf[i*4:], checksum)
	}

	_, err := checksumfile.Write(buf)
	if err != nil {
		return err
	}

	return checksumfile.Sync()
}
