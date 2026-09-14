package chunkserver

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/PraveenC2002/gfs/internal"
)

var hiveStorePath string

func makeChunkPath(handle uint64) string {
	return filepath.Join(hiveStorePath, strconv.FormatUint(handle, 10)+".chunk")
}

func makeChecksumPath(handle uint64) string {
	return filepath.Join(hiveStorePath, strconv.FormatUint(handle, 10)+".sum")
}

type bufferMap struct {
	sync.Mutex
	entries map[uint32][]byte
}

type chunkLeaseSet struct {
	sync.Mutex
	entries map[uint64]struct{}
}

type chunkHandleSet struct {
	sync.Mutex
	entries map[uint64]struct{}
}

type versionMap struct {
	sync.Mutex
	entries map[uint32]uint32
}

// 64mb -> 64kb blocks = 1024 checksums --> 4bytes each = 4kb

const (
	blockSize      = 64 << 10
	chunkSize      = 64 << 20
	blocksPerChunk = chunkSize / blockSize // 1024
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type checksums struct {
	handle uint64
	table  []uint32
}

func (checksums *checksums) verify(idx int, n int) error {

	if len(checksums.table) < idx+n {
		return internal.ErrOpFailed
	}

	chunkPath := makeChunkPath(checksums.handle)

	chunkFile, err := os.Open(chunkPath)
	if err != nil {
		return err
	}
	defer chunkFile.Close()

	cfi, err := chunkFile.Stat()
	if err != nil {
		return err
	}

	nChunks := cfi.Size() / blockSize

	if cfi.Size()%blockSize != 0 {
		nChunks++
	}

	if idx+n > int(nChunks) {
		return ErrFileCorrupt
	}

	offset := idx * blockSize
	_, err = chunkFile.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}

	block := make([]byte, blockSize)

	for i := idx; i < idx+n; i++ {

		nRead, err := io.ReadFull(chunkFile, block)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			if errors.Is(err, io.EOF) {
				return ErrFileCorrupt
			}
			return err
		}

		if checksums.table[i] != crc32.Checksum(block[:nRead], castagnoli) {
			return ErrFileCorrupt
		}
	}

	return nil
}

func (checksums *checksums) update(chunkFile *os.File, idx int, n int) error {

	if idx < 0 || idx+n > blocksPerChunk {
		return internal.ErrOpFailed
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

func (checksums *checksums) flush() error {

	path := makeChecksumPath(checksums.handle)
	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, len(checksums.table)*4)

	for i, checksum := range checksums.table {
		binary.BigEndian.PutUint32(buf[i*4:], checksum)
	}

	_, err = file.Write(buf)
	if err != nil {
		return err
	}

	return file.Sync()
}

func newChecksums(handle uint64) (*checksums, error) {

	path := makeChecksumPath(handle)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	file.Close()

	store, err := os.Open(hiveStorePath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	err = store.Sync()
	if err != nil {
		return nil, err
	}

	return &checksums{
		handle: handle,
	}, nil
}

func loadChecksums(handle uint64) (*checksums, error) {

	path := makeChecksumPath(handle)

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	checksums := &checksums{
		handle: handle,
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if fi.Size()%4 != 0 || fi.Size() > blocksPerChunk*4 {
		return nil, ErrFileCorrupt
	}

	n := int(fi.Size() / 4) // number of entries actually stored

	// the .sum file must hold exactly one entry per block of the chunk file,
	// otherwise some blocks have no checksum and nothing would ever notice.
	cfi, err := os.Stat(makeChunkPath(handle))
	if err != nil {
		return nil, err
	}

	nBlocks := int(cfi.Size() / blockSize)
	if cfi.Size()%blockSize != 0 {
		nBlocks++
	}

	if n != nBlocks {
		return nil, ErrFileCorrupt
	}

	buf := make([]byte, n*4)
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, err
	}

	checksums.table = make([]uint32, n)

	for i := 0; i < n; i++ {
		checksums.table[i] = binary.BigEndian.Uint32(buf[i*4:])
	}

	return checksums, nil
}

type persistentState struct {
	*versionMap
}

func newPersistentSpace() *persistentState {

	return &persistentState{
		versionMap: &versionMap{
			Mutex:   sync.Mutex{},
			entries: make(map[uint32]uint32),
		},
	}
}

func init() {

	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	hiveStorePath = filepath.Join(home, "hiveStore")

}

var ErrFileCorrupt = errors.New("file corrupt")
