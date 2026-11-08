package chunkserver

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/slothhcommits/Hive-FS/internal"
)

type store struct {
	path string
}

func newStore(path string) *store {
	return &store{
		path: path,
	}
}

// generic

func (st *store) buildState() *persistentState {

	err := os.MkdirAll(st.path, 0o755)
	if err != nil {
		panic(err)
	}
	dir, err := os.ReadDir(st.path)
	if err != nil {
		panic(err)
	}

	garbage := []string{}
	handles := make(map[uint64]struct{})

	versionMap := &versionMap{
		Mutex:   sync.Mutex{},
		entries: make(map[uint64]uint32),
	}

	chunkSet := &chunkSet{
		Mutex:   sync.Mutex{},
		entries: make(map[uint64]*chunk),
	}

	for _, f := range dir {

		if !f.IsDir() {

			switch {

			case strings.HasSuffix(f.Name(), ".ver"):
				n, err := strconv.ParseUint(strings.TrimSuffix(f.Name(), ".ver"), 10, 64)
				if err != nil {
					garbage = append(garbage, f.Name())
					continue
				}
				handles[n] = struct{}{}

			case strings.HasSuffix(f.Name(), ".sum"):
				n, err := strconv.ParseUint(strings.TrimSuffix(f.Name(), ".sum"), 10, 64)
				if err != nil {
					garbage = append(garbage, f.Name())
					continue
				}
				handles[n] = struct{}{}

			case strings.HasSuffix(f.Name(), ".chunk"):
				n, err := strconv.ParseUint(strings.TrimSuffix(f.Name(), ".chunk"), 10, 64)
				if err != nil {
					garbage = append(garbage, f.Name())
					continue
				}
				handles[n] = struct{}{}

			default:
				garbage = append(garbage, f.Name())
			}

		} else {
			garbage = append(garbage, f.Name())
		}
	}

	for handle := range handles {
		
		checksums, err := st.loadChecksums(handle)
		if err != nil {
			st.deleteChunkFiles(handle)
			continue
		}

		ver, err := st.readVersion(handle)
		if err != nil {
			st.deleteChunkFiles(handle)
			continue
		}

		versionMap.entries[handle] = ver
		chunkSet.entries[handle] = newChunk(handle, checksums)
		
	}
	
	for _, f := range garbage {
		st.deleteFile(filepath.Join(st.path, f))
	}

	err = st.syncStore()
	if err != nil {
		panic(err)
	}

	return &persistentState{
		versionMap: versionMap,
		chunkSet:   chunkSet,
	}
}

func (st *store) syncStore() error {

	store, err := os.Open(st.path)
	if err != nil {
		return err
	}
	defer store.Close()

	err = store.Sync()
	if err != nil {
		panic(err)
	}

	return nil
}

func (st *store) newFile(path string) (*os.File, error) {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}

	err = st.syncStore()
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (st *store) openFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR, 0o644)
}

func (st *store) deleteFile(path string) error {

	err := os.Remove(path)
	if err != nil {
		return err
	}

	err = st.syncStore()
	if err != nil {
		return err
	}

	return nil
}

// chunk
func (st *store) makeChunkPath(handle uint64) string {
	return filepath.Join(st.path, strconv.FormatUint(handle, 10)+".chunk")
}

func (st *store) makeNewChunkFiles(handle uint64) error {

	chunkFile, err := st.newFile(st.makeChunkPath(handle))
	if err != nil {
		// If file exists, do not delete any chunk files... Just return the error as is
		return err
	}
	chunkFile.Close()

	checksumFile, err := st.newFile(st.makeChecksumPath(handle))
	if err != nil {
		// You can come here only when chunk file does not exist and is created successfully
		// So if file exists here you correctly delete all the associated chunk files
		st.deleteChunkFiles(handle)
		return err
	}
	checksumFile.Close()

	err = st.newVersionFile(handle)
	if err != nil {
		// You can come here only when chunk and checksum file does not exist and are created successfully
		// So if file exists here you correctly delete all the associated chunk files
		st.deleteChunkFiles(handle)
		return err
	}

	return st.syncStore()
}

func (st *store) deleteChunkFiles(handle uint64) error {

	err := errors.Join(
		st.deleteFile(st.makeChunkPath(handle)),
		st.deleteFile(st.makeChecksumPath(handle)),
		st.deleteFile(st.makeVersionPath(handle)),
		st.syncStore(),
	)

	return err
}

// reason to pass files is because you don't have to open close file again and again across operations
func (st *store) writeChunk(f *os.File, offset int64, buf []byte) error {

	_, err := f.WriteAt(buf, offset)
	if err != nil {
		return err
	}

	return f.Sync()
}

// checksums
func (st *store) makeChecksumPath(handle uint64) string {
	return filepath.Join(st.path, strconv.FormatUint(handle, 10)+".sum")
}

func (st *store) loadChecksums(handle uint64) (*checksums, error) {

	path := st.makeChecksumPath(handle)

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
		return nil, internal.ErrFileCorrupt
	}

	n := int(fi.Size() / 4) // number of entries actually stored

	// the .sum file must hold exactly one entry per block of the chunk file,
	// otherwise some blocks have no checksum and nothing would ever notice.
	cfi, err := os.Stat(st.makeChunkPath(handle))
	if err != nil {
		return nil, err
	}

	nBlocks := int(cfi.Size() / blockSize)
	if cfi.Size()%blockSize != 0 {
		nBlocks++
	}

	if n != nBlocks {
		return nil, internal.ErrFileCorrupt
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

// versions
func (st *store) makeVersionPath(handle uint64) string {
	return filepath.Join(st.path, strconv.FormatUint(handle, 10)+".ver")
}

func (st *store) newVersionFile(handle uint64) error {

	vf, err := st.newFile(st.makeVersionPath(handle))
	if err != nil {
		return err
	}
	defer vf.Close()

	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 0)
	_, err = vf.WriteAt(buf[:], 0)
	if err != nil {
		return err
	}

	err = vf.Sync()
	if err != nil {
		return err
	}

	return st.syncStore()
}

func (st *store) readVersion(handle uint64) (uint32, error) {

	f, err := st.openFile(st.makeVersionPath(handle))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var buf [4]byte
	_, err = io.ReadFull(f, buf[:])
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf[:]), nil
}

func (st *store) logVersion(handle uint64, version uint32) error {

	vf, err := st.openFile(st.makeVersionPath(handle))
	if err != nil {
		return err
	}

	defer vf.Close()

	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], version)
	_, err = vf.WriteAt(buf[:], 0)
	if err != nil {
		return err
	}

	return vf.Sync()
}
