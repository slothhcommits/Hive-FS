package chunkserver

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

var hiveStorePath string

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

type checksums struct {
	checksumTable *crc32.Table
	table         map[uint64][blocksPerChunk]uint32
	file          *os.File
}

func newChecksums(path string) (*checksums, error) {

	
	
}

type opLog struct {
}

type persistentState struct {
	*versionMap
	*checksums
}

func newPersistentSpace() *persistentState {

	return &persistentState{
		checksums: &checksums{
			Mutex:         sync.Mutex{},
			checksumTable: crc32.MakeTable(crc32.Castagnoli),
			table:         make(map[uint64][blocksPerChunk]uint32),
		},
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
