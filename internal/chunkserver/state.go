package chunkserver

import (
	"sync"
	"sync/atomic"
	"time"
)

type bufferEntry struct {
	chunkHandle uint64
	buf         []byte
	// zero while the stream is still filling buf, set once the push completes
	pushedAt time.Time
}

type bufferMap struct {
	sync.Mutex
	entries map[uint32]*bufferEntry
}

type leaseMap struct {
	sync.Mutex
	entries map[uint64]time.Time
}

// persistent state

type chunk struct {
	mu     sync.RWMutex
	handle uint64
	ctr    uint64
	// unix nanos of the last mutation this server accepted as primary, read
	// lock free by the heartbeat loop to decide whether to renew the lease
	lastWrite atomic.Int64
	*checksums
}

func newChunk(handle uint64, checksums *checksums) *chunk {
	return &chunk{
		mu:        sync.RWMutex{},
		handle:    handle,
		ctr:       0,
		checksums: checksums,
	}
}

type chunkSet struct {
	sync.Mutex
	entries map[uint64]*chunk
}

type versionMap struct {
	sync.Mutex
	entries map[uint64]uint32
}

type persistentState struct {
	*versionMap
	*chunkSet
}

/*

*/
