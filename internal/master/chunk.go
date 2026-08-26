package master

import (
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PraveenC2002/gfs/internal/messages"
)

var chunkHandle atomic.Uint64

func getChunkHandle() uint64 {
	return chunkHandle.Add(1)
}

const (
	defaultRepFactor   uint32        = 3
	chunkSeverDeadline time.Duration = 90 * time.Second
	leaseDeadline      time.Duration = 60 * time.Second
)

type chunk struct {
	sync.RWMutex
	id             uint64
	ref            uint32
	version        uint32
	repFactor      uint32
	primary        string
	leaseGrantTime time.Time
	locations      map[string]time.Time
}

func newChunk() *chunk {
	return &chunk{
		RWMutex:   sync.RWMutex{},
		id:        getChunkHandle(),
		ref:       0,
		version:   0,
		repFactor: defaultRepFactor,
		locations: make(map[string]time.Time),
	}
}
func (c *chunk) getLocations(exclude []string) []string {

	excludeSet := make(map[string]struct{})
	for _, loc := range exclude {
		excludeSet[loc] = struct{}{}
	}

	locations := make([]string, 0, len(c.locations))

	for loc, t := range c.locations {
		if _, ok := excludeSet[loc]; !ok && (t.IsZero() || time.Since(t) < chunkSeverDeadline) {
			locations = append(locations, loc)
		}
	}

	return locations
}

func (c *chunk) updateRef(tx *transaction, ref uint32) {

	tx.addOp(&operation{
		t:    UPDATE_REF,
		args: []string{strconv.FormatUint(c.id, 10), strconv.FormatUint(uint64(ref), 10)},
	})

	tx.addFunc(func() {
		c.ref = ref
	})

}

func (c *chunk) updateVersion(tx *transaction, ver uint32) {

	tx.addOp(&operation{
		t:    UPDATE_VERSION,
		args: []string{strconv.FormatUint(c.id, 10), strconv.FormatUint(uint64(ver), 10)},
	})

	tx.addFunc(func() {
		c.version = ver
	})
}

func (c *chunk) updateRep(tx *transaction, rep uint32) {

	tx.addOp(&operation{
		t:    UPDATE_REP,
		args: []string{strconv.FormatUint(c.id, 10), strconv.FormatUint(uint64(rep), 10)},
	})

	tx.addFunc(func() {
		c.repFactor = rep
	})
}

type chunkTable struct {
	mu      sync.RWMutex
	entries map[uint64]*chunk
}

func newChunkTable() *chunkTable {
	return &chunkTable{
		mu:      sync.RWMutex{},
		entries: map[uint64]*chunk{},
	}
}

// func (ct *chunkTable) getChunk(id uint64) (*chunk, error) {

// 	ct.mu.RLock()
// 	defer ct.mu.RUnlock()
// 	chunk, ok := ct.entries[id]
// 	if !ok {
// 		return nil, &ErrNotFound{}
// 	}
// 	return chunk, nil
// }

func (ct *chunkTable) addChunk(tx *transaction, c *chunk) {

	tx.addOp(&operation{
		t: CREATE_CHUNK,
		args: []string{strconv.FormatUint(c.id, 10),
			strconv.FormatUint(uint64(c.ref), 10),
			strconv.FormatUint(uint64(c.version), 10),
			strconv.FormatUint(uint64(c.repFactor), 10)},
	})

	tx.addFunc(func() {
		ct.mu.Lock()
		_, ok := ct.entries[c.id]
		if !ok {
			ct.entries[c.id] = c

		}
		ct.mu.Unlock()
	})
}

func (ct *chunkTable) remove(tx *transaction, handle uint64) {

	tx.addOp(&operation{
		t:    DELETE_CHUNK,
		args: []string{strconv.FormatUint(handle, 10)},
	})

	tx.addFunc(func() {
		ct.mu.Lock()
		delete(ct.entries, handle)
		ct.mu.Unlock()
	})
	
}

// TODO: drop stale replicas
func (ct *chunkTable) updateLocations(hb *messages.Heartbeat) {

	ct.mu.RLock()
	for _, chunkInfo := range hb.Chunks {

		if chunk, ok := ct.entries[chunkInfo.Id]; ok {
			chunk.Lock()
			chunk.locations[hb.Addr] = time.Now()
			chunk.Unlock()
		}
	}
	ct.clearStale()
	ct.mu.RUnlock()
}

// handle stale primary
func (ct *chunkTable) clearStale() {

	for _, c := range ct.entries {
		c.Lock()
		for loc, t := range c.locations {
			if time.Since(t) > chunkSeverDeadline {
				delete(c.locations, loc)
			}
		}
		c.Unlock()
	}
}

type chunkServer struct {
	lastBeat time.Time
	state    *messages.Heartbeat
}

type chunkServerRegistry struct {
	mu      sync.RWMutex
	entries map[string]*chunkServer
}

func newChunkServerRegistry() *chunkServerRegistry {
	return &chunkServerRegistry{
		mu:      sync.RWMutex{},
		entries: map[string]*chunkServer{},
	}
}

func (csr *chunkServerRegistry) update(hb *messages.Heartbeat) {
	csr.mu.Lock()
	cs, ok := csr.entries[hb.Addr]
	if !ok {
		csr.entries[hb.Addr] = &chunkServer{
			lastBeat: time.Now(),
			state:    hb,
		}
	} else {
		cs.state = hb
		cs.lastBeat = time.Now()
	}
	csr.mu.Unlock()
}

func (csr *chunkServerRegistry) getServers(n uint32, excludeList []string) ([]string, error) {

	excludeSet := make(map[string]struct{})
	for _, server := range excludeList {
		excludeSet[server] = struct{}{}
	}

	var servers []struct {
		addr     string
		diskUtil uint32
	}
	csr.mu.RLock()
	for _, cs := range csr.entries {
		if _, ok := excludeSet[cs.state.Addr]; ok {
			continue
		}
		if time.Since(cs.lastBeat) < chunkSeverDeadline {
			servers = append(servers, struct {
				addr     string
				diskUtil uint32
			}{addr: cs.state.Addr, diskUtil: cs.state.DiskUtil})
		}
	}
	csr.mu.RUnlock()

	if uint32(len(servers)) < n {
		return nil, &ErrNotFound{}
	}

	sort.Slice(servers, func(i, j int) bool {
		return servers[i].diskUtil < servers[j].diskUtil
	})

	serverAddrs := make([]string, 0, 2*n)
	for _, cs := range servers {
		if uint32(len(serverAddrs)) == 2*n {
			break
		}
		serverAddrs = append(serverAddrs, cs.addr)
	}

	var rands []string
	var ctr uint64 = uint64(rand.Uint32())

	for {
		if uint32(len(rands)) == n {
			break
		}
		rands = append(rands, serverAddrs[ctr%uint64(len(serverAddrs))])
		ctr++
	}
	return rands, nil
}

func (csr *chunkServerRegistry) getDeadServers() []*chunkServer {

	var servers []*chunkServer
	csr.mu.Lock()
	for addr, cs := range csr.entries {
		if time.Since(cs.lastBeat) > chunkSeverDeadline {
			delete(csr.entries, addr)
			servers = append(servers, cs)
		}
	}
	csr.mu.Unlock()

	return servers
}
