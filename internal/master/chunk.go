package master

import (
	"maps"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/PraveenC2002/gfs/internal/messages"
)

const (
	defaultRepFactor   uint32        = 3
	chunkServerDeadline time.Duration = 90 * time.Second
	leaseDeadline      time.Duration = 60 * time.Second
)

type chunk struct {
	sync.Mutex     // TODO: this could be inside a lock manager
	id             uint64
	ref            uint32
	version        uint32
	repFactor      uint32
	primary        string
	leaseGrantTime time.Time
	locations      map[string]time.Time
}

func newChunk(id uint64) *chunk {
	return &chunk{
		Mutex:     sync.Mutex{},
		id:        id,
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
		if _, ok := excludeSet[loc]; !ok {
			if t.IsZero() || time.Since(t) < chunkServerDeadline {
				locations = append(locations, loc)
			} else {
				delete(c.locations, loc)
			}
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

const (
	repRequestExpiry time.Duration = 70 * time.Second
)

type chunkTable struct {
	mu      sync.RWMutex
	entries map[uint64]*chunk

	stagedCreateMu sync.Mutex
	stagedCreate   map[uint64]struct{}
	// handle -> destination server -> time the replicate request was issued
	repRequestsMu sync.Mutex
	repRequests   map[uint64]map[string]time.Time
}

func newChunkTable() *chunkTable {
	return &chunkTable{
		mu:             sync.RWMutex{},
		entries:        map[uint64]*chunk{},
		stagedCreateMu: sync.Mutex{},
		stagedCreate:   make(map[uint64]struct{}),
		repRequestsMu:  sync.Mutex{},
		repRequests:    make(map[uint64]map[string]time.Time),
	}
}

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

func (ct *chunkTable) processHeartbeat(hb *messages.Chunkserver_Heartbeat) *messages.Master_HeartbeatRes {

	res := &messages.Master_HeartbeatRes{}

	ct.mu.RLock()
	chunks := maps.Clone(ct.entries)
	ct.mu.RUnlock()

	for _, chunkInfo := range hb.Chunks {

		if chunk, ok := chunks[chunkInfo.Id]; ok {

			chunk.Lock()

			if chunkInfo.Version >= chunk.version {

				chunk.locations[hb.Addr] = time.Now()
				ct.repRequestsMu.Lock()
				if reqs, ok := ct.repRequests[chunk.id]; ok {
					delete(reqs, hb.Addr)
					if len(reqs) == 0 {
						delete(ct.repRequests, chunk.id)
					}
				}
				ct.repRequestsMu.Unlock()

				if chunkInfo.Version > chunk.version {
					res.UpdateVersion = append(res.UpdateVersion, &messages.Master_UpdateVersion{
						Id:      chunkInfo.Id,
						Version: chunk.version,
					})
				}

				if chunkInfo.ExtendLease {
					if chunk.primary == hb.Addr {
						chunk.leaseGrantTime = time.Now()
						res.ExtendLease = append(res.ExtendLease, chunkInfo.Id)
					} else {
						res.RevokeLease = append(res.RevokeLease, chunkInfo.Id)
					}
				}
			} else if chunkInfo.Version < chunk.version {
				res.Delete = append(res.Delete, chunkInfo.Id)
				delete(chunk.locations, hb.Addr)
			}

			for loc, t := range chunk.locations {
				if !t.IsZero() && time.Since(t) > chunkServerDeadline {
					delete(chunk.locations, loc)
				}
			}

			chunk.Unlock()
		} else {
			ct.stagedCreateMu.Lock()
			if _, ok := ct.stagedCreate[chunkInfo.Id]; !ok {
				res.Delete = append(res.Delete, chunkInfo.Id)
			}
			ct.stagedCreateMu.Unlock()
		}
	}

	return res
}

type chunkServer struct {
	lastBeat time.Time
	state    *messages.Chunkserver_Heartbeat
}

type chunkServerRegistry struct {
	mu      sync.RWMutex
	entries map[string]*chunkServer
}

func newChunkServerRegistry() *chunkServerRegistry {
	return &chunkServerRegistry{
		mu:      sync.RWMutex{},
		entries: make(map[string]*chunkServer),
	}
}

func (csr *chunkServerRegistry) prune() {

	csr.mu.Lock()
	for addr, server := range csr.entries {
		if time.Since(server.lastBeat) > chunkServerDeadline {
			delete(csr.entries, addr)
		}
	}
	csr.mu.Unlock()

}

func (csr *chunkServerRegistry) update(hb *messages.Chunkserver_Heartbeat) {
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
		if time.Since(cs.lastBeat) < chunkServerDeadline {
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
