package master

// import (
// 	"errors"
// 	"reflect"
// 	"slices"
// 	"testing"
// 	"time"

// 	"github.com/slothhcommits/Hive-FS/internal/messages"
// )

// // ---------------------------------------------------------------------------
// // chunk.getLocations
// // ---------------------------------------------------------------------------

// func TestChunk_GetLocations(t *testing.T) {

// 	// anything for the non-location fields; locations: A and B fresh, C dead, D zeroed
// 	newC := func() *chunk {
// 		c := newChunk(1)
// 		c.repFactor = 3
// 		c.version = 2
// 		c.ref = 1
// 		c.primary = "A"
// 		c.leaseGrantTime = time.Now()
// 		c.locations = map[string]time.Time{
// 			"A": time.Now().Add(-10 * time.Second),
// 			"B": time.Now().Add(-30 * time.Second),
// 			"C": time.Now().Add(-200 * time.Second),
// 			"D": {},
// 		}
// 		return c
// 	}

// 	t.Run("nil exclusion list", func(t *testing.T) {
// 		got := newC().getLocations(nil)
// 		slices.Sort(got)
// 		want := []string{"A", "B", "D"}
// 		if !slices.Equal(got, want) {
// 			t.Fatalf("want %v, got %v", want, got)
// 		}
// 	})

// 	t.Run("exclusion list is not nil", func(t *testing.T) {
// 		got := newC().getLocations([]string{"A", "C"})
// 		slices.Sort(got)
// 		want := []string{"B", "D"}
// 		if !slices.Equal(got, want) {
// 			t.Fatalf("want %v, got %v", want, got)
// 		}
// 	})
// }

// // ---------------------------------------------------------------------------
// // chunkTable.processHeartbeat
// // ---------------------------------------------------------------------------

// func cstHB(addr string, infos ...*messages.Chunkserver_ChunkInfo) *messages.Chunkserver_Heartbeat {
// 	return &messages.Chunkserver_Heartbeat{Addr: addr, Chunks: infos}
// }

// func cstInfo(id uint64, version uint32, extend bool) *messages.Chunkserver_ChunkInfo {
// 	return &messages.Chunkserver_ChunkInfo{Id: id, Version: version, ExtendLease: extend}
// }

// func cstWithin(t *testing.T, label string, got, before, after time.Time) {
// 	t.Helper()
// 	if got.Before(before) || got.After(after) {
// 		t.Fatalf("%s: %v is not within [%v, %v]", label, got, before, after)
// 	}
// }

// func TestChunkTable_ProcessHeartbeat(t *testing.T) {

// 	const id = uint64(7)

// 	// master version 5, primary P, replicas P and R1 both fresh
// 	newTable := func() (*chunkTable, *chunk) {
// 		c := newChunk(id)
// 		c.version = 5
// 		c.ref = 1
// 		c.primary = "P"
// 		c.leaseGrantTime = time.Now().Add(-10 * time.Second)
// 		c.locations = map[string]time.Time{
// 			"P":  time.Now().Add(-5 * time.Second),
// 			"R1": time.Now().Add(-5 * time.Second),
// 		}
// 		ct := newChunkTable()
// 		ct.entries[id] = c
// 		return ct, c
// 	}

// 	t.Run("reported version lower than master", func(t *testing.T) {
// 		ct, c := newTable()
// 		res := ct.processHeartbeat(cstHB("R1", cstInfo(id, 4, false)))

// 		if !reflect.DeepEqual(res.Delete, []uint64{id}) {
// 			t.Fatalf("Delete: want [%d], got %v", id, res.Delete)
// 		}
// 		if _, ok := c.locations["R1"]; ok {
// 			t.Fatal("the reporter must be dropped from locations")
// 		}
// 	})

// 	t.Run("reported version greater than master", func(t *testing.T) {
// 		ct, c := newTable()
// 		res := ct.processHeartbeat(cstHB("R1", cstInfo(id, 6, false)))

// 		if len(res.UpdateVersion) != 1 || res.UpdateVersion[0].Id != id || res.UpdateVersion[0].Version != 5 {
// 			t.Fatalf("UpdateVersion: want [{%d 5}], got %v", id, res.UpdateVersion)
// 		}
// 		if c.version != 5 {
// 			t.Fatalf("master version must stay 5, got %d", c.version)
// 		}
// 		if len(res.Delete) != 0 {
// 			t.Fatalf("Delete: want empty, got %v", res.Delete)
// 		}
// 	})

// 	t.Run("reported version equals master", func(t *testing.T) {
// 		ct, c := newTable()
// 		before := time.Now()
// 		res := ct.processHeartbeat(cstHB("R1", cstInfo(id, 5, false)))
// 		after := time.Now()

// 		if len(res.Delete) != 0 || len(res.UpdateVersion) != 0 || len(res.ExtendLease) != 0 || len(res.RevokeLease) != 0 {
// 			t.Fatalf("expected an empty response, got %v", res)
// 		}
// 		cstWithin(t, "R1 location time", c.locations["R1"], before, after)
// 		if c.version != 5 {
// 			t.Fatalf("master version must stay 5, got %d", c.version)
// 		}
// 	})

// 	t.Run("unknown chunk", func(t *testing.T) {
// 		ct, _ := newTable()
// 		res := ct.processHeartbeat(cstHB("R1", cstInfo(99, 1, false)))

// 		if !reflect.DeepEqual(res.Delete, []uint64{99}) {
// 			t.Fatalf("Delete: want [99], got %v", res.Delete)
// 		}
// 	})

// 	t.Run("lease extension from the primary", func(t *testing.T) {
// 		ct, c := newTable()
// 		before := time.Now()
// 		res := ct.processHeartbeat(cstHB("P", cstInfo(id, 5, true)))
// 		after := time.Now()

// 		if !reflect.DeepEqual(res.ExtendLease, []uint64{id}) {
// 			t.Fatalf("ExtendLease: want [%d], got %v", id, res.ExtendLease)
// 		}
// 		if len(res.RevokeLease) != 0 {
// 			t.Fatalf("RevokeLease: want empty, got %v", res.RevokeLease)
// 		}
// 		cstWithin(t, "lease grant time", c.leaseGrantTime, before, after)
// 		if c.primary != "P" {
// 			t.Fatalf("primary must stay P, got %q", c.primary)
// 		}
// 	})

// 	t.Run("lease extension from a non primary", func(t *testing.T) {
// 		ct, c := newTable()
// 		origGrant := c.leaseGrantTime
// 		res := ct.processHeartbeat(cstHB("R1", cstInfo(id, 5, true)))

// 		if !reflect.DeepEqual(res.RevokeLease, []uint64{id}) {
// 			t.Fatalf("RevokeLease: want [%d], got %v", id, res.RevokeLease)
// 		}
// 		if len(res.ExtendLease) != 0 {
// 			t.Fatalf("ExtendLease: want empty, got %v", res.ExtendLease)
// 		}
// 		if c.primary != "P" || !c.leaseGrantTime.Equal(origGrant) {
// 			t.Fatalf("primary and lease must be untouched, got %q and %v", c.primary, c.leaseGrantTime)
// 		}
// 	})

// 	t.Run("beat arrives after a replica expired", func(t *testing.T) {
// 		c := newChunk(id)
// 		c.version = 5
// 		c.ref = 1
// 		c.primary = "X"
// 		c.leaseGrantTime = time.Now().Add(-10 * time.Second)
// 		c.locations = map[string]time.Time{
// 			"X": time.Now().Add(-200 * time.Second),
// 			"Y": time.Now().Add(-5 * time.Second),
// 		}
// 		ct := newChunkTable()
// 		ct.entries[id] = c

// 		before := time.Now()
// 		ct.processHeartbeat(cstHB("Y", cstInfo(id, 5, false)))
// 		after := time.Now()

// 		if _, ok := c.locations["X"]; ok {
// 			t.Fatal("the expired replica must be dropped from locations")
// 		}
// 		if c.primary != "" {
// 			t.Fatalf("primary must be cleared, got %q", c.primary)
// 		}
// 		if !c.leaseGrantTime.IsZero() {
// 			t.Fatalf("lease grant time must be zeroed, got %v", c.leaseGrantTime)
// 		}
// 		cstWithin(t, "Y location time", c.locations["Y"], before, after)
// 	})
// }

// // ---------------------------------------------------------------------------
// // chunkServerRegistry.getServers
// // ---------------------------------------------------------------------------

// func cstRegistry(addrs ...string) *chunkServerRegistry {
// 	csr := newChunkServerRegistry()
// 	for i, a := range addrs {
// 		csr.entries[a] = &chunkServer{
// 			lastBeat: time.Now(),
// 			state:    &messages.Chunkserver_Heartbeat{Addr: a, DiskUtil: uint32(i)},
// 		}
// 	}
// 	return csr
// }

// func cstNotFound(err error) bool {
// 	return errors.As(err, new(*ErrNotFound))
// }

// func TestChunkServerRegistry_GetServers(t *testing.T) {

// 	t.Run("n greater than available", func(t *testing.T) {
// 		csr := cstRegistry("s1", "s2", "s3")
// 		got, err := csr.getServers(4, nil)
// 		if !cstNotFound(err) || got != nil {
// 			t.Fatalf("want ErrNotFound and nil, got %v and %v", got, err)
// 		}
// 	})

// 	t.Run("exclude list covers all available", func(t *testing.T) {
// 		csr := cstRegistry("s1", "s2", "s3")
// 		got, err := csr.getServers(2, []string{"s1", "s2", "s3"})
// 		if !cstNotFound(err) || got != nil {
// 			t.Fatalf("want ErrNotFound and nil, got %v and %v", got, err)
// 		}
// 	})

// 	t.Run("available minus excluded is less than n", func(t *testing.T) {
// 		csr := cstRegistry("s1", "s2", "s3", "s4")
// 		got, err := csr.getServers(3, []string{"s1", "s2"})
// 		if !cstNotFound(err) || got != nil {
// 			t.Fatalf("want ErrNotFound and nil, got %v and %v", got, err)
// 		}
// 	})

// 	t.Run("success excludes and returns exactly n", func(t *testing.T) {
// 		csr := cstRegistry("s1", "s2", "s3", "s4", "s5", "s6")
// 		exclude := []string{"s2"}
// 		// the start offset is random
// 		for i := 0; i < 200; i++ {
// 			got, err := csr.getServers(3, exclude)
// 			if err != nil {
// 				t.Fatalf("iteration %d: %v", i, err)
// 			}
// 			if len(got) != 3 {
// 				t.Fatalf("iteration %d: want 3 servers, got %v", i, got)
// 			}
// 			seen := map[string]bool{}
// 			for _, a := range got {
// 				if a == "s2" {
// 					t.Fatalf("iteration %d: excluded server returned: %v", i, got)
// 				}
// 				if _, ok := csr.entries[a]; !ok {
// 					t.Fatalf("iteration %d: unknown server returned: %v", i, got)
// 				}
// 				if seen[a] {
// 					t.Fatalf("iteration %d: duplicate server: %v", i, got)
// 				}
// 				seen[a] = true
// 			}
// 		}
// 	})

// 	t.Run("n is 0", func(t *testing.T) {
// 		csr := cstRegistry("s1", "s2", "s3")
// 		got, err := csr.getServers(0, nil)
// 		if err != nil || len(got) != 0 {
// 			t.Fatalf("want empty and nil, got %v and %v", got, err)
// 		}
// 	})

// 	t.Run("exclude list has an address outside the registry", func(t *testing.T) {
// 		csr := cstRegistry("s1", "s2", "s3")
// 		for i := 0; i < 50; i++ {
// 			got, err := csr.getServers(3, []string{"nope"})
// 			if err != nil {
// 				t.Fatalf("iteration %d: %v", i, err)
// 			}
// 			slices.Sort(got)
// 			if want := []string{"s1", "s2", "s3"}; !slices.Equal(got, want) {
// 				t.Fatalf("iteration %d: want %v, got %v", i, want, got)
// 			}
// 		}
// 	})
// }
