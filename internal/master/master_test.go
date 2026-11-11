package master

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/slothhcommits/Hive-FS/internal"
	"github.com/slothhcommits/Hive-FS/internal/hive"
	"github.com/slothhcommits/Hive-FS/internal/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// message types
// ============================================================================

var (
	tUpdateVersion = uint8(messages.Master_MESSAGE_TYPE_UPDATE_VERSION)
	tTakeLease     = uint8(messages.Master_MESSAGE_TYPE_TAKE_LEASE)
	tRevokeLease   = uint8(messages.Master_MESSAGE_TYPE_REVOKE_LEASE)
	tLocalCopy     = uint8(messages.Master_MESSAGE_TYPE_LOCAL_COPY_CHUNK)
	tReplicate     = uint8(messages.Master_MESSAGE_TYPE_REPLICATE_CHUNK)
	tDeleteChunk   = uint8(messages.Master_MESSAGE_TYPE_DELETE_CHUNK)

	tCreateFile = uint8(messages.Client_MESSAGE_TYPE_CREATE_FILE)
	tReadFile   = uint8(messages.Client_MESSAGE_TYPE_READ_FILE)
	tReplicas   = uint8(messages.Client_MESSAGE_TYPE_CHUNK_REPLICAS)
	tSnapshot   = uint8(messages.Client_MESSAGE_TYPE_SNAPSHOT)
	tMove       = uint8(messages.Client_MESSAGE_TYPE_MOVE)
	tList       = uint8(messages.Client_MESSAGE_TYPE_LIST)
	tDelete     = uint8(messages.Client_MESSAGE_TYPE_DELETE)
	tHeartbeat  = uint8(messages.Chunkserver_MESSAGE_TYPE_HEARTBEAT)
	tAck        = uint8(messages.MessageType_ACK)
)

func newUV() *messages.Master_UpdateVersion { return &messages.Master_UpdateVersion{} }
func newGL() *messages.Master_GrantLease    { return &messages.Master_GrantLease{} }
func newRL() *messages.Master_RevokeLease   { return &messages.Master_RevokeLease{} }
func newLC() *messages.Master_LocalCopy     { return &messages.Master_LocalCopy{} }
func newRep() *messages.Master_Replicate    { return &messages.Master_Replicate{} }
func newDel() *messages.Master_DeleteChunk  { return &messages.Master_DeleteChunk{} }

// ============================================================================
// fake chunkserver / peer / transport (in-memory, synctest friendly)
// ============================================================================

type recvMsg struct {
	typ     uint8
	payload []byte
	at      time.Time
}

// replyFn decides how a fake chunkserver answers one request.
type replyFn func(*hive.Message) (reply *hive.Message, delay time.Duration, drop bool)

func ackOK() replyFn {
	return func(*hive.Message) (*hive.Message, time.Duration, bool) {
		return internal.MakeAckMessage(nil), 0, false
	}
}

func nack() replyFn {
	return func(*hive.Message) (*hive.Message, time.Duration, bool) {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, errors.New("nack")), 0, false
	}
}

// after delivers the reply of fn after d.
func after(d time.Duration, fn replyFn) replyFn {
	return func(m *hive.Message) (*hive.Message, time.Duration, bool) {
		r, _, drop := fn(m)
		return r, d, drop
	}
}

type fakeCS struct {
	addr     string
	mu       sync.Mutex
	recv     []recvMsg
	handlers map[uint8]replyFn
}

func (f *fakeCS) on(typ uint8, fn replyFn) {
	f.mu.Lock()
	f.handlers[typ] = fn
	f.mu.Unlock()
}

func (f *fakeCS) record(msg *hive.Message) {
	f.mu.Lock()
	f.recv = append(f.recv, recvMsg{typ: msg.Type, payload: slices.Clone(msg.Payload), at: time.Now()})
	f.mu.Unlock()
}

func (f *fakeCS) handler(typ uint8) replyFn {
	f.mu.Lock()
	defer f.mu.Unlock()
	if fn, ok := f.handlers[typ]; ok {
		return fn
	}
	return ackOK()
}

func (f *fakeCS) of(typ uint8) []recvMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recvMsg
	for _, r := range f.recv {
		if r.typ == typ {
			out = append(out, r)
		}
	}
	return out
}

type fakePeer struct{ cs *fakeCS }

func (p *fakePeer) Send(_ context.Context, msg *hive.Message) error {
	p.cs.record(msg)
	return nil
}

func (p *fakePeer) Request(ctx context.Context, msg *hive.Message) (<-chan *hive.Message, error) {
	p.cs.record(msg)
	reply, delay, drop := p.cs.handler(msg.Type)(msg)
	ch := make(chan *hive.Message, 1)

	if !drop {
		select {
		case <-time.After(delay):
			ch <- reply
		case <-ctx.Done():
		}
	}

	return ch, nil
}

func (p *fakePeer) Unregister(*hive.Message) {}
func (p *fakePeer) RemoteAddr() string       { return p.cs.addr }
func (p *fakePeer) Close() error             { return nil }

type fakeTransport struct {
	mu      sync.Mutex
	servers map[string]*fakeCS
	reqCh   chan hive.Request
	done    chan struct{}
	once    sync.Once
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		servers: make(map[string]*fakeCS),
		reqCh:   make(chan hive.Request),
		done:    make(chan struct{}),
	}
}

func (t *fakeTransport) add(addr string) *fakeCS {
	cs := &fakeCS{addr: addr, handlers: make(map[uint8]replyFn)}
	t.mu.Lock()
	t.servers[addr] = cs
	t.mu.Unlock()
	return cs
}

func (t *fakeTransport) get(addr string) *fakeCS {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.servers[addr]
}

func (t *fakeTransport) ListenAndAccept(string) error { <-t.done; return nil }
func (t *fakeTransport) Consume() <-chan hive.Request { return t.reqCh }
func (t *fakeTransport) Streams() <-chan *hive.Stream { return nil }
func (t *fakeTransport) OpenStream(context.Context, string, *hive.Message) (*hive.Stream, error) {
	return nil, errors.New("fake: streams unsupported")
}
func (t *fakeTransport) Close() error {
	t.once.Do(func() { close(t.done) })
	return nil
}
func (t *fakeTransport) Dial(_ context.Context, addr string) (hive.Peer, error) {
	cs := t.get(addr)
	if cs == nil {
		return nil, fmt.Errorf("fake: no server %s", addr)
	}
	return &fakePeer{cs: cs}, nil
}

// ============================================================================
// env + helpers
// ============================================================================

type env struct {
	m     *master
	tr    *fakeTransport
	dir   string
	names []string
}

func servers(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("cs%d", i+1)
	}
	return out
}

func newEnvAt(t *testing.T, dir string, names ...string) *env {
	t.Helper()
	tr := newFakeTransport()
	m := &master{
		MasterOpts:      &MasterOpts{HivePath: dir},
		transport:       tr,
		persistentState: newPersistentState(dir),
		csr:             newChunkServerRegistry(),
		closeCh:         make(chan struct{}),
	}
	m.logger.open()
	t.Cleanup(func() { m.logger.logFile.Close() })

	e := &env{m: m, tr: tr, dir: dir, names: names}
	for i, n := range names {
		tr.add(n)
		m.csr.update(&messages.Chunkserver_Heartbeat{Addr: n, DiskUtil: uint32(i)})
	}
	return e
}

func newEnv(t *testing.T, names ...string) *env {
	t.Helper()
	return newEnvAt(t, t.TempDir(), names...)
}

func (e *env) cs(addr string) *fakeCS { return e.tr.get(addr) }

func (e *env) count(typ uint8) int {
	n := 0
	for _, a := range e.names {
		n += len(e.cs(a).of(typ))
	}
	return n
}

func (e *env) node(path string) *node {
	e.m.namespace.mu.RLock()
	defer e.m.namespace.mu.RUnlock()
	return e.m.namespace.entries[path]
}

func (e *env) chunk(id uint64) *chunk {
	e.m.chunkTable.mu.RLock()
	defer e.m.chunkTable.mu.RUnlock()
	return e.m.chunkTable.entries[id]
}

func (e *env) create(t *testing.T, path string) {
	t.Helper()
	requireOK(t, e.m.createFileHandler(createMsg(path)))
}

func (e *env) writeReq(path string, idx uint32, last bool) *hive.Message {
	return e.m.writeHandler(reqMsg(tReplicas, &messages.Client_GetChunkReplicas{Path: path, ChunkIdx: idx, LastChunk: last}))
}

func createMsg(path string) *hive.Message {
	return reqMsg(tCreateFile, &messages.Client_CreateFile{Path: path})
}

func readReq(path string, idx, count uint32) *hive.Message {
	return reqMsg(tReadFile, &messages.Client_Read{Path: path, Idx: idx, Count: count})
}

func reqMsg(typ uint8, p proto.Message) *hive.Message {
	b, err := proto.Marshal(p)
	if err != nil {
		panic(err)
	}
	return &hive.Message{Payload: b, Metadata: hive.Metadata{Type: typ, Size: uint32(len(b))}}
}

func decodeAck(t *testing.T, msg *hive.Message) *messages.Ack {
	t.Helper()
	require.NotNil(t, msg)
	require.Equal(t, tAck, msg.Type)
	ack := &messages.Ack{}
	require.NoError(t, proto.Unmarshal(msg.Payload, ack))
	return ack
}

func requireOK(t *testing.T, msg *hive.Message) {
	t.Helper()
	ack := decodeAck(t, msg)
	require.True(t, ack.Success, "unexpected error ack: %v", ack.Err)
}

func requireErr(t *testing.T, msg *hive.Message, code messages.Error_Code) {
	t.Helper()
	ack := decodeAck(t, msg)
	require.False(t, ack.Success)
	require.NotNil(t, ack.Err)
	assert.Equal(t, code, ack.Err.Code)
}

func ackData(t *testing.T, msg *hive.Message, out proto.Message) {
	t.Helper()
	ack := decodeAck(t, msg)
	require.True(t, ack.Success, "unexpected error ack: %v", ack.Err)
	require.NoError(t, proto.Unmarshal(ack.Data, out))
}

func chunkOf(t *testing.T, msg *hive.Message) *messages.Master_Chunk {
	t.Helper()
	c := &messages.Master_Chunk{}
	ackData(t, msg, c)
	return c
}

type rec[T any] struct {
	to  string
	at  time.Time
	msg T
}

func collect[T proto.Message](t *testing.T, e *env, typ uint8, mk func() T) []rec[T] {
	t.Helper()
	var out []rec[T]
	for _, addr := range e.names {
		for _, r := range e.cs(addr).of(typ) {
			m := mk()
			require.NoError(t, proto.Unmarshal(r.payload, m))
			out = append(out, rec[T]{to: addr, at: r.at, msg: m})
		}
	}
	return out
}

func keysOf(m map[string]time.Time) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func bubble(t *testing.T, f func(t *testing.T)) {
	t.Helper()
	defer goleak.VerifyNone(t)
	synctest.Test(t, f)
}

// ---------- persisted state comparison ----------

type nsView struct {
	Dir     bool
	Handles []uint64
}

type delView struct {
	At      int64
	Dir     bool
	Handles []uint64
}

type chView struct{ Ref, Version, Rep uint32 }

type stateView struct {
	NS  map[string]nsView
	Del map[string]delView
	CT  map[uint64]chView
}

func norm(h []uint64) []uint64 {
	out := make([]uint64, len(h))
	copy(out, h)
	return out
}

func viewState(p *persistentState) stateView {
	v := stateView{NS: map[string]nsView{}, Del: map[string]delView{}, CT: map[uint64]chView{}}

	p.namespace.mu.RLock()
	for k, n := range p.namespace.entries {
		v.NS[k] = nsView{Dir: n.dir, Handles: norm(n.chunkHandles)}
	}
	p.namespace.mu.RUnlock()

	p.namespace.deletedMu.Lock()
	for k, d := range p.namespace.deleted {
		v.Del[k] = delView{At: d.deletedAt, Dir: d.dir, Handles: norm(d.chunkHandles)}
	}
	p.namespace.deletedMu.Unlock()

	p.chunkTable.mu.RLock()
	for k, c := range p.chunkTable.entries {
		c.Lock()
		v.CT[k] = chView{Ref: c.ref, Version: c.version, Rep: c.repFactor}
		c.Unlock()
	}
	p.chunkTable.mu.RUnlock()

	return v
}

// assertPersisted rebuilds state from what is on disk and compares it to live state.
func assertPersisted(t *testing.T, e *env) {
	t.Helper()
	fresh := newPersistentState(e.dir)
	fresh.buildState()
	want := viewState(e.m.persistentState)
	got := viewState(fresh)
	require.Equal(t, want.NS, got.NS, "namespace") // assert
	require.Equal(t, want.Del, got.Del, "deleted")
	require.Equal(t, want.CT, got.CT, "chunktable") // assert
}

// ============================================================================
// updateVersion
// ============================================================================

func assertUVMsgs(t *testing.T, e *env, id uint64, version uint32, n int) {
	t.Helper()
	msgs := collect(t, e, tUpdateVersion, newUV)
	require.Len(t, msgs, n)
	for _, m := range msgs {
		assert.Equal(t, id, m.msg.Id)
		assert.Equal(t, version, m.msg.Version)
	}
}

func TestUpdateVersion_NoInTimeAck(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(4)...)
		c := newChunk(1)
		c.version = 3
		for _, s := range e.names {
			c.locations[s] = time.Now()
		}
		e.cs("cs1").on(tUpdateVersion, nack())
		e.cs("cs2").on(tUpdateVersion, after(3*time.Second, nack()))
		e.cs("cs3").on(tUpdateVersion, after(20*time.Second, ackOK()))
		e.cs("cs4").on(tUpdateVersion, after(40*time.Second, ackOK()))

		start := time.Now()
		acked, err := e.m.updateVersion(c)

		assert.Nil(t, acked)
		assert.ErrorIs(t, err, internal.ErrOpFailed)
		assert.Equal(t, versionAckTimeout, time.Since(start))
		assertUVMsgs(t, e, 1, 4, 4)
	})
}

func TestUpdateVersion_PartialAcks(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(6)...)
		c := newChunk(1)
		c.version = 3
		for _, s := range e.names {
			c.locations[s] = time.Now()
		}
		e.cs("cs1").on(tUpdateVersion, ackOK())
		e.cs("cs2").on(tUpdateVersion, after(5*time.Second, ackOK()))
		e.cs("cs3").on(tUpdateVersion, nack())
		e.cs("cs4").on(tUpdateVersion, after(3*time.Second, nack()))
		e.cs("cs5").on(tUpdateVersion, after(20*time.Second, ackOK()))
		e.cs("cs6").on(tUpdateVersion, after(30*time.Second, ackOK()))

		start := time.Now()
		acked, err := e.m.updateVersion(c)

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"cs1", "cs2"}, keysOf(acked))
		assert.True(t, acked["cs1"].Equal(start))
		assert.True(t, acked["cs2"].Equal(start.Add(5*time.Second)))
		assertUVMsgs(t, e, 1, 4, 6)
	})
}

// ============================================================================
// grantLease
// ============================================================================

func leaseChunk(e *env) *chunk {
	c := newChunk(1)
	c.version = 3
	for _, s := range e.names {
		c.locations[s] = time.Now()
	}
	return c
}

func TestGrantLease_UpdateVersionFails(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(3)...)
		c := leaseChunk(e)
		for _, s := range e.names {
			e.cs(s).on(tUpdateVersion, nack())
		}

		err := e.m.grantLease(c)

		assert.ErrorIs(t, err, internal.ErrOpFailed)
		assert.Equal(t, 0, e.count(tTakeLease))
		assert.Empty(t, c.primary)
	})
}

func TestGrantLease_LeaseRefused(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(3)...)
		c := leaseChunk(e)
		for _, s := range e.names {
			e.cs(s).on(tTakeLease, nack())
		}

		err := e.m.grantLease(c)

		assert.ErrorIs(t, err, internal.ErrOpFailed)
		assert.Equal(t, 3, e.count(tTakeLease))
		assert.Empty(t, c.primary)
	})
}

func TestGrantLease_OnlyOneServerGrants(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(4)...)
		c := leaseChunk(e)
		e.cs("cs1").on(tUpdateVersion, nack())
		e.cs("cs2").on(tTakeLease, nack())
		e.cs("cs4").on(tTakeLease, nack())

		require.NoError(t, e.m.grantLease(c))

		assert.Equal(t, "cs3", c.primary)
		assert.True(t, c.leaseGrantTime.Equal(time.Now()))
		assert.ElementsMatch(t, []string{"cs2", "cs3", "cs4"}, keysOf(c.locations))
		assert.Empty(t, e.cs("cs1").of(tTakeLease))

		leases := collect(t, e, tTakeLease, newGL)
		var toCS3 []rec[*messages.Master_GrantLease]
		for _, l := range leases {
			assert.Equal(t, uint64(1), l.msg.Id)
			if l.to == "cs3" {
				toCS3 = append(toCS3, l)
			}
		}
		assert.Len(t, toCS3, 1)
	})
}

// ============================================================================
// addNewChunk
// ============================================================================

func TestAddNewChunk_Happy(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(4)...)
		e.create(t, "/a/f")
		f := e.node("/a/f")
		require.NotNil(t, f)

		require.NoError(t, e.m.addNewChunk("/a/f", f))

		require.Len(t, f.chunkHandles, 1)
		c := e.chunk(f.chunkHandles[0])
		require.NotNil(t, c)
		assert.Equal(t, uint32(1), c.ref)
		assert.Equal(t, uint32(1), c.version)
		assert.Equal(t, defaultRepFactor, c.repFactor)
		assert.Len(t, c.locations, 3)
		assert.Contains(t, c.locations, c.primary)
		assert.NotEmpty(t, c.primary)
		assert.Empty(t, e.m.chunkTable.stagedCreate)
		assert.Equal(t, 3, e.count(tUpdateVersion))
		assert.Equal(t, 1, e.count(tTakeLease))
		assertPersisted(t, e)
	})
}

// ============================================================================
// copyOnWrite
// ============================================================================

func TestCopyOnWrite_PartialAcks(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newEnv(t, servers(10)...)
		old := newChunk(100)
		old.version = 4
		old.ref = 2
		old.repFactor = 10
		for _, s := range e.names {
			old.locations[s] = time.Now()
		}
		e.m.chunkHandle.Store(100)
		for i := 1; i <= 5; i++ {
			e.cs(fmt.Sprintf("cs%d", i)).on(tLocalCopy, nack())
		}

		nc, err := e.m.copyOnWrite(old)

		require.NoError(t, err)
		assert.Equal(t, uint64(101), nc.id)
		assert.Equal(t, uint32(4), nc.version)
		assert.Equal(t, uint32(10), nc.repFactor)
		assert.Equal(t, uint32(1), nc.ref)
		assert.ElementsMatch(t, []string{"cs6", "cs7", "cs8", "cs9", "cs10"}, keysOf(nc.locations))
		assert.Len(t, old.locations, 10)
		assert.Contains(t, e.m.chunkTable.stagedCreate, nc.id)

		copies := collect(t, e, tLocalCopy, newLC)
		require.Len(t, copies, 10)
		seen := map[string]bool{}
		for _, c := range copies {
			assert.Equal(t, uint64(100), c.msg.SrcId)
			assert.Equal(t, uint64(101), c.msg.DstId)
			seen[c.to] = true
		}
		assert.Len(t, seen, 10)
	})
}

// ============================================================================
// writeHandler
// ============================================================================

func newWriteEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t, servers(4)...)
	e.create(t, "/d/f")
	return e
}

func TestWriteHandler_CreatesChunk(t *testing.T) {
	e := newWriteEnv(t)

	res := chunkOf(t, e.writeReq("/d/f", 0, false))

	f := e.node("/d/f")
	require.Len(t, f.chunkHandles, 1)
	c := e.chunk(f.chunkHandles[0])
	require.NotNil(t, c)
	assert.Equal(t, c.id, res.ChunkId)
	assert.Equal(t, uint32(0), res.ChunkIdx)
	assert.Equal(t, uint32(1), res.Version)
	assert.NotEmpty(t, res.Primary)
	assert.Equal(t, c.primary, res.Primary)
	assert.Len(t, res.Locations, 3)
	assert.Contains(t, res.Locations, res.Primary)
	assert.Equal(t, c.leaseGrantTime.Add(leaseDeadline).Unix(), res.LeaseExpiry)
	assert.Equal(t, uint32(1), c.ref)
	assert.Equal(t, uint32(1), c.version)
	assert.Empty(t, e.m.chunkTable.stagedCreate)

	uvs := collect(t, e, tUpdateVersion, newUV)
	require.Len(t, uvs, 3)
	for _, u := range uvs {
		assert.Equal(t, c.id, u.msg.Id)
		assert.Equal(t, uint32(1), u.msg.Version)
	}
	assert.Equal(t, 1, e.count(tTakeLease))
	assertPersisted(t, e)
}

func TestWriteHandler_LastChunk(t *testing.T) {
	e := newWriteEnv(t)
	chunkOf(t, e.writeReq("/d/f", 0, false))
	chunkOf(t, e.writeReq("/d/f", 1, false))
	handles := e.node("/d/f").chunkHandles
	require.Len(t, handles, 2)

	res := chunkOf(t, e.writeReq("/d/f", 0, true))

	assert.Equal(t, uint32(1), res.ChunkIdx)
	assert.Equal(t, handles[1], res.ChunkId)
	assert.Len(t, e.node("/d/f").chunkHandles, 2)

	// last chunk of an empty file creates chunk 0
	e.create(t, "/d/g")
	res = chunkOf(t, e.writeReq("/d/g", 0, true))
	assert.Equal(t, uint32(0), res.ChunkIdx)
	assert.Len(t, e.node("/d/g").chunkHandles, 1)
	assertPersisted(t, e)
}

func TestWriteHandler_LiveLease(t *testing.T) {
	e := newWriteEnv(t)
	first := chunkOf(t, e.writeReq("/d/f", 0, false))
	uv, tl := e.count(tUpdateVersion), e.count(tTakeLease)

	second := chunkOf(t, e.writeReq("/d/f", 0, false))

	assert.Equal(t, first.ChunkId, second.ChunkId)
	assert.Equal(t, first.Primary, second.Primary)
	assert.Equal(t, first.Version, second.Version)
	assert.Equal(t, first.LeaseExpiry, second.LeaseExpiry)
	assert.Equal(t, uv, e.count(tUpdateVersion))
	assert.Equal(t, tl, e.count(tTakeLease))
	assertPersisted(t, e)
}

func TestWriteHandler_ExpiredLease(t *testing.T) {
	bubble(t, func(t *testing.T) {
		e := newWriteEnv(t)
		first := chunkOf(t, e.writeReq("/d/f", 0, false))
		time.Sleep(leaseDeadline + time.Second)
		tl := e.count(tTakeLease)

		res := chunkOf(t, e.writeReq("/d/f", 0, false))

		assert.Equal(t, first.ChunkId, res.ChunkId)
		assert.Equal(t, uint32(2), res.Version)
		assert.NotEmpty(t, res.Primary)
		assert.Contains(t, res.Locations, res.Primary)
		assert.Equal(t, time.Now().Add(leaseDeadline).Unix(), res.LeaseExpiry)
		assert.Equal(t, uint32(2), e.chunk(first.ChunkId).version)
		assert.Equal(t, tl+1, e.count(tTakeLease))

		var v1, v2 int
		for _, u := range collect(t, e, tUpdateVersion, newUV) {
			switch u.msg.Version {
			case 1:
				v1++
			case 2:
				v2++
			}
		}
		assert.Equal(t, 3, v1)
		assert.Equal(t, 3, v2)
		assertPersisted(t, e)
	})
}

func TestWriteHandler_CopyOnWrite(t *testing.T) {
	e := newWriteEnv(t)
	first := chunkOf(t, e.writeReq("/d/f", 0, false))
	requireOK(t, e.m.snapshotHandler(reqMsg(tSnapshot, &messages.Client_Snapshot{Src: "/d/f", Dst: "/d/g"})))
	require.Equal(t, uint32(2), e.chunk(first.ChunkId).ref)

	res := chunkOf(t, e.writeReq("/d/f", 0, false))

	assert.NotEqual(t, first.ChunkId, res.ChunkId)
	assert.Equal(t, uint32(2), res.Version)
	assert.NotEmpty(t, res.Primary)
	assert.Equal(t, []uint64{res.ChunkId}, e.node("/d/f").chunkHandles)
	assert.Equal(t, []uint64{first.ChunkId}, e.node("/d/g").chunkHandles)
	assert.Equal(t, uint32(1), e.chunk(first.ChunkId).ref)
	nc := e.chunk(res.ChunkId)
	require.NotNil(t, nc)
	assert.Equal(t, uint32(1), nc.ref)
	assert.Equal(t, uint32(2), nc.version)
	assert.Empty(t, e.m.chunkTable.stagedCreate)

	copies := collect(t, e, tLocalCopy, newLC)
	require.Len(t, copies, 3)
	for _, c := range copies {
		assert.Equal(t, first.ChunkId, c.msg.SrcId)
		assert.Equal(t, res.ChunkId, c.msg.DstId)
	}
	assertPersisted(t, e)
}

func TestWriteHandler_SparseWrite(t *testing.T) {
	e := newWriteEnv(t)

	requireErr(t, e.writeReq("/d/f", 1, false), messages.Error_CODE_BAD_REQUEST)

	assert.Empty(t, e.node("/d/f").chunkHandles)
	assert.Equal(t, 0, e.count(tUpdateVersion))
	assertPersisted(t, e)
}

// ============================================================================
// snapshotHandler
// ============================================================================

func TestSnapshotHandler_Dir(t *testing.T) {
	e := newEnv(t, servers(4)...)
	e.create(t, "/s/a")
	e.create(t, "/s/sub/b")
	paths := []string{"/s/a", "/s/sub/b"}
	var ids []uint64
	for _, p := range paths {
		ids = append(ids, chunkOf(t, e.writeReq(p, 0, false)).ChunkId)
	}

	requireOK(t, e.m.snapshotHandler(reqMsg(tSnapshot, &messages.Client_Snapshot{Src: "/s", Dst: "/t"})))

	// namespace
	for src, dst := range map[string]string{"/s": "/t", "/s/a": "/t/a", "/s/sub": "/t/sub", "/s/sub/b": "/t/sub/b"} {
		s, d := e.node(src), e.node(dst)
		require.NotNil(t, s, src)
		require.NotNil(t, d, dst)
		assert.Equal(t, s.dir, d.dir)
		assert.Equal(t, s.chunkHandles, d.chunkHandles)
	}
	// refs
	for _, id := range ids {
		assert.Equal(t, uint32(2), e.chunk(id).ref)
	}
	// lease revocation on each live primary
	want := map[string][]uint64{}
	for _, id := range ids {
		p := e.chunk(id).primary
		want[p] = append(want[p], id)
	}
	for _, s := range e.names {
		revs := e.cs(s).of(tRevokeLease)
		exp, ok := want[s]
		if !ok {
			assert.Empty(t, revs, s)
			continue
		}
		slices.Sort(exp)
		require.Len(t, revs, 1, s)
		rl := &messages.Master_RevokeLease{}
		require.NoError(t, proto.Unmarshal(revs[0].payload, rl))
		assert.Equal(t, exp, rl.Handles)
	}
	assertPersisted(t, e)
}

// ============================================================================
// moveHandler
// ============================================================================

func TestMoveHandler(t *testing.T) {
	e := newEnv(t)
	e.create(t, "/m/f")
	e.create(t, "/m/d/x")
	e.create(t, "/m/g")
	move := func(src, dst string) {
		requireOK(t, e.m.moveHandler(reqMsg(tMove, &messages.Client_Move{Src: src, Dst: dst})))
	}

	t.Run("file to dir", func(t *testing.T) {
		move("/m/f", "/m/d")
		assert.Nil(t, e.node("/m/f"))
		require.NotNil(t, e.node("/m/d/f"))
		assert.False(t, e.node("/m/d/f").dir)
		assertPersisted(t, e)
	})

	t.Run("rename file", func(t *testing.T) {
		move("/m/g", "/m/h")
		assert.Nil(t, e.node("/m/g"))
		require.NotNil(t, e.node("/m/h"))
		assert.False(t, e.node("/m/h").dir)
		assertPersisted(t, e)
	})

	t.Run("rename dir", func(t *testing.T) {
		move("/m/d", "/m/e")
		for _, p := range []string{"/m/d", "/m/d/x", "/m/d/f"} {
			assert.Nil(t, e.node(p), p)
		}
		require.NotNil(t, e.node("/m/e"))
		assert.True(t, e.node("/m/e").dir)
		for _, p := range []string{"/m/e/x", "/m/e/f"} {
			require.NotNil(t, e.node(p), p)
			assert.False(t, e.node(p).dir)
		}
		assertPersisted(t, e)
	})
}

// ============================================================================
// deleteHandler
// ============================================================================

func TestDeleteHandler_Dir(t *testing.T) {
	e := newEnv(t, servers(4)...)
	e.create(t, "/x/a")
	e.create(t, "/x/b/c")
	e.create(t, "/keep")
	id := chunkOf(t, e.writeReq("/x/a", 0, false)).ChunkId

	requireOK(t, e.m.deleteHandler(reqMsg(tDelete, &messages.Client_Delete{Path: "/x"})))

	for _, p := range []string{"/x", "/x/a", "/x/b", "/x/b/c"} {
		assert.Nil(t, e.node(p), p)
	}
	assert.NotNil(t, e.node("/keep"))

	byPath := map[string]*deletedNode{}
	var at int64
	for key, d := range e.m.namespace.deleted {
		i := strings.LastIndex(key, "/")
		byPath[key[:i]] = d
		if at == 0 {
			at = d.deletedAt
		}
		assert.Equal(t, at, d.deletedAt)
		assert.Equal(t, key[i+1:], fmt.Sprint(d.deletedAt))
	}
	require.Len(t, byPath, 4)
	assert.True(t, byPath["/x"].dir)
	assert.True(t, byPath["/x/b"].dir)
	assert.False(t, byPath["/x/a"].dir)
	assert.False(t, byPath["/x/b/c"].dir)
	assert.Equal(t, []uint64{id}, byPath["/x/a"].chunkHandles)
	assertPersisted(t, e)
}

// ============================================================================
// listHandler
// ============================================================================

func TestListHandler(t *testing.T) {
	e := newEnv(t)
	e.create(t, "/l/a")
	e.create(t, "/l/b/c")
	e.create(t, "/other/z")

	var res messages.Master_Subtree
	ackData(t, e.m.listHandler(reqMsg(tList, &messages.Client_List{Path: "/l"})), &res)

	assert.ElementsMatch(t, []string{"/l/a", "/l/b/c"}, res.Contents)
}

// ============================================================================
// createFileHandler
// ============================================================================

func TestCreateFileHandler(t *testing.T) {
	e := newEnv(t)

	t.Run("create", func(t *testing.T) {
		requireOK(t, e.m.createFileHandler(createMsg("/c/f")))
		require.NotNil(t, e.node("/"))
		assert.True(t, e.node("/").dir)
		require.NotNil(t, e.node("/c"))
		assert.True(t, e.node("/c").dir)
		require.NotNil(t, e.node("/c/f"))
		assert.False(t, e.node("/c/f").dir)
		assertPersisted(t, e)
	})

	t.Run("existing", func(t *testing.T) {
		requireErr(t, e.m.createFileHandler(createMsg("/c/f")), messages.Error_CODE_EXISTS)
		assertPersisted(t, e)
	})
}

func TestCreateFileHandler_Root(t *testing.T) {
	e := newEnv(t)

	requireErr(t, e.m.createFileHandler(createMsg("/")), messages.Error_CODE_BAD_REQUEST)

	assert.Empty(t, e.m.namespace.entries)
}

// ============================================================================
// readFileHandler
// ============================================================================

func TestReadFileHandler(t *testing.T) {
	e := newEnv(t, servers(4)...)
	e.create(t, "/r/f")
	for i := uint32(0); i < 3; i++ {
		requireOK(t, e.writeReq("/r/f", i, false))
	}
	handles := e.node("/r/f").chunkHandles
	require.Len(t, handles, 3)

	t.Run("count zero", func(t *testing.T) {
		requireErr(t, e.m.readFileHandler(readReq("/r/f", 0, 0)), messages.Error_CODE_BAD_REQUEST)
	})
	t.Run("idx equals len", func(t *testing.T) {
		requireErr(t, e.m.readFileHandler(readReq("/r/f", 3, 1)), messages.Error_CODE_BAD_REQUEST)
	})
	t.Run("path is dir", func(t *testing.T) {
		requireErr(t, e.m.readFileHandler(readReq("/r", 0, 1)), messages.Error_CODE_BAD_REQUEST)
	})
	t.Run("path missing", func(t *testing.T) {
		requireErr(t, e.m.readFileHandler(readReq("/r/nope", 0, 1)), messages.Error_CODE_NOT_FOUND)
	})

	check := func(t *testing.T, idx, count uint32, wantN int) {
		t.Helper()
		var f messages.Master_File
		ackData(t, e.m.readFileHandler(readReq("/r/f", idx, count)), &f)
		require.Len(t, f.Chunks, wantN)
		for i, c := range f.Chunks {
			assert.Equal(t, handles[int(idx)+i], c.ChunkId)
			assert.Equal(t, idx+uint32(i), c.ChunkIdx)
			assert.Equal(t, uint32(1), c.Version)
			assert.Len(t, c.Locations, 3)
		}
	}

	t.Run("clamp at end", func(t *testing.T) { check(t, 2, 2, 1) })
	t.Run("whole file", func(t *testing.T) { check(t, 0, 3, 3) })
	t.Run("count over len", func(t *testing.T) { check(t, 0, 4, 3) })
}

// ============================================================================
// heartbeatHandler
// ============================================================================

func TestHeartbeatHandler(t *testing.T) {
	e := newEnv(t)
	m := e.m
	const addr = "cs-hb"

	add := func(id uint64, version uint32, primary string) *chunk {
		c := newChunk(id)
		c.version = version
		c.primary = primary
		m.chunkTable.entries[id] = c
		return c
	}
	c1 := add(1, 5, "")
	c2 := add(2, 5, "")
	c3 := add(3, 5, "")
	c6 := add(6, 5, addr)
	c7 := add(7, 5, "other")
	c6.leaseGrantTime = time.Now().Add(-30 * time.Second)
	m.chunkTable.stagedCreate[5] = struct{}{}

	hb := &messages.Chunkserver_Heartbeat{
		Addr:     addr,
		DiskUtil: 42,
		Chunks: []*messages.Chunkserver_ChunkInfo{
			{Id: 1, Version: 5},                    // equal
			{Id: 2, Version: 6},                    // higher
			{Id: 3, Version: 4},                    // lower
			{Id: 4, Version: 1},                    // unknown
			{Id: 5, Version: 1},                    // staged create
			{Id: 6, Version: 5, ExtendLease: true}, // primary
			{Id: 7, Version: 5, ExtendLease: true}, // not primary
		},
	}

	res := m.heartbeatHandler(reqMsg(tHeartbeat, hb))

	assert.Equal(t, uint8(messages.Master_MESSAGE_TYPE_HEARTBEAT_RES), res.Type)
	out := &messages.Master_HeartbeatRes{}
	require.NoError(t, proto.Unmarshal(res.Payload, out))
	assert.ElementsMatch(t, []uint64{3, 4}, out.Delete)
	require.Len(t, out.UpdateVersion, 1)
	assert.Equal(t, uint64(2), out.UpdateVersion[0].Id)
	assert.Equal(t, uint32(5), out.UpdateVersion[0].Version)
	assert.Equal(t, []uint64{6}, out.ExtendLease)
	assert.Equal(t, []uint64{7}, out.RevokeLease)

	for _, c := range []*chunk{c1, c2, c6, c7} {
		assert.Contains(t, c.locations, addr, c.id)
	}
	assert.NotContains(t, c3.locations, addr)
	assert.WithinDuration(t, time.Now(), c6.leaseGrantTime, time.Second)

	cs := m.csr.entries[addr]
	require.NotNil(t, cs)
	assert.Equal(t, uint32(42), cs.state.DiskUtil)
	assert.WithinDuration(t, time.Now(), cs.lastBeat, time.Second)
}

// ============================================================================
// reReplicate
// ============================================================================

func TestReReplicate(t *testing.T) {
	bubble(t, func(t *testing.T) {
		names := servers(8)
		e := newEnv(t, names...)
		m := e.m
		start := time.Now()

		mk := func(id uint64, ref, rep uint32, primary string, locs ...string) *chunk {
			c := newChunk(id)
			c.ref = ref
			c.repFactor = rep
			c.version = 2
			c.primary = primary
			for _, l := range locs {
				c.locations[l] = time.Now()
			}
			m.chunkTable.entries[id] = c
			return c
		}
		mk(1, 1, 3, "", "cs1")                         // under, needs 2
		mk(2, 1, 2, "", "cs1", "cs2")                  // rep == locations
		mk(3, 1, 2, "cs1", "cs1", "cs2", "cs3", "cs4") // over by 2
		mk(4, 0, 3, "", "cs1")                         // ref == 0
		c5 := mk(5, 1, 3, "")                          // only stale location
		c5.locations["cs5"] = time.Now().Add(-2 * chunkServerDeadline)
		mk(6, 1, 3, "", "cs1", "cs2") // under, needs 1

		refresh := func() {
			for _, s := range names {
				m.csr.update(&messages.Chunkserver_Heartbeat{Addr: s})
			}
			for _, c := range m.chunkTable.entries {
				c.Lock()
				for l := range c.locations {
					c.locations[l] = time.Now()
				}
				c.Unlock()
			}
		}
		pending := func() map[uint64]map[string]time.Time {
			m.chunkTable.repRequestsMu.Lock()
			defer m.chunkTable.repRequestsMu.Unlock()
			out := map[uint64]map[string]time.Time{}
			for h, r := range m.chunkTable.repRequests {
				out[h] = map[string]time.Time{}
				for a, ts := range r {
					out[h][a] = ts
				}
			}
			return out
		}

		m.wg.Add(1)
		go m.reReplicate()
		synctest.Wait()

		// ---- iteration 1 (t=0)
		reps := collect(t, e, tReplicate, newRep)
		by := map[uint64][]rec[*messages.Master_Replicate]{}
		for _, r := range reps {
			by[r.msg.Handle] = append(by[r.msg.Handle], r)
			assert.Equal(t, uint32(2), r.msg.Version)
			assert.True(t, r.at.Equal(start))
		}
		assert.Len(t, reps, 3)
		require.Len(t, by[1], 2)
		dsts := map[string]bool{}
		for _, r := range by[1] {
			assert.NotEqual(t, "cs1", r.to)
			assert.Equal(t, "cs1", r.msg.Src)
			dsts[r.to] = true
		}
		assert.Len(t, dsts, 2)
		require.Len(t, by[6], 1)
		assert.NotContains(t, []string{"cs1", "cs2"}, by[6][0].to)
		assert.Contains(t, []string{"cs1", "cs2"}, by[6][0].msg.Src)

		dels := collect(t, e, tDeleteChunk, newDel)
		require.Len(t, dels, 2)
		delTo := map[string]bool{}
		for _, d := range dels {
			assert.Equal(t, uint64(3), d.msg.Handle)
			assert.Contains(t, []string{"cs2", "cs3", "cs4"}, d.to) // never the primary
			delTo[d.to] = true
		}
		assert.Len(t, delTo, 2)
		assert.Len(t, m.chunkTable.entries[3].locations, 2)
		assert.Contains(t, m.chunkTable.entries[3].locations, "cs1")
		assert.Empty(t, m.chunkTable.entries[5].locations)

		pend := pending()
		assert.Len(t, pend, 2)
		assert.Len(t, pend[1], 2)
		assert.Len(t, pend[6], 1)
		for _, ts := range pend[1] {
			assert.True(t, ts.Equal(start))
		}

		// ---- iterations 2 and 3 (t=30, 60): pending requests are excluded
		for i := 1; i <= 2; i++ {
			time.Sleep(29 * time.Second)
			synctest.Wait()
			refresh()
			time.Sleep(time.Second)
			synctest.Wait()

			assert.Equal(t, time.Duration(i)*30*time.Second, time.Since(start))
			assert.Len(t, collect(t, e, tReplicate, newRep), 3)
			assert.Len(t, collect(t, e, tDeleteChunk, newDel), 2)
			pend = pending()
			assert.Len(t, pend[1], 2)
			for _, ts := range pend[1] {
				assert.True(t, ts.Equal(start))
			}
		}

		// ---- iteration 4 (t=90): old requests expired, new ones fired
		time.Sleep(29 * time.Second)
		synctest.Wait()
		refresh()
		time.Sleep(time.Second)
		synctest.Wait()

		assert.Equal(t, 90*time.Second, time.Since(start))
		reps = collect(t, e, tReplicate, newRep)
		assert.Len(t, reps, 6)
		var fresh []rec[*messages.Master_Replicate]
		for _, r := range reps {
			if r.at.Equal(start.Add(90 * time.Second)) {
				fresh = append(fresh, r)
			}
		}
		assert.Len(t, fresh, 3)
		assert.Len(t, collect(t, e, tDeleteChunk, newDel), 2)
		pend = pending()
		assert.Len(t, pend[1], 2)
		assert.Len(t, pend[6], 1)
		for _, ts := range pend[1] {
			assert.True(t, ts.Equal(start.Add(90*time.Second)))
		}

		m.Close()
		m.wg.Wait()
	})
}

// ============================================================================
// garbageCollector
// ============================================================================

const (
	gcShared = 1100
	gcUnique = 2000
	gcFiles  = 11
	gcYoung  = 2
	gcYoungN = 50
	gcBudget = 10 * 1000
)

type gcSnap struct {
	deleted map[string][]uint64
	at      map[string]int64
	refs    map[uint64]uint32
}

func takeGC(m *master) gcSnap {
	s := gcSnap{deleted: map[string][]uint64{}, at: map[string]int64{}, refs: map[uint64]uint32{}}
	m.namespace.deletedMu.Lock()
	for k, d := range m.namespace.deleted {
		s.deleted[k] = norm(d.chunkHandles)
		s.at[k] = d.deletedAt
	}
	m.namespace.deletedMu.Unlock()
	m.chunkTable.mu.RLock()
	for k, c := range m.chunkTable.entries {
		c.Lock()
		s.refs[k] = c.ref
		c.Unlock()
	}
	m.chunkTable.mu.RUnlock()
	return s
}

func verifyGCStep(t *testing.T, before, after gcSnap, now time.Time) {
	t.Helper()

	consumed := map[uint64]uint32{}
	consumedEntries, expiredEntries := 0, 0
	exhausted := false

	for key, orig := range before.deleted {
		rem, still := after.deleted[key]
		if now.Sub(time.Unix(0, before.at[key])) <= nodeExpiryWindow {
			require.True(t, still, key)
			assert.Equal(t, orig, rem, key)
			continue
		}
		expiredEntries++
		n := len(orig)
		if still {
			n = len(orig) - len(rem)
			require.GreaterOrEqual(t, n, 0, key)
			require.Equal(t, orig[n:], rem, key)
			if len(rem) > 0 {
				exhausted = true
			}
		}
		if n > 0 {
			consumedEntries++
		}
		for _, h := range orig[:n] {
			if _, ok := before.refs[h]; ok {
				consumed[h]++
			}
		}
	}

	// budget: distinct handles + entries, the last entry may overshoot by one
	used := len(consumed) + consumedEntries
	assert.LessOrEqual(t, used, gcBudget+1) // assert
	if exhausted {
		assert.GreaterOrEqual(t, used, gcBudget-expiredEntries)
	}

	// ref math and chunktable sweep
	exp := map[uint64]uint32{}
	var zeros []uint64
	for h, r := range before.refs {
		var e uint32
		if r > consumed[h] {
			e = r - consumed[h]
		}
		exp[h] = e
		if e == 0 {
			zeros = append(zeros, h)
		}
	}
	slices.Sort(zeros)
	survivors := max(0, len(zeros)-gcBudget)
	removed := zeros[:len(zeros)-survivors]
	kept := zeros[len(zeros)-survivors:]

	live := 0
	for h, e := range exp {
		if e > 0 {
			live++
			got, ok := after.refs[h]
			require.True(t, ok, h)
			require.Equal(t, e, got, h)
		}
	}
	for _, h := range removed {
		_, ok := after.refs[h]
		require.False(t, ok, h)
	}
	for _, h := range kept {
		got, ok := after.refs[h]
		require.True(t, ok, h)
		require.Equal(t, uint32(0), got, h)
	}
	assert.Len(t, after.refs, live+survivors) // assert
}

func TestGarbageCollector(t *testing.T) {
	bubble(t, func(t *testing.T) {
		old := maxLogSize
		maxLogSize = 1 << 40
		t.Cleanup(func() { maxLogSize = old })

		e := newEnv(t)
		m := e.m

		type file struct {
			path    string
			handles []uint64
		}
		refs := map[uint64]uint32{}
		var oldFiles, youngFiles []file

		for h := uint64(1); h <= gcShared; h++ {
			refs[h] = gcFiles
		}
		for i := 0; i < gcFiles; i++ {
			f := file{path: fmt.Sprintf("/gc/f%d", i)}
			for h := uint64(1); h <= gcShared; h++ {
				f.handles = append(f.handles, h)
			}
			base := uint64(10000 + i*gcUnique)
			for j := uint64(1); j <= gcUnique; j++ {
				f.handles = append(f.handles, base+j)
				refs[base+j] = 1
			}
			oldFiles = append(oldFiles, f)
		}
		for i := 0; i < gcYoung; i++ {
			f := file{path: fmt.Sprintf("/gc/y%d", i)}
			base := uint64(500000 + i*gcYoungN)
			for j := uint64(1); j <= gcYoungN; j++ {
				f.handles = append(f.handles, base+j)
				refs[base+j] = 1
			}
			youngFiles = append(youngFiles, f)
		}

		tx := newTransaction()
		for h, r := range refs {
			c := newChunk(h)
			c.ref = r
			c.version = 1
			m.chunkTable.addChunk(tx, c)
		}
		m.commitTransaction(tx)

		mkFile := func(f file) {
			tx := newTransaction()
			require.NoError(t, m.namespace.create(tx, f.path))
			m.commitTransaction(tx)
			n := e.node(f.path)
			for _, h := range f.handles {
				tx = newTransaction()
				n.appendChunkHandle(tx, f.path, h)
				m.commitTransaction(tx)
			}
		}
		rm := func(f file) {
			tx := newTransaction()
			require.NoError(t, m.namespace.delete(tx, f.path))
			m.commitTransaction(tx)
		}

		for _, f := range oldFiles {
			mkFile(f)
		}
		for _, f := range youngFiles {
			mkFile(f)
		}
		for _, f := range oldFiles {
			rm(f)
		}
		time.Sleep(nodeExpiryWindow + time.Second)
		for _, f := range youngFiles {
			rm(f)
		}
		require.Len(t, takeGC(m).deleted, gcFiles+gcYoung)

		before := takeGC(m)
		m.wg.Add(1)
		go m.garbageCollector()

		for iter := 1; iter <= 8; iter++ {
			synctest.Wait()
			after := takeGC(m)
			verifyGCStep(t, before, after, time.Now())
			assertPersisted(t, e) // failed
			before = after
			time.Sleep(gcInterval)
		}

		assert.Empty(t, before.deleted)
		assert.Empty(t, before.refs)

		m.Close()
		m.wg.Wait()
	})
}

// ============================================================================
// Boot / Close (real TCP, real time)
// ============================================================================

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func startBoot(m *master) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Boot(ctx) }()
	return cancel, done
}

func dialMaster(t *testing.T, addr string) (*hive.TCPTransport, hive.Peer) {
	t.Helper()
	client := hive.NewTCPTransport()
	var peer hive.Peer
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p, err := client.Dial(ctx, addr)
		if err != nil {
			return false
		}
		peer = p
		return true
	}, 5*time.Second, 20*time.Millisecond)
	return client, peer
}

func wireCreate(t *testing.T, peer hive.Peer, path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := peer.Request(ctx, createMsg(path))
	require.NoError(t, err)
	select {
	case res := <-ch:
		requireOK(t, res)
	case <-ctx.Done():
		t.Fatal("no response to create request")
	}
}

func waitDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Boot did not return")
	}
}

func waitCheckpoint(t *testing.T, dir string) {
	t.Helper()
	require.Eventually(t, func() bool {
		files, err := os.ReadDir(dir)
		if err != nil {
			return false
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".cp") && f.Name() != "temp.cp" {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
}

func hasNode(m *master, path string) bool {
	m.namespace.mu.RLock()
	defer m.namespace.mu.RUnlock()
	_, ok := m.namespace.entries[path]
	return ok
}

func TestBoot_ExistingCheckpointAndLogs(t *testing.T) {
	defer goleak.VerifyNone(t)

	dir := t.TempDir()
	seed := newEnvAt(t, dir, servers(3)...)
	seed.create(t, "/b/f1")
	seed.create(t, "/b/f2")
	requireOK(t, seed.writeReq("/b/f1", 0, false))

	// force a checkpoint rotation on the next commit, then return to normal size
	old := maxLogSize
	t.Cleanup(func() { maxLogSize = old })
	maxLogSize = 1
	seed.create(t, "/b/f3")
	maxLogSize = old
	seed.create(t, "/b/f4")
	waitCheckpoint(t, dir)

	want := viewState(seed.m.persistentState)
	require.NoError(t, seed.m.logger.logFile.Close())

	addr := freeAddr(t)
	m := NewMaster(&MasterOpts{ListenAddr: addr, HivePath: dir})
	cancel, done := startBoot(m)
	defer cancel()

	client, peer := dialMaster(t, addr)
	defer client.Close()

	assert.Equal(t, want, viewState(m.persistentState))

	wireCreate(t, peer, "/b/new")
	assert.True(t, hasNode(m, "/b/new"))

	start := time.Now()
	cancel()
	waitDone(t, done)
	assert.Less(t, time.Since(start), 3*time.Second)
}

func TestBoot_NoHiveDir(t *testing.T) {
	defer goleak.VerifyNone(t)

	home := t.TempDir()
	t.Setenv("HOME", home)
	hivePath := filepath.Join(home, ".hiveFS")
	require.NoDirExists(t, hivePath)

	addr := freeAddr(t)
	m := NewMaster(&MasterOpts{ListenAddr: addr})
	assert.Equal(t, hivePath, m.HivePath)
	cancel, done := startBoot(m)
	defer cancel()

	client, peer := dialMaster(t, addr)
	defer client.Close()

	require.DirExists(t, hivePath)
	wireCreate(t, peer, "/n/f")
	assert.True(t, hasNode(m, "/n/f"))

	files, err := os.ReadDir(hivePath)
	require.NoError(t, err)
	var logSize int64
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			info, err := f.Info()
			require.NoError(t, err)
			logSize += info.Size()
		}
	}
	assert.Greater(t, logSize, int64(0))

	start := time.Now()
	cancel()
	waitDone(t, done)
	assert.Less(t, time.Since(start), 3*time.Second)
}

func TestClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	dir := t.TempDir()
	addr := freeAddr(t)
	m := NewMaster(&MasterOpts{ListenAddr: addr, HivePath: dir})
	cancel, done := startBoot(m)
	defer cancel()

	client, peer := dialMaster(t, addr)
	defer client.Close()
	wireCreate(t, peer, "/c/f1")
	wireCreate(t, peer, "/c/f2")
	want := viewState(m.persistentState)

	start := time.Now()
	require.NoError(t, m.Close())
	waitDone(t, done)
	assert.Less(t, time.Since(start), 3*time.Second)

	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return false
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)

	assert.NotPanics(t, func() { assert.NoError(t, m.Close()) })

	fresh := newPersistentState(dir)
	fresh.buildState()
	assert.Equal(t, want, viewState(fresh))
}
