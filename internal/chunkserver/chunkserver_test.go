package chunkserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"

	"github.com/slothhcommits/Hive-FS/internal"
	"github.com/slothhcommits/Hive-FS/internal/hive"
	"github.com/slothhcommits/Hive-FS/internal/messages"
)

const waitFor = 5 * time.Second

// ================================================================ helpers

func leakCheck(t *testing.T) {
	t.Helper()
	// registered first, so it runs after every other cleanup of the test
	t.Cleanup(func() { goleak.VerifyNone(t) })
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, waitFor, 10*time.Millisecond)
}

func msgOf(typ uint8, m proto.Message) *hive.Message {
	p, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return &hive.Message{Payload: p, Metadata: hive.Metadata{Type: typ, Size: uint32(len(p))}}
}

func recvMsg(t *testing.T, ch <-chan *hive.Message) *hive.Message {
	t.Helper()
	select {
	case m := <-ch:
		require.NotNil(t, m)
		return m
	case <-time.After(waitFor):
		t.Fatal("timed out waiting for reply")
	}
	return nil
}

func decodeAck(t *testing.T, m *hive.Message) *messages.Ack {
	t.Helper()
	require.NotNil(t, m)
	require.Equal(t, uint8(messages.MessageType_ACK), m.Type)
	ack := &messages.Ack{}
	require.NoError(t, proto.Unmarshal(m.Payload, ack))
	return ack
}

func requireOK(t *testing.T, m *hive.Message) *messages.Ack {
	t.Helper()
	ack := decodeAck(t, m)
	require.True(t, ack.Success, "unexpected error ack: %v", ack.Err)
	return ack
}

func requireErrCode(t *testing.T, m *hive.Message, code messages.Error_Code) *messages.Ack {
	t.Helper()
	ack := decodeAck(t, m)
	require.False(t, ack.Success, "expected an error ack")
	require.NotNil(t, ack.Err)
	require.Equal(t, code, ack.Err.Code, "message: %s", ack.Err.Message)
	return ack
}

func requireBusy(t *testing.T, m *hive.Message) {
	t.Helper()
	ack := requireErrCode(t, m, messages.Error_CODE_CHUNKSERVER_BUSY)
	assert.Contains(t, ack.Err.Message, "busy")
}

func decodeWriteAck(t *testing.T, m *hive.Message) *messages.Chunkserver_WriteAck {
	t.Helper()
	require.NotNil(t, m)
	require.Equal(t, uint8(messages.Chunkserver_MESSAGE_TYPE_WRITE_ACK), m.Type)
	ack := &messages.Chunkserver_WriteAck{}
	require.NoError(t, proto.Unmarshal(m.Payload, ack))
	return ack
}

func writeAckMsg(success bool, errMsg string) *hive.Message {
	ack := &messages.Chunkserver_WriteAck{Success: success}
	if errMsg != "" {
		ack.Err = &messages.Chunkserver_WriteError{Message: errMsg}
	}
	return msgOf(uint8(messages.Chunkserver_MESSAGE_TYPE_WRITE_ACK), ack)
}

// ackOffsetErr unwraps the Client_WriteAck carried in Ack.Data.
func ackOffsetErr(m *hive.Message) (int64, error) {
	ack := &messages.Ack{}
	if err := proto.Unmarshal(m.Payload, ack); err != nil {
		return 0, err
	}
	if !ack.Success {
		return 0, fmt.Errorf("commit failed: %v", ack.Err)
	}
	wa := &messages.Client_WriteAck{}
	if err := proto.Unmarshal(ack.Data, wa); err != nil {
		return 0, err
	}
	return wa.Offset, nil
}

func ackOffset(t *testing.T, m *hive.Message) int64 {
	t.Helper()
	off, err := ackOffsetErr(m)
	require.NoError(t, err)
	return off
}

func commitMsg(bufferID uint32, handle uint64, offset int64, secondaries ...string) *hive.Message {
	return msgOf(uint8(messages.Client_MESSAGE_TYPE_COMMIT_WRITE), &messages.Client_Write{
		BufferId:    bufferID,
		ChunkId:     handle,
		Offset:      offset,
		Secondaries: secondaries,
	})
}

func grantMsg(h uint64) *hive.Message {
	return msgOf(uint8(messages.Master_MESSAGE_TYPE_TAKE_LEASE), &messages.Master_GrantLease{Id: h})
}

func updateMsg(h uint64, v uint32) *hive.Message {
	return msgOf(uint8(messages.Master_MESSAGE_TYPE_UPDATE_VERSION), &messages.Master_UpdateVersion{Id: h, Version: v})
}

func copyMsg(src, dst uint64) *hive.Message {
	return msgOf(uint8(messages.Master_MESSAGE_TYPE_LOCAL_COPY_CHUNK), &messages.Master_LocalCopy{SrcId: src, DstId: dst})
}

func replicateMsg(h uint64, v uint32, src string) *hive.Message {
	return msgOf(uint8(messages.Master_MESSAGE_TYPE_REPLICATE_CHUNK), &messages.Master_Replicate{Handle: h, Version: v, Src: src})
}

func pingMsg() *hive.Message {
	return msgOf(uint8(messages.Chunkserver_MESSAGE_TYPE_PING), &messages.Ack{Success: true})
}

func spliceAt(data []byte, off int, buf []byte) []byte {
	out := slices.Clone(data)
	if need := off + len(buf); need > len(out) {
		out = append(out, make([]byte, need-len(out))...)
	}
	copy(out[off:], buf)
	return out
}

func corruptByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer f.Close()
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
}

func readTail(t *testing.T, path string, n int) []byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	fi, err := f.Stat()
	require.NoError(t, err)
	buf := make([]byte, n)
	_, err = f.ReadAt(buf, fi.Size()-int64(n))
	require.NoError(t, err)
	return buf
}

// ------------------------------------------------------------ server state

func newTestServer(t *testing.T) *chunkserver {
	t.Helper()
	cs := NewChunkServer(&ChunkServerOpts{
		ListenAddr: freeAddr(t),
		MasterAddr: freeAddr(t), // nothing listens here
		StorePath:  t.TempDir(),
	})
	cs.persistentState = cs.store.buildState()
	t.Cleanup(func() { cs.Close() })
	return cs
}

// serve runs the listener plus the request and stream loops, and nothing periodic.
func serve(t *testing.T, cs *chunkserver) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	listenDone := make(chan struct{})
	cs.wg.Add(2)
	go cs.handleRequests(ctx)
	go cs.handleStreams(ctx)
	go func() {
		cs.transport.ListenAndAccept(cs.ListenAddr)
		close(listenDone)
	}()
	t.Cleanup(func() {
		cancel()
		cs.Close()
		cs.wg.Wait()
		<-listenDone
	})
	waitListening(t, cs.ListenAddr)
}

func seedChunk(t *testing.T, cs *chunkserver, handle uint64, version uint32, data []byte) *chunk {
	t.Helper()
	c, err := cs.mintNewChunk(handle)
	require.NoError(t, err)
	if len(data) > 0 {
		require.NoError(t, os.WriteFile(cs.store.makeChunkPath(handle), data, 0o644))
		c.checksums.table = tableFor(data)
		require.NoError(t, os.WriteFile(cs.store.makeChecksumPath(handle), encodeTable(c.checksums.table), 0o644))
	}
	require.NoError(t, cs.updateChunkVersion(handle, version))
	return c
}

func chunkOf(cs *chunkserver, h uint64) (*chunk, bool) {
	cs.chunkSet.Lock()
	defer cs.chunkSet.Unlock()
	c, ok := cs.chunkSet.entries[h]
	return c, ok
}

func versionOf(cs *chunkserver, h uint64) (uint32, bool) {
	cs.versionMap.Lock()
	defer cs.versionMap.Unlock()
	v, ok := cs.versionMap.entries[h]
	return v, ok
}

func leaseOf(cs *chunkserver, h uint64) (time.Time, bool) {
	cs.leaseMap.Lock()
	defer cs.leaseMap.Unlock()
	l, ok := cs.leaseMap.entries[h]
	return l, ok
}

func bufferOf(cs *chunkserver, id uint32) (*bufferEntry, bool) {
	cs.bufferMap.Lock()
	defer cs.bufferMap.Unlock()
	e, ok := cs.bufferMap.entries[id]
	return e, ok
}

func grantLease(cs *chunkserver, h uint64, at time.Time) {
	cs.leaseMap.Lock()
	cs.leaseMap.entries[h] = at
	cs.leaseMap.Unlock()
}

func putBuffer(cs *chunkserver, id uint32, h uint64, data []byte, pushed bool) {
	e := &bufferEntry{chunkHandle: h, buf: data}
	if pushed {
		e.pushedAt = time.Now()
	}
	cs.bufferMap.Lock()
	cs.bufferMap.entries[id] = e
	cs.bufferMap.Unlock()
}

func ctrOf(c *chunk) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ctr
}

func holdChunk(t *testing.T, c *chunk) {
	t.Helper()
	c.mu.Lock()
	var once sync.Once
	t.Cleanup(func() { once.Do(c.mu.Unlock) })
}

// ------------------------------------------------------------- fake peers

type fakePeer struct {
	tr   *hive.TCPTransport
	addr string
	done chan struct{}
	wg   sync.WaitGroup
}

func newFakePeerAt(t *testing.T, addr string, onReq func(hive.Request)) *fakePeer {
	t.Helper()
	p := &fakePeer{tr: hive.NewTCPTransport(), addr: addr, done: make(chan struct{})}
	go func() {
		p.tr.ListenAndAccept(p.addr)
		close(p.done)
	}()
	if onReq != nil {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for r := range p.tr.Consume() {
				onReq(r)
			}
		}()
	}
	t.Cleanup(func() {
		p.tr.Close()
		<-p.done
		p.wg.Wait()
	})
	waitListening(t, p.addr)
	return p
}

func newFakePeer(t *testing.T, onReq func(hive.Request)) *fakePeer {
	t.Helper()
	return newFakePeerAt(t, freeAddr(t), onReq)
}

func ackHandler(r hive.Request) { r.Respond(internal.MakeAckMessage(nil)) }

func (p *fakePeer) nextStream(t *testing.T) *hive.Stream {
	t.Helper()
	select {
	case s, ok := <-p.tr.Streams():
		require.True(t, ok, "streams channel closed")
		return s
	case <-time.After(waitFor):
		t.Fatal("timed out waiting for a stream")
	}
	return nil
}

// streamPair returns the client and the server end of a fresh stream.
func streamPair(t *testing.T, typ uint8, meta proto.Message) (client, server *hive.Stream) {
	t.Helper()
	front := hive.NewTCPTransport()
	addr := freeAddr(t)
	done := make(chan struct{})
	go func() {
		front.ListenAndAccept(addr)
		close(done)
	}()
	waitListening(t, addr)
	ct := hive.NewTCPTransport()
	t.Cleanup(func() {
		ct.Close()
		front.Close()
		<-done
	})

	client, err := ct.OpenStream(context.Background(), addr, msgOf(typ, meta))
	require.NoError(t, err)
	select {
	case server = <-front.Streams():
	case <-time.After(waitFor):
		t.Fatal("stream was not delivered")
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func readAck(t *testing.T, s *hive.Stream) *messages.Ack {
	t.Helper()
	m, err := s.ReadStreamMessage()
	require.NoError(t, err)
	return decodeAck(t, m)
}

func readErrAck(t *testing.T, s *hive.Stream, code messages.Error_Code) *messages.Ack {
	t.Helper()
	ack := readAck(t, s)
	require.False(t, ack.Success)
	require.NotNil(t, ack.Err)
	require.Equal(t, code, ack.Err.Code, "message: %s", ack.Err.Message)
	return ack
}

func rpcErr(tr *hive.TCPTransport, addr string, msg *hive.Message) (*hive.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, err := tr.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	ch, err := p.Request(ctx, msg)
	if err != nil {
		return nil, err
	}
	select {
	case m := <-ch:
		return m, nil
	case <-ctx.Done():
		p.Unregister(msg)
		return nil, ctx.Err()
	}
}

func rpc(t *testing.T, tr *hive.TCPTransport, addr string, msg *hive.Message) *hive.Message {
	t.Helper()
	m, err := rpcErr(tr, addr, msg)
	require.NoError(t, err)
	return m
}

func readChunkStream(tr *hive.TCPTransport, addr string, h uint64, length uint32) ([]byte, error) {
	s, err := tr.OpenStream(context.Background(), addr, msgOf(
		uint8(messages.Client_MESSAGE_TYPE_READ_STREAM),
		&messages.Client_ReadStreamMetadata{ChunkId: h, Version: 1, Offset: 0, Length: length},
	))
	if err != nil {
		return nil, err
	}
	defer s.Close()

	hdr, err := s.ReadStreamMessage()
	if err != nil {
		return nil, err
	}
	if hdr.Type != uint8(messages.Chunkserver_MESSAGE_TYPE_READ_STREAM) {
		ack := &messages.Ack{}
		if err := proto.Unmarshal(hdr.Payload, ack); err == nil && ack.Err != nil {
			return nil, fmt.Errorf("code %v: %s", ack.Err.Code, ack.Err.Message)
		}
		return nil, fmt.Errorf("unexpected message type %d", hdr.Type)
	}
	rs := &messages.Chunkserver_ReadStream{}
	if err := proto.Unmarshal(hdr.Payload, rs); err != nil {
		return nil, err
	}
	buf := make([]byte, rs.Length)
	if _, err := io.ReadFull(s, buf); err != nil {
		return nil, err
	}
	if err := s.ValidateRead(); err != nil {
		return nil, err
	}
	return buf, nil
}

func pushWrite(tr *hive.TCPTransport, addr string, meta *messages.Client_WriteStreamMetadata, data []byte) error {
	s, err := tr.OpenStream(context.Background(), addr, msgOf(uint8(messages.Client_MESSAGE_TYPE_WRITE_DATA_STREAM), meta))
	if err != nil {
		return err
	}
	defer s.Close()
	if _, err := s.Write(data); err != nil {
		return err
	}
	if err := s.WriteChecksum(); err != nil {
		return err
	}
	m, err := s.ReadStreamMessage()
	if err != nil {
		return err
	}
	ack := &messages.Ack{}
	if err := proto.Unmarshal(m.Payload, ack); err != nil {
		return err
	}
	if !ack.Success {
		return fmt.Errorf("push rejected: %v", ack.Err)
	}
	return nil
}

func runConcurrent(n int, fn func(i int) error) []error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(i)
		}()
	}
	wg.Wait()
	return errs
}

// stallReplication starts a re-replication of handle against a source that accepts
// the stream and then goes quiet, so the destination chunk stays locked.
func stallReplication(t *testing.T, cs *chunkserver, handle uint64) (release func()) {
	t.Helper()
	src := newFakePeer(t, nil)
	res := make(chan *hive.Message, 1)
	go func() { res <- cs.replicationHandler(replicateMsg(handle, 1, src.addr)) }()
	held := src.nextStream(t)

	var once sync.Once
	release = func() {
		once.Do(func() {
			held.Close()
			select {
			case <-res:
			case <-time.After(waitFor):
				t.Error("replication handler did not return")
			}
		})
	}
	t.Cleanup(release)
	return release
}

// stallPrimary starts a primary write whose only secondary never answers until released.
func stallPrimary(t *testing.T, cs *chunkserver, handle uint64) (release func()) {
	t.Helper()
	seedChunk(t, cs, handle, 1, pseudoData(7, 1000))
	grantLease(cs, handle, time.Now())
	putBuffer(cs, 900, handle, []byte("held"), true)

	gate := make(chan struct{})
	reached := make(chan struct{}, 1)
	sec := newFakePeer(t, func(r hive.Request) {
		select {
		case reached <- struct{}{}:
		default:
		}
		<-gate
		r.Respond(writeAckMsg(true, ""))
	})
	res := make(chan *hive.Message, 1)
	go func() { res <- cs.primaryWriteHandler(commitMsg(900, handle, 0, sec.addr)) }()
	select {
	case <-reached:
	case <-time.After(waitFor):
		t.Fatal("primary write never reached the secondary")
	}

	var once sync.Once
	release = func() {
		once.Do(func() {
			close(gate)
			select {
			case <-res:
			case <-time.After(waitFor):
				t.Error("primary write handler did not return")
			}
		})
	}
	t.Cleanup(release)
	return release
}

// ======================================================================= Boot

func TestBoot(t *testing.T) {
	leakCheck(t)
	dir := t.TempDir()
	data1 := pseudoData(1, 100)
	triplet := func(h uint64, ver uint32, data []byte) {
		mustWrite(t, filepath.Join(dir, fmt.Sprintf("%d.chunk", h)), data)
		mustWrite(t, filepath.Join(dir, fmt.Sprintf("%d.sum", h)), encodeTable(tableFor(data)))
		mustWrite(t, filepath.Join(dir, fmt.Sprintf("%d.ver", h)), verBytes(ver))
	}
	triplet(1, 3, data1)
	triplet(2, 5, nil)
	mustWrite(t, filepath.Join(dir, "9.chunk"), []byte("orphan"))
	mustWrite(t, filepath.Join(dir, "junk.txt"), []byte("x"))

	listen := freeAddr(t)
	cs := NewChunkServer(&ChunkServerOpts{ListenAddr: listen, MasterAddr: freeAddr(t), StorePath: dir})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		cs.Boot(ctx)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	waitListening(t, listen)

	ct := hive.NewTCPTransport()
	t.Cleanup(func() { ct.Close() })

	// accepts requests
	requireOK(t, rpc(t, ct, listen, pingMsg()))

	// accepts streams, and serves the persisted chunk at its persisted version
	got, err := readChunkStream(ct, listen, 1, 100)
	require.NoError(t, err)
	assert.Equal(t, data1, got)

	// shuts down promptly
	cancel()
	select {
	case <-stopped:
	case <-time.After(waitFor):
		t.Fatal("Boot did not return after cancel")
	}

	// the stopped channel orders these reads after Boot's writes
	require.NotNil(t, cs.persistentState)
	cs.versionMap.Lock()
	assert.Equal(t, map[uint64]uint32{1: 3, 2: 5}, cs.versionMap.entries)
	cs.versionMap.Unlock()
	cs.chunkSet.Lock()
	assert.Len(t, cs.chunkSet.entries, 2)
	for _, h := range []uint64{1, 2} {
		c, ok := cs.chunkSet.entries[h]
		require.True(t, ok)
		assert.Equal(t, h, c.handle)
	}
	cs.chunkSet.Unlock()
	assert.False(t, exists(filepath.Join(dir, "9.chunk")), "orphan must be cleaned")
	assert.False(t, exists(filepath.Join(dir, "junk.txt")), "garbage must be cleaned")
}

// =============================================================== sendRequest

func TestSendRequest(t *testing.T) {
	leakCheck(t)

	t.Run("success", func(t *testing.T) {
		cs := newTestServer(t)
		p := newFakePeer(t, ackHandler)
		res, err := cs.sendRequest(context.Background(), p.addr, pingMsg())
		require.NoError(t, err)
		requireOK(t, res)
	})

	t.Run("nil ctx is fire and forget", func(t *testing.T) {
		cs := newTestServer(t)
		var got atomic.Int64
		p := newFakePeer(t, func(r hive.Request) { got.Add(1) })
		res, err := cs.sendRequest(nil, p.addr, pingMsg()) //nolint:staticcheck
		require.NoError(t, err)
		assert.Nil(t, res)
		require.Eventually(t, func() bool { return got.Load() == 1 }, waitFor, 10*time.Millisecond)
	})

	t.Run("cancelled ctx never goes out", func(t *testing.T) {
		cs := newTestServer(t)
		var got atomic.Int64
		p := newFakePeer(t, func(r hive.Request) {
			got.Add(1)
			ackHandler(r)
		})
		_, err := cs.transport.Dial(context.Background(), p.addr)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := cs.sendRequest(ctx, p.addr, pingMsg())
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, res)
		time.Sleep(100 * time.Millisecond)
		assert.Zero(t, got.Load())
	})

	t.Run("dial failure", func(t *testing.T) {
		cs := newTestServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		res, err := cs.sendRequest(ctx, freeAddr(t), pingMsg())
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("ctx expires before the reply", func(t *testing.T) {
		cs := newTestServer(t)
		p := newFakePeer(t, func(r hive.Request) {})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		res, err := cs.sendRequest(ctx, p.addr, pingMsg())
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, res)
	})

	t.Run("reconnects after the peer restarts", func(t *testing.T) {
		cs := newTestServer(t)
		addr := freeAddr(t)
		p1 := newFakePeerAt(t, addr, ackHandler)
		res, err := cs.sendRequest(context.Background(), addr, pingMsg())
		require.NoError(t, err)
		requireOK(t, res)

		p1.tr.Close()
		<-p1.done
		time.Sleep(200 * time.Millisecond)
		newFakePeerAt(t, addr, ackHandler)

		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			res, err := cs.sendRequest(ctx, addr, pingMsg())
			return err == nil && res != nil && res.Type == uint8(messages.MessageType_ACK)
		}, 2*waitFor, 50*time.Millisecond)
	})
}

// ============================================================== mintNewChunk

func TestMintNewChunk(t *testing.T) {
	leakCheck(t)

	t.Run("existing handle", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 0, nil)
		c, err := cs.mintNewChunk(1)
		assert.ErrorIs(t, err, internal.ErrExists)
		assert.Nil(t, c)
	})

	t.Run("new handle", func(t *testing.T) {
		cs := newTestServer(t)
		c, err := cs.mintNewChunk(2)
		require.NoError(t, err)

		got, ok := chunkOf(cs, 2)
		require.True(t, ok)
		assert.Same(t, c, got)
		assert.Equal(t, uint64(2), c.handle)

		v, ok := versionOf(cs, 2)
		require.True(t, ok)
		assert.Zero(t, v)

		for _, p := range []string{cs.store.makeChunkPath(2), cs.store.makeChecksumPath(2), cs.store.makeVersionPath(2)} {
			assert.FileExists(t, p)
		}
	})
}

// ============================================================== deletechunk

func TestDeleteChunk(t *testing.T) {
	leakCheck(t)

	allGone := func(t *testing.T, cs *chunkserver, h uint64) {
		t.Helper()
		_, ok := chunkOf(cs, h)
		assert.False(t, ok, "chunk set entry")
		_, ok = versionOf(cs, h)
		assert.False(t, ok, "version map entry")
		_, ok = leaseOf(cs, h)
		assert.False(t, ok, "lease map entry")
		for _, p := range []string{cs.store.makeChunkPath(h), cs.store.makeChecksumPath(h), cs.store.makeVersionPath(h)} {
			assert.NoFileExists(t, p)
		}
	}

	t.Run("non existing chunk", func(t *testing.T) {
		cs := newTestServer(t)
		assert.ErrorIs(t, cs.deletechunk(404), fs.ErrNotExist)
	})

	t.Run("partially existing in memory", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 2, pseudoData(1, 100))
		cs.versionMap.Lock()
		delete(cs.versionMap.entries, 1)
		cs.versionMap.Unlock()

		assert.NoError(t, cs.deletechunk(1))
		allGone(t, cs, 1)
	})

	t.Run("partially existing on disk", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 2, pseudoData(1, 100))
		require.NoError(t, os.Remove(cs.store.makeChecksumPath(1)))

		assert.ErrorIs(t, cs.deletechunk(1), fs.ErrNotExist)
		allGone(t, cs, 1)
	})

	t.Run("fully existing chunk", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 2, pseudoData(1, 100))
		grantLease(cs, 1, time.Now())

		assert.NoError(t, cs.deletechunk(1))
		allGone(t, cs, 1)
	})
}

// ============================================================== verifyChunk

func TestVerifyChunk(t *testing.T) {
	leakCheck(t)
	data := pseudoData(1, 2*blockSize+5000)

	t.Run("non existing chunk", func(t *testing.T) {
		cs := newTestServer(t)
		err := cs.verifyChunk(newChunk(99, newChecksums(99)))
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("bad chunk", func(t *testing.T) {
		cs := newTestServer(t)
		c := seedChunk(t, cs, 1, 1, data)
		corruptByte(t, cs.store.makeChunkPath(1), blockSize+7)
		assert.ErrorIs(t, cs.verifyChunk(c), internal.ErrFileCorrupt)
	})

	t.Run("good chunk", func(t *testing.T) {
		cs := newTestServer(t)
		c := seedChunk(t, cs, 1, 1, data)
		assert.NoError(t, cs.verifyChunk(c))
	})
}

// ======================================================= version bookkeeping

func TestUpdateChunkVersion(t *testing.T) {
	leakCheck(t)

	t.Run("non existing chunk", func(t *testing.T) {
		cs := newTestServer(t)
		assert.ErrorIs(t, cs.updateChunkVersion(1, 2), internal.ErrUnavailable)
	})

	t.Run("chunk version above the one passed", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 5, nil)
		assert.ErrorIs(t, cs.updateChunkVersion(1, 3), internal.ErrBadRequest)
		v, _ := versionOf(cs, 1)
		assert.Equal(t, uint32(5), v)
		assert.Equal(t, uint32(5), rawVersion(t, cs.store, 1))
	})

	t.Run("valid request", func(t *testing.T) {
		cs := newTestServer(t)
		c := seedChunk(t, cs, 1, 1, nil)
		c.mu.Lock()
		c.ctr = 7
		c.mu.Unlock()

		require.NoError(t, cs.updateChunkVersion(1, 3))
		v, _ := versionOf(cs, 1)
		assert.Equal(t, uint32(3), v)
		assert.Zero(t, ctrOf(c))
		assert.Equal(t, uint32(3), rawVersion(t, cs.store, 1))
	})
}

func TestDowndateChunkVersion(t *testing.T) {
	leakCheck(t)

	t.Run("non existing chunk", func(t *testing.T) {
		cs := newTestServer(t)
		assert.ErrorIs(t, cs.downdateChunkVersion(1, 2), internal.ErrUnavailable)
	})

	t.Run("version above the chunk's", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 2, nil)
		require.NoError(t, cs.downdateChunkVersion(1, 5))
		v, _ := versionOf(cs, 1)
		assert.Equal(t, uint32(2), v)
		assert.Equal(t, uint32(2), rawVersion(t, cs.store, 1))
	})

	t.Run("valid request", func(t *testing.T) {
		cs := newTestServer(t)
		seedChunk(t, cs, 1, 5, nil)
		require.NoError(t, cs.downdateChunkVersion(1, 3))
		v, _ := versionOf(cs, 1)
		assert.Equal(t, uint32(3), v)
		assert.Equal(t, uint32(3), rawVersion(t, cs.store, 1))
	})
}

