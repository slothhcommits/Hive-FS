package hive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

const (
	tReq    uint8 = 1
	tAck    uint8 = 200
	tHold   uint8 = 99 // echo handler never answers this type
	waitFor       = 5 * time.Second
)

// ---------------------------------------------------------------- helpers

func leakCheck(t *testing.T) {
	t.Helper()
	// registered first, so it runs after every other cleanup of the test
	t.Cleanup(func() { goleak.VerifyNone(t) })
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func mkMsg(typ uint8, p []byte) *Message {
	return &Message{Metadata: Metadata{Type: typ, Size: uint32(len(p))}, Payload: p}
}

type handler func(Request)

func echo(r Request) {
	m := r.Message()
	if m.Type == tHold {
		return
	}
	r.Respond(mkMsg(tAck, m.Payload))
}

func swallow(Request) {}

type node struct {
	tr   *TCPTransport
	addr string
	done chan struct{} // closed when ListenAndAccept returns
}

// startNode starts a listening transport. h == nil means nobody consumes Consume().
func startNode(t *testing.T, h handler) *node {
	t.Helper()
	n := &node{tr: NewTCPTransport(), addr: freeAddr(t), done: make(chan struct{})}
	go func() {
		n.tr.ListenAndAccept(n.addr)
		close(n.done)
	}()
	if h != nil {
		go func() {
			for r := range n.tr.Consume() {
				h(r)
			}
		}()
	}
	t.Cleanup(func() { n.tr.Close() })

	waitUntil(t, waitFor, "listener", func() bool {
		c, err := net.DialTimeout("tcp", n.addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
	return n
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func recvOr(ch <-chan *Message, d time.Duration) *Message {
	select {
	case m := <-ch:
		return m
	case <-time.After(d):
		return nil
	}
}

func recvStream(t *testing.T, n *node) *Stream {
	t.Helper()
	select {
	case s, ok := <-n.tr.Streams():
		if !ok {
			t.Fatal("streams channel closed")
		}
		return s
	case <-time.After(waitFor):
		t.Fatal("timed out waiting for stream")
	}
	return nil
}

func cachedPeers(tr *TCPTransport) int {
	tr.outboundPeersMu.Lock()
	defer tr.outboundPeersMu.Unlock()
	return len(tr.outboundPeers)
}

func pendingLen(p Peer) int {
	op := p.(*OutboundPeer)
	op.pendingMu.Lock()
	defer op.pendingMu.Unlock()
	return len(op.pending)
}

func isCanceled(err error) bool {
	return err != nil && (errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "canceled"))
}

func isDeadline(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func doRequest(ctx context.Context, from *node, to string, payload []byte) error {
	p, err := from.tr.Dial(ctx, to)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	ch, err := p.Request(ctx, mkMsg(tReq, payload))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	select {
	case m := <-ch:
		if m.Type != tAck {
			return fmt.Errorf("unexpected reply type %d", m.Type)
		}
		if !bytes.Equal(m.Payload, payload) {
			return errors.New("reply payload mismatch")
		}
		return nil
	case <-time.After(waitFor):
		return errors.New("timed out waiting for ack")
	case <-ctx.Done():
		return ctx.Err()
	}
}

type ctxMaker func() (context.Context, context.CancelFunc)

func noTimer() (context.Context, context.CancelFunc) {
	return context.Background(), func() {}
}

func withTimer() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func hammer(t *testing.T, froms []*node, to *node, goroutines, iters int, mk ctxMaker) {
	t.Helper()
	var wg sync.WaitGroup
	for fi, f := range froms {
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iters; j++ {
					ctx, cancel := mk()
					err := doRequest(ctx, f, to.addr, []byte(fmt.Sprintf("p-%d-%d-%d", fi, g, j)))
					cancel()
					if err != nil {
						t.Errorf("peer %d routine %d iter %d: %v", fi, g, j, err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
}

type rawRemote struct {
	ln    net.Listener
	conns chan net.Conn
	wg    sync.WaitGroup
}

func startRaw(t *testing.T) *rawRemote {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &rawRemote{ln: ln, conns: make(chan net.Conn, 64)}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			r.conns <- c
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		r.wg.Wait()
		close(r.conns)
		for c := range r.conns {
			c.Close()
		}
	})
	return r
}

func (r *rawRemote) addr() string { return r.ln.Addr().String() }

func (r *rawRemote) accept(t *testing.T) net.Conn {
	t.Helper()
	select {
	case c := <-r.conns:
		return c
	case <-time.After(waitFor):
		t.Fatal("raw remote: nothing accepted")
	}
	return nil
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// expectClosedSilently asserts the remote closes the conn without sending any frame.
func expectClosedSilently(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(waitFor))
	var buf [16]byte
	n, err := c.Read(buf[:])
	if n != 0 {
		t.Fatalf("expected no data, got %d bytes", n)
	}
	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("expected connection to be closed by server, err=%v", err)
	}
}

func closeAll(t *testing.T, nodes ...*node) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, n := range nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n.tr.Close()
			}()
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * waitFor):
		t.Fatal("Close did not return")
	}
}

func drainUntilClosed[T any](t *testing.T, name string, ch <-chan T, each func(T)) {
	t.Helper()
	timeout := time.After(waitFor)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return
			}
			if each != nil {
				each(v)
			}
		case <-timeout:
			t.Fatalf("%s channel was not closed", name)
		}
	}
}

func assertConnClosedErr(t *testing.T, what string, err error) {
	t.Helper()
	var ecc *ErrConnClosed
	if !errors.As(err, &ecc) {
		t.Fatalf("%s: expected ErrConnClosed, got %v", what, err)
	}
}

func assertConnClosedMsg(t *testing.T, m *Message) {
	t.Helper()
	if m == nil {
		t.Fatal("pending request got no message on close")
	}
	if m.Type != uint8(ConnClosed) || string(m.Payload) != "connection closed" {
		t.Fatalf("expected ErrConnClosed message, got type=%d payload=%q", m.Type, m.Payload)
	}
}

// validateClosed checks everything that must hold for a closed transport.
func validateClosed(t *testing.T, n *node) {
	t.Helper()
	drainUntilClosed(t, "Consume", n.tr.Consume(), nil)
	drainUntilClosed(t, "Streams", n.tr.Streams(), func(s *Stream) { s.Close() })

	_, err := n.tr.Dial(context.Background(), n.addr)
	assertConnClosedErr(t, "Dial after Close", err)
	assertConnClosedErr(t, "ListenAndAccept after Close", n.tr.ListenAndAccept(freeAddr(t)))

	// double close must not panic
	n.tr.Close()

	select {
	case <-n.done:
	case <-time.After(waitFor):
		t.Fatal("ListenAndAccept did not return after Close")
	}
	if c := cachedPeers(n.tr); c != 0 {
		t.Fatalf("outbound cache not empty after Close: %d", c)
	}
}

func segData(seed, idx, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(seed*131 + idx*17 + i*7)
	}
	return b
}

var segSizes = []int{1, 100, 4096, 70000}

func sendSegments(s *Stream, seed int) error {
	for i, n := range segSizes {
		if _, err := s.Write(segData(seed, i, n)); err != nil {
			return err
		}
		if err := s.WriteChecksum(); err != nil {
			return err
		}
	}
	return nil
}

func readSegments(s *Stream, seed int) error {
	for i, n := range segSizes {
		buf := make([]byte, n)
		if _, err := io.ReadFull(s, buf); err != nil {
			return fmt.Errorf("segment %d read: %w", i, err)
		}
		if err := s.ValidateRead(); err != nil {
			return fmt.Errorf("segment %d checksum: %w", i, err)
		}
		if !bytes.Equal(buf, segData(seed, i, n)) {
			return fmt.Errorf("segment %d data mismatch", i)
		}
	}
	return nil
}

// ------------------------------------------------------ ordinary requests

func TestRequest_SingleAck(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	p, err := a.tr.Dial(context.Background(), b.addr)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := p.Request(context.Background(), mkMsg(tReq, []byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	m := recvOr(ch, waitFor)
	if m == nil {
		t.Fatal("no ack")
	}
	if m.Type != tAck || string(m.Payload) != "hello" || m.Size != 5 {
		t.Fatalf("bad ack: type=%d payload=%q size=%d", m.Type, m.Payload, m.Size)
	}
	if pendingLen(p) != 0 {
		t.Fatal("pending map not cleaned after reply")
	}
}

func TestRequest_ManyRoutinesOnePeer_DialCache(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	// concurrent first dials must all resolve to the same cached peer
	peers := make([]Peer, 20)
	var wg sync.WaitGroup
	for i := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := a.tr.Dial(context.Background(), b.addr)
			if err != nil {
				t.Errorf("dial %d: %v", i, err)
				return
			}
			peers[i] = p
		}()
	}
	wg.Wait()
	for i, p := range peers {
		if p == nil || p != peers[0] {
			t.Fatalf("dial %d returned a different peer instance", i)
		}
	}
	if c := cachedPeers(a.tr); c != 1 {
		t.Fatalf("expected 1 cached peer, got %d", c)
	}

	hammer(t, []*node{a}, b, 20, 10, noTimer)
	if c := cachedPeers(a.tr); c != 1 {
		t.Fatalf("expected 1 cached peer after traffic, got %d", c)
	}
}

func TestRequest_ManyPeersToOne(t *testing.T) {
	leakCheck(t)
	target := startNode(t, echo)
	froms := make([]*node, 5)
	for i := range froms {
		froms[i] = startNode(t, echo)
	}
	hammer(t, froms, target, 8, 5, noTimer)
	for i, f := range froms {
		if c := cachedPeers(f.tr); c != 1 {
			t.Fatalf("peer %d: expected 1 cached peer, got %d", i, c)
		}
	}
}

func TestRequest_ManyPeersToOne_TimedCtx(t *testing.T) {
	leakCheck(t)
	target := startNode(t, echo)
	froms := make([]*node, 5)
	for i := range froms {
		froms[i] = startNode(t, echo)
	}
	hammer(t, froms, target, 8, 5, withTimer)
}

func TestDial_CtxExpiresBeforeDialSucceeds(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	_, err := a.tr.Dial(ctx, b.addr)
	if !isDeadline(err) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if c := cachedPeers(a.tr); c != 0 {
		t.Fatalf("failed dial must not be cached, got %d", c)
	}
}

func TestRequest_CtxExpiresBeforeAck(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, swallow), startNode(t, swallow)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	p, err := a.tr.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	msg := mkMsg(tReq, []byte("never answered"))
	ch, err := p.Request(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-ch:
		t.Fatalf("unexpected reply: %+v", m)
	case <-ctx.Done():
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", ctx.Err())
	}
	p.Unregister(msg)
	if pendingLen(p) != 0 {
		t.Fatal("pending map not cleaned after Unregister")
	}
}

func TestDialAndRequest_CancelledCtx(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := a.tr.Dial(cctx, b.addr); !isCanceled(err) {
		t.Fatalf("Dial with cancelled ctx: expected cancel error, got %v", err)
	}
	if c := cachedPeers(a.tr); c != 0 {
		t.Fatalf("cancelled dial must not be cached, got %d", c)
	}

	p, err := a.tr.Dial(context.Background(), b.addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Request(cctx, mkMsg(tReq, []byte("x"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("Request with cancelled ctx: expected context.Canceled, got %v", err)
	}
	if pendingLen(p) != 0 {
		t.Fatal("failed Request left a pending entry")
	}
}

func TestSend_ManyPeersToOne(t *testing.T) {
	leakCheck(t)
	var got atomic.Int64
	target := startNode(t, func(r Request) { got.Add(1) })
	froms := make([]*node, 4)
	for i := range froms {
		froms[i] = startNode(t, echo)
	}

	const goroutines, iters = 8, 10
	var wg sync.WaitGroup
	for fi, f := range froms {
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < iters; j++ {
					ctx, cancel := withTimer()
					p, err := f.tr.Dial(ctx, target.addr)
					if err == nil {
						err = p.Send(ctx, mkMsg(tReq, []byte(fmt.Sprintf("s-%d-%d-%d", fi, g, j))))
					}
					cancel()
					if err != nil {
						t.Errorf("peer %d routine %d: %v", fi, g, err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()

	want := int64(len(froms) * goroutines * iters)
	waitUntil(t, waitFor, "all sent messages to arrive", func() bool { return got.Load() == want })
}

func TestPeerEvictionOnWriteFailure(t *testing.T) {
	leakCheck(t)
	a := startNode(t, echo)
	r := startRaw(t)
	ctx := context.Background()

	p1, err := a.tr.Dial(ctx, r.addr())
	if err != nil {
		t.Fatal(err)
	}
	r.accept(t).Close()

	var sendErr error
	end := time.Now().Add(waitFor)
	for time.Now().Before(end) {
		if sendErr = p1.Send(ctx, mkMsg(tReq, []byte("x"))); sendErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sendErr == nil {
		t.Fatal("Send never failed after remote close")
	}
	if errors.Is(sendErr, context.Canceled) || errors.Is(sendErr, context.DeadlineExceeded) {
		t.Fatalf("expected a non-ctx error, got %v", sendErr)
	}
	p2, err := a.tr.Dial(ctx, r.addr())
	if err != nil {
		t.Fatal(err)
	}
	if p2 == p1 {
		t.Fatal("Dial returned the stale peer, cache entry was not evicted")
	}

	// ctx side: a cancelled ctx must not evict
	b := startNode(t, echo)
	q1, err := a.tr.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := q1.Send(cctx, mkMsg(tReq, []byte("x"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	q2, err := a.tr.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	if q2 != q1 {
		t.Fatal("cancelled-ctx Send evicted the peer")
	}
}

func TestRequest_OnClosedConnection(t *testing.T) {
	leakCheck(t)
	a := startNode(t, echo)
	r := startRaw(t)
	ctx := context.Background()

	p, err := a.tr.Dial(ctx, r.addr())
	if err != nil {
		t.Fatal(err)
	}
	rc := r.accept(t)

	// a request already waiting when the remote goes away gets ErrConnClosed
	pending, err := p.Request(ctx, mkMsg(tReq, []byte("pending")))
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	assertConnClosedMsg(t, recvOr(pending, waitFor))

	waitUntil(t, waitFor, "peer eviction", func() bool { return cachedPeers(a.tr) == 0 })

	// a request on the dead connection fails with the net closed error
	_, err = p.Request(ctx, mkMsg(tReq, []byte("late")))
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed, got %v", err)
	}
	if pendingLen(p) != 0 {
		t.Fatal("failed Request left a pending entry")
	}
}

// --------------------------------------------------------- ordinary streams

func TestStream_SequentialChecksummedSegments(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	meta := mkMsg(7, []byte("stream-meta"))
	out, err := a.tr.OpenStream(context.Background(), b.addr, meta)
	if err != nil {
		t.Fatal(err)
	}
	in := recvStream(t, b)

	if in.Metadata.Type != 7 || string(in.Metadata.Payload) != "stream-meta" ||
		in.Metadata.Size != uint32(len("stream-meta")) || in.Metadata.frameFlags != flagStream {
		t.Fatalf("bad stream metadata: %+v", in.Metadata)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- sendSegments(out, 5) }()
	if err := readSegments(in, 5); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	out.Close()
	in.Close()
}

func TestStream_Web(t *testing.T) {
	leakCheck(t)
	const n, per = 4, 10
	nodes := make([]*node, n)
	for i := range nodes {
		nodes[i] = startNode(t, echo)
	}

	var mu sync.Mutex
	seen := map[skey]bool{}
	var recvWG sync.WaitGroup

	for di, d := range nodes {
		recvWG.Add(1)
		go func() {
			defer recvWG.Done()
			var hw sync.WaitGroup
			defer hw.Wait()
			for k := 0; k < (n-1)*per; k++ {
				select {
				case s := <-d.tr.Streams():
					hw.Add(1)
					go func() {
						defer hw.Done()
						defer s.Close()
						p := s.Metadata.Payload
						if s.Metadata.Type != 9 || len(p) != 3 {
							t.Errorf("node %d: bad metadata %+v", di, s.Metadata)
							return
						}
						key := skey{int(p[0]), int(p[1]), int(p[2])}
						if key.to != di || key.from == di {
							t.Errorf("node %d: stream routed wrongly: %+v", di, key)
							return
						}
						mu.Lock()
						dup := seen[key]
						seen[key] = true
						mu.Unlock()
						if dup {
							t.Errorf("duplicate stream %+v", key)
							return
						}
						if err := readSegments(s, key.seed()); err != nil {
							t.Errorf("stream %+v: %v", key, err)
						}
					}()
				case <-time.After(2 * waitFor):
					t.Errorf("node %d: timed out waiting for stream %d", di, k)
					return
				}
			}
		}()
	}

	var sendWG sync.WaitGroup
	for fi, f := range nodes {
		for ti, to := range nodes {
			if fi == ti {
				continue
			}
			for i := 0; i < per; i++ {
				sendWG.Add(1)
				go func() {
					defer sendWG.Done()
					s, err := f.tr.OpenStream(context.Background(), to.addr, mkMsg(9, []byte{byte(fi), byte(ti), byte(i)}))
					if err != nil {
						t.Errorf("open %d->%d: %v", fi, ti, err)
						return
					}
					defer s.Close()
					if err := sendSegments(s, skey{fi, ti, i}.seed()); err != nil {
						t.Errorf("send %d->%d: %v", fi, ti, err)
					}
				}()
			}
		}
	}
	sendWG.Wait()
	recvWG.Wait()

	mu.Lock()
	defer mu.Unlock()
	if want := n * (n - 1) * per; len(seen) != want {
		t.Fatalf("expected %d streams received, got %d", want, len(seen))
	}
}

type skey struct{ from, to, idx int }

func (k skey) seed() int { return k.from*10000 + k.to*100 + k.idx }

func TestOpenStream_CtxCancelledBeforeDial(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, err := a.tr.OpenStream(ctx, b.addr, mkMsg(3, []byte("m")))
	if !isCanceled(err) || s != nil {
		t.Fatalf("expected cancel error and nil stream, got %v, %v", s, err)
	}
	select {
	case <-b.tr.Streams():
		t.Fatal("no stream should have reached the listener")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestOpenStream_CtxCancelledMidStream(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, err := a.tr.OpenStream(ctx, b.addr, mkMsg(3, []byte("m")))
	if err != nil {
		t.Fatal(err)
	}
	in := recvStream(t, b)

	readErr := make(chan error, 1)
	go func() {
		var p [1]byte
		_, err := in.Read(p[:])
		readErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("blocked read returned without error after cancel")
		}
	case <-time.After(waitFor):
		t.Fatal("blocked read was not released by ctx cancel")
	}

	var werr error
	end := time.Now().Add(waitFor)
	for time.Now().Before(end) {
		if _, werr = out.Write([]byte("data")); werr != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if werr == nil {
		t.Fatal("write on cancelled stream never failed")
	}
	in.Close()
	out.Close()
}

func TestOpenStream_CloseThenCancel(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)

	ctx, cancel := context.WithCancel(context.Background())
	out, err := a.tr.OpenStream(ctx, b.addr, mkMsg(3, []byte("m")))
	if err != nil {
		t.Fatal(err)
	}
	in := recvStream(t, b)

	out.Close()
	cancel()  // AfterFunc was stopped by Close, must be harmless
	out.Close() // double close must not panic

	var p [1]byte
	in.conn.SetReadDeadline(time.Now().Add(waitFor))
	if _, err := in.Read(p[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF on remote after stream close, got %v", err)
	}
	in.Close()
}

// ------------------------------------------------------ transport closure

func closureScenario(t *testing.T, n, perPair int) {
	t.Helper()
	nodes := make([]*node, n)
	for i := range nodes {
		nodes[i] = startNode(t, echo)
	}

	var mu sync.Mutex
	outs := map[skey]*Stream{}
	ins := map[skey]*Stream{}
	var gotIn atomic.Int64
	var consWG sync.WaitGroup
	for _, d := range nodes {
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			for s := range d.tr.Streams() {
				p := s.Metadata.Payload
				mu.Lock()
				ins[skey{int(p[0]), int(p[1]), int(p[2])}] = s
				mu.Unlock()
				gotIn.Add(1)
			}
		}()
	}

	// concurrent Dial+Request and OpenStream between every pair
	var wg sync.WaitGroup
	for fi, f := range nodes {
		for ti, to := range nodes {
			if fi == ti {
				continue
			}
			for i := 0; i < perPair; i++ {
				wg.Add(2)
				go func() {
					defer wg.Done()
					ctx, cancel := withTimer()
					defer cancel()
					if err := doRequest(ctx, f, to.addr, []byte(fmt.Sprintf("r-%d-%d-%d", fi, ti, i))); err != nil {
						t.Errorf("request %d->%d: %v", fi, ti, err)
					}
				}()
				go func() {
					defer wg.Done()
					// background ctx: cancelling it would close the stream
					s, err := f.tr.OpenStream(context.Background(), to.addr, mkMsg(9, []byte{byte(fi), byte(ti), byte(i)}))
					if err != nil {
						t.Errorf("open %d->%d: %v", fi, ti, err)
						return
					}
					mu.Lock()
					outs[skey{fi, ti, i}] = s
					mu.Unlock()
				}()
			}
		}
	}
	wg.Wait()

	// one unanswered request per pair
	var pending []<-chan *Message
	for fi, f := range nodes {
		for ti, to := range nodes {
			if fi == ti {
				continue
			}
			p, err := f.tr.Dial(context.Background(), to.addr)
			if err != nil {
				t.Fatal(err)
			}
			ch, err := p.Request(context.Background(), mkMsg(tHold, nil))
			if err != nil {
				t.Fatal(err)
			}
			pending = append(pending, ch)
		}
	}

	want := int64(n * (n - 1) * perPair)
	waitUntil(t, 2*waitFor, "all streams to be accepted", func() bool { return gotIn.Load() == want })

	closeAll(t, nodes...)

	for _, nd := range nodes {
		validateClosed(t, nd)
	}
	for _, ch := range pending {
		assertConnClosedMsg(t, recvOr(ch, waitFor))
	}

	// established streams survive transport closure
	consWG.Wait()
	mu.Lock()
	defer mu.Unlock()
	if int64(len(ins)) != want || int64(len(outs)) != want {
		t.Fatalf("expected %d streams on each side, got in=%d out=%d", want, len(ins), len(outs))
	}
	for k, out := range outs {
		in, ok := ins[k]
		if !ok {
			t.Errorf("stream %+v missing on receiving side", k)
			continue
		}
		errCh := make(chan error, 1)
		go func() { errCh <- sendSegments(out, k.seed()) }()
		if err := readSegments(in, k.seed()); err != nil {
			t.Errorf("stream %+v after close: %v", k, err)
		}
		if err := <-errCh; err != nil {
			t.Errorf("stream %+v send after close: %v", k, err)
		}
		out.Close()
		in.Close()
	}
}

func TestClose_TwoPeers(t *testing.T) {
	leakCheck(t)
	closureScenario(t, 2, 10)
}

func TestClose_TransportWeb(t *testing.T) {
	leakCheck(t)
	closureScenario(t, 5, 4)
}

func TestClose_NeverListened(t *testing.T) {
	leakCheck(t)
	tr := NewTCPTransport()

	done := make(chan struct{})
	go func() {
		tr.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatal("Close hung on a transport that never listened")
	}

	drainUntilClosed(t, "Consume", tr.Consume(), nil)
	drainUntilClosed(t, "Streams", tr.Streams(), nil)
	tr.Close() // double close, no panic
	_, err := tr.Dial(context.Background(), "127.0.0.1:1")
	assertConnClosedErr(t, "Dial after Close", err)
	assertConnClosedErr(t, "ListenAndAccept after Close", tr.ListenAndAccept(freeAddr(t)))
}

func TestClose_FullBuffersBlockedPushers(t *testing.T) {
	leakCheck(t)
	b := startNode(t, nil) // nobody consumes
	a := startNode(t, echo)
	ctx := context.Background()

	p, err := a.tr.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cap(b.tr.Messages)+50; i++ {
		if err := p.Send(ctx, mkMsg(tReq, []byte("m"))); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, waitFor, "Messages buffer to fill", func() bool { return len(b.tr.Messages) == cap(b.tr.Messages) })

	var outs []*Stream
	for i := 0; i < cap(b.tr.StreamsCh)+5; i++ {
		s, err := a.tr.OpenStream(ctx, b.addr, mkMsg(9, []byte{byte(i)}))
		if err != nil {
			t.Fatal(err)
		}
		outs = append(outs, s)
	}
	waitUntil(t, waitFor, "StreamsCh buffer to fill", func() bool { return len(b.tr.StreamsCh) == cap(b.tr.StreamsCh) })

	closeAll(t, b)

	drainUntilClosed(t, "Consume", b.tr.Consume(), nil)
	drainUntilClosed(t, "Streams", b.tr.Streams(), func(s *Stream) { s.Close() })
	validateClosed(t, b)

	for _, s := range outs {
		s.Close()
	}
}

func TestClose_RemoteSeesEOF(t *testing.T) {
	leakCheck(t)

	// inbound side: raw client connected to a transport
	b := startNode(t, echo)
	c := dialRaw(t, b.addr)
	req := mkMsg(tReq, []byte("hi"))
	req.reqID = 5
	if err := writeFrame(c, req); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(waitFor))
	var resp Message
	if err := validateProtocol(c, &resp); err != nil {
		t.Fatal(err)
	}
	if err := readPayload(c, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Type != tAck || resp.reqID != 5 {
		t.Fatalf("bad ack: %+v", resp)
	}
	b.tr.Close()
	var buf [1]byte
	c.SetReadDeadline(time.Now().Add(waitFor))
	if _, err := c.Read(buf[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("inbound remote: expected EOF, got %v", err)
	}

	// outbound side: transport dialled a raw remote
	a := startNode(t, echo)
	r := startRaw(t)
	if _, err := a.tr.Dial(context.Background(), r.addr()); err != nil {
		t.Fatal(err)
	}
	rc := r.accept(t)
	a.tr.Close()
	rc.SetReadDeadline(time.Now().Add(waitFor))
	if _, err := rc.Read(buf[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("outbound remote: expected EOF, got %v", err)
	}
}

// ----------------------------------------------------------- ordinary errors

func TestErr_InvalidProtocol(t *testing.T) {
	leakCheck(t)
	b := startNode(t, echo)
	c := dialRaw(t, b.addr)

	h := makeHeader(mkMsg(tReq, nil))
	copy(h, "XXXX")
	if _, err := c.Write(h[:offReqID+reqIDSize]); err != nil {
		t.Fatal(err)
	}
	expectClosedSilently(t, c)
}

func TestErr_UnsupportedVersion(t *testing.T) {
	leakCheck(t)
	b := startNode(t, echo)
	c := dialRaw(t, b.addr)

	h := makeHeader(mkMsg(tReq, nil))
	h[offVersion] = protocolVersion + 1
	if _, err := c.Write(h[:offReqID+reqIDSize]); err != nil {
		t.Fatal(err)
	}
	expectClosedSilently(t, c)
}

func TestErr_PayloadTooLarge(t *testing.T) {
	leakCheck(t)
	b := startNode(t, echo)
	c := dialRaw(t, b.addr)

	msg := mkMsg(tReq, nil)
	msg.Size = maxPayloadSize + 1
	msg.reqID = 77
	if _, err := c.Write(makeHeader(msg)); err != nil {
		t.Fatal(err)
	}

	c.SetReadDeadline(time.Now().Add(waitFor))
	var resp Message
	if err := validateProtocol(c, &resp); err != nil {
		t.Fatal(err)
	}
	if err := readPayload(c, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Type != uint8(PayloadTooLarge) || resp.reqID != 77 || len(resp.Payload) != 0 {
		t.Fatalf("bad error frame: type=%d reqID=%d payload=%d", resp.Type, resp.reqID, len(resp.Payload))
	}
	expectClosedSilently(t, c)
}

func TestErr_SizeMismatch(t *testing.T) {
	leakCheck(t)
	a, b := startNode(t, echo), startNode(t, echo)
	ctx := context.Background()

	bad := func() *Message {
		return &Message{Metadata: Metadata{Type: tReq, Size: 5}, Payload: []byte("abc")}
	}

	p, err := a.tr.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Send(ctx, bad()); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("Send: expected ErrSizeMismatch, got %v", err)
	}

	p, err = a.tr.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Request(ctx, bad()); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("Request: expected ErrSizeMismatch, got %v", err)
	}
	if pendingLen(p) != 0 {
		t.Fatal("failed Request left a pending entry")
	}
}

func TestErr_ConnClosed(t *testing.T) {
	leakCheck(t)

	if got := (&ErrConnClosed{}).Error(); got != "connection closed" {
		t.Fatalf("unexpected error text %q", got)
	}

	// pending request gets the ErrConnClosed message when the transport closes
	a, b := startNode(t, swallow), startNode(t, swallow)
	p, err := a.tr.Dial(context.Background(), b.addr)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := p.Request(context.Background(), mkMsg(tReq, []byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	closeAll(t, a)
	assertConnClosedMsg(t, recvOr(ch, waitFor))

	// Dial and ListenAndAccept return ErrConnClosed after Close
	_, err = a.tr.Dial(context.Background(), b.addr)
	assertConnClosedErr(t, "Dial", err)
	assertConnClosedErr(t, "ListenAndAccept", a.tr.ListenAndAccept(freeAddr(t)))
}