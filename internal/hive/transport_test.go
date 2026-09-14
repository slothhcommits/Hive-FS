package hive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// freeAddr grabs a port the kernel says is free and hands it back. There is a
// small race between closing here and the transport binding it, but nothing
// else in the test binary is listening, so in practice it holds.
func freeAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	return addr
}

type testServer struct {
	tr   *TCPTransport
	addr string
}

// newTestServer starts a transport on a free port and pumps its request and
// stream channels into the supplied handlers. Both handlers may be nil.
// The transport is closed when the test finishes.
func newTestServer(t *testing.T, onReq func(Request), onStream func(*Stream)) *testServer {
	t.Helper()

	srv := &testServer{
		tr:   NewTCPTransport(),
		addr: freeAddr(t),
	}

	go srv.tr.ListenAndAccept(srv.addr)

	go func() {
		for req := range srv.tr.Consume() {
			if onReq != nil {
				go onReq(req)
			}
		}
	}()

	go func() {
		for s := range srv.tr.Streams() {
			if onStream != nil {
				go onStream(s)
			} else {
				s.Close()
			}
		}
	}()

	srv.waitReady(t)
	t.Cleanup(func() { srv.tr.Close() })

	return srv
}

// waitReady blocks until the listener accepts connections.
func (s *testServer) waitReady(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", s.addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("server never came up on %s", s.addr)
}

// echo answers every request with the same type and payload it received.
func echo(req Request) {
	in := req.Message()
	req.Respond(&Message{
		Metadata: Metadata{Type: in.Type, Flags: in.Flags, Size: in.Size},
		Payload:  in.Payload,
	})
}

func msg(msgType uint8, payload []byte) *Message {
	return &Message{
		Metadata: Metadata{Type: msgType, Size: uint32(len(payload))},
		Payload:  payload,
	}
}

// await waits for one reply or fails the test.
func await(t *testing.T, ch <-chan *Message, within time.Duration) *Message {
	t.Helper()

	select {
	case m := <-ch:
		if m == nil {
			t.Fatal("reply channel closed without a message")
		}
		return m
	case <-time.After(within):
		t.Fatal("timed out waiting for a reply")
		return nil
	}
}

// ---------------------------------------------------------------------------
// request / response
// ---------------------------------------------------------------------------

func TestRequestResponse(t *testing.T) {

	srv := newTestServer(t, echo, nil)

	peer, err := NewTCPTransport().Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer peer.Close()

	t.Run("round trip", func(t *testing.T) {

		ch, err := peer.Request(msg(7, []byte("ping")))
		if err != nil {
			t.Fatalf("request: %v", err)
		}

		res := await(t, ch, 2*time.Second)

		if res.Type != 7 {
			t.Errorf("type = %d, want 7", res.Type)
		}
		if string(res.Payload) != "ping" {
			t.Errorf("payload = %q, want %q", res.Payload, "ping")
		}
	})

	t.Run("empty payload", func(t *testing.T) {

		ch, err := peer.Request(msg(3, nil))
		if err != nil {
			t.Fatalf("request: %v", err)
		}

		res := await(t, ch, 2*time.Second)

		if res.Size != 0 || len(res.Payload) != 0 {
			t.Errorf("got %d bytes, want an empty payload", len(res.Payload))
		}
	})

	t.Run("max payload", func(t *testing.T) {

		payload := bytes.Repeat([]byte{0x5a}, int(maxPayloadSize))

		ch, err := peer.Request(msg(4, payload))
		if err != nil {
			t.Fatalf("request: %v", err)
		}

		res := await(t, ch, 5*time.Second)

		if !bytes.Equal(res.Payload, payload) {
			t.Errorf("payload mismatch: got %d bytes, want %d", len(res.Payload), len(payload))
		}
	})

	t.Run("oversize payload is refused locally", func(t *testing.T) {

		payload := bytes.Repeat([]byte{1}, int(maxPayloadSize)+1)

		_, err := peer.Request(msg(5, payload))

		var target *ErrPayloadTooLarge
		if !errors.As(err, &target) {
			t.Fatalf("err = %v, want *ErrPayloadTooLarge", err)
		}
	})
}

// Replies must find their own request even when many are in flight at once on
// the same connection, and even when the server answers out of order.
func TestReplyCorrelation(t *testing.T) {

	const inFlight = 200

	// Hold every request until all of them have arrived, then answer them in
	// reverse order so that no reply can be matched by arrival order alone.
	var (
		mu      sync.Mutex
		pending []Request
		release = make(chan struct{})
		once    sync.Once
	)

	srv := newTestServer(t, func(req Request) {
		mu.Lock()
		pending = append(pending, req)
		full := len(pending) == inFlight
		mu.Unlock()

		if full {
			once.Do(func() {
				mu.Lock()
				batch := pending
				mu.Unlock()

				for i := len(batch) - 1; i >= 0; i-- {
					echo(batch[i])
				}
				close(release)
			})
		}
	}, nil)

	peer, err := NewTCPTransport().Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer peer.Close()

	chans := make([]<-chan *Message, inFlight)
	for i := range chans {
		ch, err := peer.Request(msg(1, []byte(fmt.Sprintf("req-%d", i))))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		chans[i] = ch
	}

	select {
	case <-release:
	case <-time.After(10 * time.Second):
		t.Fatal("server never received all the requests")
	}

	for i, ch := range chans {
		res := await(t, ch, 10*time.Second)
		if want := fmt.Sprintf("req-%d", i); string(res.Payload) != want {
			t.Fatalf("request %d got reply %q, want %q — replies are crossing wires",
				i, res.Payload, want)
		}
	}
}

func TestSendNoReply(t *testing.T) {

	got := make(chan *Message, 1)

	srv := newTestServer(t, func(req Request) {
		select {
		case got <- req.Message():
		default:
		}
	}, nil)

	peer, err := NewTCPTransport().Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer peer.Close()

	if err := peer.Send(msg(9, []byte("fire and forget"))); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case in := <-got:
		if in.reqID != noReplyReqID {
			t.Errorf("reqID = %d, want %d for a fire-and-forget message", in.reqID, noReplyReqID)
		}
		if string(in.Payload) != "fire and forget" {
			t.Errorf("payload = %q", in.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the message")
	}
}

// A caller that gives up must be able to drop its slot, and a late reply for
// that slot must not disturb anything else.
func TestUnregisterDropsLateReply(t *testing.T) {

	hold := make(chan struct{})

	srv := newTestServer(t, func(req Request) {
		<-hold
		echo(req)
	}, nil)

	peer, err := NewTCPTransport().Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer peer.Close()

	abandoned := msg(1, []byte("abandoned"))
	if _, err := peer.Request(abandoned); err != nil {
		t.Fatalf("request: %v", err)
	}
	peer.Unregister(abandoned)

	kept := msg(1, []byte("kept"))
	ch, err := peer.Request(kept)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	close(hold)

	res := await(t, ch, 5*time.Second)
	if string(res.Payload) != "kept" {
		t.Fatalf("payload = %q, want %q", res.Payload, "kept")
	}
}

func TestDialCachesPeers(t *testing.T) {

	srv := newTestServer(t, echo, nil)
	client := NewTCPTransport()
	defer client.Close()

	first, err := client.Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	second, err := client.Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial again: %v", err)
	}

	if first != second {
		t.Fatal("a second dial to the same address opened a new connection")
	}
}

func TestDialUnreachable(t *testing.T) {

	client := NewTCPTransport()
	defer client.Close()

	if _, err := client.Dial(freeAddr(t)); err == nil {
		t.Fatal("dialling a dead address returned no error")
	}
}

// Every pending caller must be woken when the connection goes away, otherwise
// a dead peer parks its callers forever.
func TestPendingWokenOnClose(t *testing.T) {

	hold := make(chan struct{})
	defer close(hold)

	srv := newTestServer(t, func(req Request) { <-hold }, nil)

	peer, err := NewTCPTransport().Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	chans := make([]<-chan *Message, 8)
	for i := range chans {
		ch, err := peer.Request(msg(1, []byte("waiting")))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		chans[i] = ch
	}

	peer.Close()

	for i, ch := range chans {
		res := await(t, ch, 2*time.Second)
		if res.Type != uint8(ConnClosed) {
			t.Fatalf("caller %d got type %d, want the connection-closed code %d",
				i, res.Type, ConnClosed)
		}
	}
}

// ---------------------------------------------------------------------------
// protocol rejection over a live socket
// ---------------------------------------------------------------------------

func TestServerRejectsBadFrames(t *testing.T) {

	srv := newTestServer(t, echo, nil)

	cases := []struct {
		name  string
		frame func() []byte
		want  ProtocolErrCode
	}{
		{
			name: "bad magic",
			frame: func() []byte {
				h := makeHeader(msg(1, nil))
				h[0] = 'X'
				return h
			},
			want: InvalidProtocol,
		},
		{
			name: "unsupported version",
			frame: func() []byte {
				h := makeHeader(msg(1, nil))
				h[offVersion] = protocolVersion + 1
				return h
			},
			want: UnsupportedVersion,
		},
		{
			name: "declared length over max",
			frame: func() []byte {
				m := msg(1, nil)
				m.Size = maxPayloadSize + 1
				return makeHeader(m)
			},
			want: PayloadTooLarge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			conn, err := net.DialTimeout("tcp", srv.addr, dialTimeout)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()

			if _, err := conn.Write(tc.frame()); err != nil {
				t.Fatalf("write: %v", err)
			}

			conn.SetReadDeadline(time.Now().Add(2 * time.Second))

			// A version or magic failure is detected before any reply can be
			// framed, so the server simply drops the connection. A length
			// failure is reported back as an error frame. Either way the
			// connection must not stay open serving the bad peer.
			reply := &Message{}
			err = validateProtocol(conn, reply)

			switch tc.want {
			case PayloadTooLarge:
				if err != nil {
					t.Fatalf("expected an error frame back, got %v", err)
				}
				if err := readPayload(conn, reply); err != nil {
					t.Fatalf("read error frame: %v", err)
				}
				if reply.Type != uint8(tc.want) {
					t.Fatalf("error code = %d, want %d", reply.Type, tc.want)
				}
			default:
				if err == nil {
					t.Fatal("server kept talking to a peer with a bad header")
				}
			}
		})
	}
}

// A payload that happens to contain the magic bytes must not be mistaken for
// the start of a new frame.
func TestPayloadContainingMagic(t *testing.T) {

	srv := newTestServer(t, echo, nil)

	peer, err := NewTCPTransport().Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer peer.Close()

	payload := bytes.Repeat(append([]byte("HVFS"), 1, 0, 0, 0, 0, 0, 0, 1, 9, 0, 0, 0, 0), 64)

	ch, err := peer.Request(msg(2, payload))
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	res := await(t, ch, 2*time.Second)
	if !bytes.Equal(res.Payload, payload) {
		t.Fatal("payload carrying the magic bytes was not framed correctly")
	}
}

// ---------------------------------------------------------------------------
// streams
// ---------------------------------------------------------------------------

// streamSink drains a stream, and reports its length and checksum.
type streamResult struct {
	streamType uint8
	n          int64
	sum        uint32
	err        error
}

func streamSink(out chan<- streamResult) func(*Stream) {
	return func(s *Stream) {
		defer s.Close()

		h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		n, err := io.Copy(h, s.Pipe)

		out <- streamResult{streamType: s.Type, n: n, sum: h.Sum32(), err: err}
	}
}

func randomBytes(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(rand.UintN(256))
	}
	return buf
}

func TestStream(t *testing.T) {

	results := make(chan streamResult, 8)
	srv := newTestServer(t, echo, streamSink(results))

	client := NewTCPTransport()
	defer client.Close()

	sizes := []int{
		1,
		int(blockish),
		1 << 20,       // 1 MB, the message cap — a stream is not bound by it
		4<<20 + 12345, // not a round number of anything
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {

			data := randomBytes(size)
			want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

			w, err := client.OpenStream(srv.addr, dialTimeout, Metadata{
				Type: 21,
				Size: uint32(size),
			})
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}

			// write in chunks, the way a real sender would
			for off := 0; off < len(data); off += 64 << 10 {
				end := min(off+(64<<10), len(data))
				if _, err := w.Write(data[off:end]); err != nil {
					t.Fatalf("write at %d: %v", off, err)
				}
			}

			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			select {
			case got := <-results:
				if got.err != nil {
					t.Fatalf("server side read: %v", got.err)
				}
				if got.n != int64(size) {
					t.Fatalf("server read %d bytes, want %d", got.n, size)
				}
				if got.sum != want {
					t.Fatal("stream arrived corrupted")
				}
				if got.streamType != 21 {
					t.Errorf("stream type = %d, want 21", got.streamType)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("server never finished reading the stream")
			}
		})
	}
}

const blockish = 64 << 10

func TestStreamGuards(t *testing.T) {

	results := make(chan streamResult, 4)
	srv := newTestServer(t, echo, streamSink(results))

	client := NewTCPTransport()
	defer client.Close()

	t.Run("zero length is refused", func(t *testing.T) {
		if _, err := client.OpenStream(srv.addr, dialTimeout, Metadata{Type: 1, Size: 0}); err == nil {
			t.Fatal("a zero-length stream was accepted")
		}
	})

	t.Run("overrun", func(t *testing.T) {

		w, err := client.OpenStream(srv.addr, dialTimeout, Metadata{Type: 1, Size: 4})
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		defer w.Close()

		if _, err := w.Write([]byte("more than four")); err == nil {
			t.Fatal("writing past the declared size was allowed")
		}
	})

	t.Run("underrun", func(t *testing.T) {

		w, err := client.OpenStream(srv.addr, dialTimeout, Metadata{Type: 1, Size: 16})
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}

		if _, err := w.Write([]byte("short")); err != nil {
			t.Fatalf("write: %v", err)
		}

		if err := w.Close(); err == nil {
			t.Fatal("closing before the declared size was reported as success")
		}

		// the receiver must see a truncated read, never a complete one
		select {
		case got := <-results:
			if got.err == nil && got.n == 16 {
				t.Fatal("receiver reported a complete stream for a truncated send")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("receiver never returned for the truncated stream")
		}
	})
}

// A stream arrives on its own channel, not as a message, and the connection is
// handed over whole — messages sent earlier on other connections keep working.
func TestStreamAndMessagesCoexist(t *testing.T) {

	results := make(chan streamResult, 4)
	srv := newTestServer(t, echo, streamSink(results))

	client := NewTCPTransport()
	defer client.Close()

	peer, err := client.Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	data := randomBytes(1 << 20)
	want := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

	w, err := client.OpenStream(srv.addr, dialTimeout, Metadata{Type: 5, Size: uint32(len(data))})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := w.Write(data)
		if err != nil {
			done <- err
			return
		}
		done <- w.Close()
	}()

	// control traffic while the bulk transfer is in flight
	for i := 0; i < 20; i++ {
		ch, err := peer.Request(msg(1, []byte(fmt.Sprintf("control-%d", i))))
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		res := await(t, ch, 5*time.Second)
		if want := fmt.Sprintf("control-%d", i); string(res.Payload) != want {
			t.Fatalf("reply = %q, want %q", res.Payload, want)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("stream: %v", err)
	}

	select {
	case got := <-results:
		if got.err != nil || got.sum != want {
			t.Fatal("stream did not arrive intact alongside control traffic")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stream never completed")
	}
}

// ---------------------------------------------------------------------------
// shutdown
// ---------------------------------------------------------------------------

func TestCloseIsIdempotentAndReleasesChannels(t *testing.T) {

	tr := NewTCPTransport()
	addr := freeAddr(t)

	listening := make(chan struct{})
	go func(tr *TCPTransport, addr string) {
		close(listening)
		tr.ListenAndAccept(addr)
	}(tr, addr)
	<-listening

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			c.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// consumers ranging over the channels must fall out rather than block
	for range tr.Consume() {
	}
	for range tr.Streams() {
	}
}

// ---------------------------------------------------------------------------
// stress
// ---------------------------------------------------------------------------

// Many callers hammering one cached connection. Every reply must come back to
// the caller that asked for it, with no lost, duplicated or crossed messages.
func TestStressConcurrentRequests(t *testing.T) {

	if testing.Short() {
		t.Skip("stress test")
	}

	const (
		callers         = 64
		perCaller       = 200
		total           = callers * perCaller
		payloadSizeMask = 511
	)

	var served atomic.Int64

	srv := newTestServer(t, func(req Request) {
		served.Add(1)
		echo(req)
	}, nil)

	client := NewTCPTransport()
	defer client.Close()

	peer, err := client.Dial(srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	var (
		wg   sync.WaitGroup
		fail atomic.Value
	)

	start := time.Now()

	for c := 0; c < callers; c++ {
		wg.Add(1)
		go func(caller int) {
			defer wg.Done()

			for i := 0; i < perCaller; i++ {

				// unique, self-describing payload of varying length
				body := make([]byte, 8+((caller*perCaller+i)&payloadSizeMask))
				binary.BigEndian.PutUint32(body[0:], uint32(caller))
				binary.BigEndian.PutUint32(body[4:], uint32(i))

				ch, err := peer.Request(msg(1, body))
				if err != nil {
					fail.Store(fmt.Sprintf("caller %d req %d: %v", caller, i, err))
					return
				}

				select {
				case res := <-ch:
					if res == nil {
						fail.Store(fmt.Sprintf("caller %d req %d: nil reply", caller, i))
						return
					}
					if !bytes.Equal(res.Payload, body) {
						fail.Store(fmt.Sprintf("caller %d req %d: reply does not match request", caller, i))
						return
					}
				case <-time.After(30 * time.Second):
					fail.Store(fmt.Sprintf("caller %d req %d: timed out", caller, i))
					return
				}
			}
		}(c)
	}

	wg.Wait()

	if v := fail.Load(); v != nil {
		t.Fatal(v)
	}

	elapsed := time.Since(start)

	if got := served.Load(); got != total {
		t.Fatalf("server handled %d requests, want %d", got, total)
	}

	t.Logf("%d requests over one connection in %s (%.0f req/s)",
		total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
}

// Many independent clients, each with its own connection, all hitting one
// server at the same time.
func TestStressManyConnections(t *testing.T) {

	if testing.Short() {
		t.Skip("stress test")
	}

	const (
		clients   = 50
		perClient = 40
	)

	srv := newTestServer(t, echo, nil)

	var (
		wg   sync.WaitGroup
		fail atomic.Value
		ok   atomic.Int64
	)

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			tr := NewTCPTransport()
			defer tr.Close()

			peer, err := tr.Dial(srv.addr)
			if err != nil {
				fail.Store(fmt.Sprintf("client %d dial: %v", id, err))
				return
			}

			for i := 0; i < perClient; i++ {
				want := []byte(fmt.Sprintf("client-%d-msg-%d", id, i))

				ch, err := peer.Request(msg(1, want))
				if err != nil {
					fail.Store(fmt.Sprintf("client %d req %d: %v", id, i, err))
					return
				}

				select {
				case res := <-ch:
					if res == nil || !bytes.Equal(res.Payload, want) {
						fail.Store(fmt.Sprintf("client %d req %d: bad reply", id, i))
						return
					}
					ok.Add(1)
				case <-time.After(30 * time.Second):
					fail.Store(fmt.Sprintf("client %d req %d: timed out", id, i))
					return
				}
			}
		}(c)
	}

	wg.Wait()

	if v := fail.Load(); v != nil {
		t.Fatal(v)
	}

	if got, want := ok.Load(), int64(clients*perClient); got != want {
		t.Fatalf("%d successful round trips, want %d", got, want)
	}

	t.Logf("%d clients x %d requests all correct", clients, perClient)
}

// Several bulk transfers at once, each verified by checksum. This is the shape
// of a real write: many chunk payloads landing on one chunkserver together.
func TestStressConcurrentStreams(t *testing.T) {

	if testing.Short() {
		t.Skip("stress test")
	}

	const (
		streams    = 12
		streamSize = 2 << 20
	)

	results := make(chan streamResult, streams)
	srv := newTestServer(t, echo, streamSink(results))

	client := NewTCPTransport()
	defer client.Close()

	payloads := make([][]byte, streams)
	sums := make(map[uint32]int, streams)

	for i := range payloads {
		payloads[i] = randomBytes(streamSize)
		sums[crc32.Checksum(payloads[i], crc32.MakeTable(crc32.Castagnoli))] = i
	}

	var (
		wg   sync.WaitGroup
		fail atomic.Value
	)

	start := time.Now()

	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			w, err := client.OpenStream(srv.addr, dialTimeout, Metadata{
				Type: 21,
				Size: uint32(streamSize),
			})
			if err != nil {
				fail.Store(fmt.Sprintf("stream %d open: %v", id, err))
				return
			}

			if _, err := w.Write(payloads[id]); err != nil {
				fail.Store(fmt.Sprintf("stream %d write: %v", id, err))
				return
			}
			if err := w.Close(); err != nil {
				fail.Store(fmt.Sprintf("stream %d close: %v", id, err))
			}
		}(i)
	}

	wg.Wait()

	if v := fail.Load(); v != nil {
		t.Fatal(v)
	}

	seen := make(map[int]bool, streams)

	for i := 0; i < streams; i++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("stream read: %v", got.err)
			}
			if got.n != streamSize {
				t.Fatalf("read %d bytes, want %d", got.n, streamSize)
			}
			id, ok := sums[got.sum]
			if !ok {
				t.Fatal("a stream arrived with a checksum matching none of the payloads")
			}
			if seen[id] {
				t.Fatalf("payload %d was delivered twice", id)
			}
			seen[id] = true
		case <-time.After(60 * time.Second):
			t.Fatalf("only %d of %d streams completed", i, streams)
		}
	}

	elapsed := time.Since(start)
	mb := float64(streams*streamSize) / (1 << 20)

	t.Logf("%d concurrent streams, %.0f MB total in %s (%.1f MB/s), all checksums matched",
		streams, mb, elapsed.Round(time.Millisecond), mb/elapsed.Seconds())
}
