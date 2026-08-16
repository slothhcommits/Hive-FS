package hive

import (
	"errors"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Transport interface {
	ListenAndAccept(addr string) error
	Consume() <-chan Request
	Streams() <-chan *Stream
	Dial(addr string) (Peer, error)
	OpenStream(string, time.Duration, Metadata) (io.WriteCloser, error)
	Close() error
}

type Request interface {
	Respond(*Message) error
	Message() *Message
}

type Peer interface {
	Send(*Message) error
	Request(*Message) (<-chan *Message, error)
	Unregister(msg *Message)
	RemoteAddr() string
	Close() error
}

type connection struct {
	mu       sync.Mutex
	conn     net.Conn
	isStream atomic.Bool
}

func (c *connection) write(msg *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	return writeFrame(c.conn, msg)
}

type Inbound struct {
	*connection
	msg *Message
}

func (i *Inbound) Respond(msg *Message) error {
	msg.reqID = i.msg.reqID
	return i.write(msg)
}

func (i *Inbound) Message() *Message {
	return i.msg
}

func randUINT32() uint32 {

	var num uint32

	for {
		num = rand.Uint32()
		if num != noReplyReqID {
			break
		}
	}

	return num
}

type OutboundPeer struct {
	*connection
	pendingMu sync.Mutex
	pending   map[uint32]chan *Message
}

func newOutboundPeer(conn net.Conn) *OutboundPeer {
	return &OutboundPeer{
		connection: &connection{mu: sync.Mutex{}, conn: conn},
		pendingMu:  sync.Mutex{},
		pending:    make(map[uint32]chan *Message),
	}
}

func (op *OutboundPeer) register() (uint32, chan *Message) {

	reqID := randUINT32()

	op.pendingMu.Lock()
	defer op.pendingMu.Unlock()

	for {
		if _, ok := op.pending[reqID]; !ok {
			break
		}
		reqID = randUINT32()
	}

	replyCh := make(chan *Message, 1)
	op.pending[reqID] = replyCh

	return reqID, replyCh
}

func (op *OutboundPeer) Unregister(msg *Message) {
	op.pendingMu.Lock()
	delete(op.pending, msg.reqID)
	op.pendingMu.Unlock()
}

func (op *OutboundPeer) Send(msg *Message) error {
	msg.reqID = noReplyReqID
	return op.connection.write(msg)
}

func (op *OutboundPeer) Request(msg *Message) (<-chan *Message, error) {
	id, ch := op.register()
	msg.reqID = id

	if err := op.write(msg); err != nil {
		op.Unregister(msg)
		return nil, err
	}
	return ch, nil
}

func (op *OutboundPeer) pushMsg(msg *Message) {
	op.pendingMu.Lock()
	ch := op.pending[msg.reqID]
	op.pendingMu.Unlock()

	if ch == nil {
		return
	}

	select {
	case ch <- msg:
	default:
	}
	op.Unregister(msg)
}

func (op *OutboundPeer) RemoteAddr() string {
	return op.conn.RemoteAddr().String()
}

func (op *OutboundPeer) Close() error {

	op.pendingMu.Lock()
	defer op.pendingMu.Unlock()

	err := ErrConnClosed{}
	msg := &Message{
		Metadata: Metadata{
			Type: err.code(),
			Size: uint32(len(err.payload())),
		},
		Payload: err.payload(),
	}

	for key, ch := range op.pending {
		select {
		case ch <- msg:
		default:
		}
		delete(op.pending, key)
	}
	return op.conn.Close()
}

const (
	dialTimeout   = 2 * time.Second
	readDeadline  = 60 * time.Second
	writeDeadline = 10 * time.Second
)

type TCPTransport struct {
	obMu          sync.Mutex
	outboundPeers map[string]*OutboundPeer

	Messages  chan Request
	StreamsCh chan *Stream

	wg        sync.WaitGroup
	closeDone sync.Once
	shutdown  chan struct{}
}

func NewTCPTransport() *TCPTransport {
	return &TCPTransport{
		outboundPeers: make(map[string]*OutboundPeer),
		Messages:      make(chan Request, 128),
		StreamsCh:     make(chan *Stream, 16),
		shutdown:      make(chan struct{}),
	}
}

func (t *TCPTransport) ListenAndAccept(addr string) error {

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-t.shutdown
		ln.Close()
	}()

	for {

		select {
		case <-t.shutdown:
			return nil
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		t.wg.Add(1)
		go t.readInbound(conn)
	}
}

func (t *TCPTransport) readInbound(conn net.Conn) {

	defer t.wg.Done()

	connection := &connection{conn: conn, mu: sync.Mutex{}}
	defer func() {
		if !connection.isStream.Load() {
			connection.conn.Close()
		}
	}()

	returnCtxWatcher := make(chan struct{}, 1)
	defer func() { returnCtxWatcher <- struct{}{} }()

	go func() {
		select {
		case <-returnCtxWatcher:
		case <-t.shutdown:
			if !connection.isStream.Load() {
				connection.conn.Close()
			}
		}
	}()

	pushMsg := func(msg *Message) {
		inb := &Inbound{
			connection: connection,
			msg:        msg,
		}
		select {
		case t.Messages <- inb:
		case <-t.shutdown:
		}
	}

	for {

		conn.SetReadDeadline(time.Now().Add(readDeadline))

		msg := &Message{}

		err := validateProtocol(conn, msg)
		if err != nil {
			log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
			return
		}

		switch {

		case (msg.frameFlags & flagStream) == flagStream:

			s, err := getStream(conn)
			if err != nil {
				log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
				return
			}

			select {
			case t.StreamsCh <- s:
				connection.isStream.Store(true)
				conn.SetReadDeadline(time.Time{})
			case <-t.shutdown:
			}
			return

		default:

			err := readPayload(conn, msg)
			if err != nil {
				t.writeError(connection, msg, err)
				log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
				return
			}

			pushMsg(msg)
		}
	}
}

// dial returns an outbound peer.
func (t *TCPTransport) Dial(addr string) (Peer, error) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	key := tcpAddr.String()

	t.obMu.Lock()
	defer t.obMu.Unlock()

	if peer, ok := t.outboundPeers[key]; ok {
		return peer, nil
	}

	conn, err := net.DialTimeout("tcp", key, dialTimeout)
	if err != nil {
		return nil, err
	}

	peer := newOutboundPeer(conn)
	t.outboundPeers[key] = peer
	t.wg.Add(1)
	go t.readOutbound(peer)
	return peer, nil
}

// on what conditions should the peer close?
// 1. caller calls peer.Close(), 2. transport shutdown, 3. ReadDeadline exceeded, protocol error, EOF, conn reset.

func (t *TCPTransport) readOutbound(peer *OutboundPeer) {

	defer t.wg.Done()
	defer peer.Close()
	defer t.dropOutbound(peer.RemoteAddr())

	returnCtxWatcher := make(chan struct{}, 1)
	defer func() { returnCtxWatcher <- struct{}{} }()

	go func() {
		select {
		case <-returnCtxWatcher:
		case <-t.shutdown:
			peer.Close()
		}
	}()

	conn := peer.conn

	for {

		conn.SetReadDeadline(time.Now().Add(readDeadline))

		msg := &Message{}

		err := validateProtocol(conn, msg)
		if err != nil {
			log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
			return
		}

		err = readPayload(conn, msg)
		if err != nil {
			t.writeError(peer.connection, msg, err)
			log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
			return
		}

		peer.pushMsg(msg)
	}

}

func (t *TCPTransport) dropOutbound(addr string) {
	t.obMu.Lock()
	delete(t.outboundPeers, addr)
	t.obMu.Unlock()
}

func (t *TCPTransport) Consume() <-chan Request {
	return t.Messages
}

func (t *TCPTransport) OpenStream(addr string, dialTimeout time.Duration, md Metadata) (io.WriteCloser, error) {

	if md.Size == 0 {
		return nil, errors.New("zero-length stream")
	}

	header := makeHeader(&Message{
		frameFlags: flagStream,
		Metadata:   md,
	})

	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}

	return &streamWriter{
		w:         conn,
		remaining: int(md.Size),
		header:    header,
	}, nil
}

func (t *TCPTransport) Streams() <-chan *Stream {
	return t.StreamsCh
}

func (t *TCPTransport) Close() error {

	t.closeDone.Do(func() { close(t.shutdown) })

	var err error

	t.obMu.Lock()
	for key, obp := range t.outboundPeers {
		if cerr := obp.Close(); cerr != nil && err == nil {
			err = cerr
		}
		delete(t.outboundPeers, key)
	}
	t.obMu.Unlock()

	t.wg.Wait()
	close(t.Messages)
	close(t.StreamsCh)

	return err
}

func (t *TCPTransport) writeError(c *connection, msg *Message, err error) {

	if err != nil {

		var protocErr ProtocolErr
		if errors.As(err, &protocErr) {
			c.write(&Message{
				Metadata: Metadata{
					Type: uint8(protocErr.code()),
					Size: uint32(len(protocErr.payload())),
				},
				reqID:   msg.reqID,
				Payload: protocErr.payload(),
			})
		}
	}

}

type TransportErrCode uint

const (
	ConnClosed TransportErrCode = 251
)

type ErrConnClosed struct{}

func (e *ErrConnClosed) Error() string {
	return "connection closed"
}

func (e *ErrConnClosed) payload() []byte {
	return []byte("connection closed")
}

func (e *ErrConnClosed) code() uint8 { return uint8(ConnClosed) }
