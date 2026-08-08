package networking

import (
	"errors"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

type Transport interface {
	ListenAndAccept(addr string) error
	Consume() <-chan Request
	Dial(addr string) (Peer, error)
	Close() error
}

type Request interface {
	Respond(*Message) error
}

type Peer interface {
	Send(*Message) error
	Request(*Message) (<-chan *Message, error)
	RemoteAddr() string
	Close() error
}

type connection struct {
	outbound bool
	mu       sync.Mutex
	conn     net.Conn
}

func (c *connection) write(msg *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
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
	wg        sync.WaitGroup
}

func newOutboundPeer(conn net.Conn) *OutboundPeer {
	return &OutboundPeer{
		connection: &connection{outbound: true, mu: sync.Mutex{}, conn: conn},
		pendingMu:  sync.Mutex{},
		pending:    make(map[uint32]chan *Message),
		wg:         sync.WaitGroup{},
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

func (op *OutboundPeer) unregister(id uint32) {
	op.pendingMu.Lock()
	delete(op.pending, id)
	op.pendingMu.Unlock()
}

func (op *OutboundPeer) Send(msg *Message) error {
	op.wg.Add(1)
	defer op.wg.Done()
	msg.reqID = noReplyReqID
	return op.connection.write(msg)
}

func (op *OutboundPeer) Request(msg *Message) (<-chan *Message, error) {
	op.wg.Add(1)
	defer op.wg.Done()
	id, ch := op.register()
	msg.reqID = id

	if err := op.write(msg); err != nil {
		op.unregister(id)
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

	ch <- msg
	op.unregister(msg.reqID)
}

func (op *OutboundPeer) RemoteAddr() string {
	return op.conn.RemoteAddr().String()
}

func (op *OutboundPeer) Close() error {

	op.wg.Wait()
	op.pendingMu.Lock()
	defer op.pendingMu.Unlock()

	err := ErrConnClosed{}
	msg := &Message{
		Type:    err.code(),
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

type TCPTransport struct {
	closeDone sync.Once
	done      chan struct{}

	obMu          sync.Mutex
	outboundPeers map[string]*OutboundPeer

	Messages chan Request
	streams  chan io.ReadCloser

	wg sync.WaitGroup
}

func (t *TCPTransport) ListenAndAccept(addr string) error {

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-t.done
		ln.Close()
	}()

	for {

		select {
			case <-t.done:
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

// TODO: Timeouts
func (t *TCPTransport) readInbound(conn net.Conn) {

	defer t.wg.Done()
	defer conn.Close()

	connection := &connection{conn: conn, mu: sync.Mutex{}}
	pushMsg := func(msg *Message) {
		inb := &Inbound{
			connection: connection,
			msg:        msg,
		}
		select {
		case t.Messages <- inb:
		case <-t.done:
		}
	}

	t.read(connection, pushMsg)
}

func (t *TCPTransport) read(c *connection, pushMsg func(*Message)) {

	doneCh := make(chan struct{}, 1)
	defer func() { doneCh <- struct{}{} }()

	go func() {
		select {
		case <-doneCh:
		case <-t.done:
			c.conn.Close()
		}
	}()

	conn := c.conn
	for {

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))

		msg := &Message{}

		err := validateProtocol(conn, msg)
		if err != nil {

			log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
			return
		}

		switch {

		case (msg.Flags & flagStream) == flagStream:

			//TODO:fix loop and pass metadata
			t.streams <- conn

		default:

			err := readPayload(conn, msg)
			if err != nil {

				var protocErr ProtocolErr
				if errors.As(err, &protocErr) {
					c.write(&Message{
						Flags:   0,
						reqID:   msg.reqID,
						Type:    uint8(protocErr.code()),
						Payload: protocErr.payload(),
					})
				}

				log.Printf("peer %s error : %s", conn.RemoteAddr().String(), err.Error())
				return
			}

			pushMsg(msg)
		}
	}
}

// dial gives an outbound peer.
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

	conn, err := net.DialTimeout("tcp", key, 2*time.Second)
	if err != nil {
		return nil, err
	}

	peer := newOutboundPeer(conn)
	t.outboundPeers[key] = peer
	t.wg.Add(1)
	go t.readOutbound(peer)
	return peer, nil
}

func (t *TCPTransport) readOutbound(peer *OutboundPeer) {

	defer t.wg.Done()

	t.read(peer.connection, peer.pushMsg)

	t.dropOutbound(peer.RemoteAddr())
	peer.Close()
}

func (t *TCPTransport) dropOutbound(addr string) {
	t.obMu.Lock()
	delete(t.outboundPeers, addr)
	t.obMu.Unlock()
}

func (t *TCPTransport) Consume() <-chan Request {
	return t.Messages
}

func (t *TCPTransport) Close() {

	t.closeDone.Do(func() { close(t.done) })

	t.obMu.Lock()
	for key, obp := range t.outboundPeers {
		obp.Close()
		delete(t.outboundPeers, key)
	}
	t.obMu.Unlock()

	t.wg.Wait()
	close(t.Messages)
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
