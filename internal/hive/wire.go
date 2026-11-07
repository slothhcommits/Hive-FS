package hive

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
)

/*
 					4bytes	   1byte     1byte   4bytes      1byte      1byte   4bytes
 frame structure : [magicByte] [version] [flags] [requestId] [msgFlags] [type] [length]      Payload [checksum]
 			       [0 1 2 3]   [4]       [5]     [6 7 8 9]   [10]       [11]   [12 13 14 15]
*/

var magicByte = []byte("HVFS")

const (

	// sizes in bytes
	headerSize     uint8 = 16
	magicByteSize  uint8 = 4
	versionSize    uint8 = 1
	frameFlagsSize uint8 = 1
	reqIDSize      uint8 = 4
	msgFlagsSize   uint8 = 1
	typeSize       uint8 = 1
	lengthSize     uint8 = 4
	// total = 16

	// header offsets
	offMagic      uint8 = 0
	offVersion    uint8 = 4
	offFrameFlags uint8 = 5
	offReqID      uint8 = 6
	offMsgFlags   uint8 = 10
	offType       uint8 = 11
	offLength     uint8 = 12

	flagZero   uint8 = 0
	flagStream uint8 = 1

	noReplyReqID uint32 = 0

	protocolVersion uint8  = 1
	maxPayloadSize  uint32 = 1024 * 1024
)

// network lib error range 240-255, serves both as error codes and type byte
type ProtocolErrCode uint8

const (
	InvalidProtocol    ProtocolErrCode = 240
	UnsupportedVersion ProtocolErrCode = 241
	PayloadTooLarge    ProtocolErrCode = 242
	CorruptedPayload   ProtocolErrCode = 243
)

type Metadata struct {
	Type  uint8
	Flags uint8
	Size  uint32
}

type Message struct {
	reqID      uint32
	frameFlags uint8
	Metadata
	Payload []byte
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type ShortMessage struct {
	Type    uint8
	Message proto.Message
}

type Stream struct {
	Metadata      *Message
	stopAfterFunc func() bool

	conn         net.Conn
	hashedReader io.Reader
	rHash        hash.Hash32
	hashedWriter io.Writer
	wHash        hash.Hash32

	Inbound chan *Message
}

func newStream(conn net.Conn, meta *Message, stop func() bool) *Stream {

	s := &Stream{
		Metadata:      meta,
		stopAfterFunc: stop,
		rHash:         crc32.New(castagnoli),
		wHash:         crc32.New(castagnoli),
		Inbound:       make(chan *Message, 10),
	}

	s.conn = conn
	s.hashedReader = io.TeeReader(conn, s.rHash)
	s.hashedWriter = io.MultiWriter(conn, s.wHash)

	return s
}

func (s *Stream) Read(p []byte) (int, error) {
	return s.hashedReader.Read(p)
}

func (s *Stream) ValidateRead() error {

	var buf [4]byte
	if _, err := io.ReadFull(s.conn, buf[:]); err != nil {
		return err
	}

	checksum := binary.BigEndian.Uint32(buf[:])
	sum := s.rHash.Sum32()
	// the next message on this stream starts its own checksum
	s.rHash.Reset()

	if sum != checksum {
		return errors.New("corrupted stream")
	}

	return nil
}

func (s *Stream) ReadStreamMessage() (*Message, error) {

	var metaBuf [5]byte
	_, err := io.ReadFull(s.conn, metaBuf[:])
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(metaBuf[:4])

	if length > maxPayloadSize {
		return nil, fmt.Errorf("exceeded max payload size")
	}
	buf := make([]byte, length)

	_, err = io.ReadFull(s.conn, buf)
	if err != nil {
		return nil, err
	}

	return &Message{
		Payload: buf,
		Metadata: Metadata{
			Type: metaBuf[4],
			Size: uint32(len(buf)),
		},
	}, nil
}

func (s *Stream) ListenInbound(ctx context.Context) {

	defer s.Close()
	defer close(s.Inbound)

	for {
		msg, err := s.ReadStreamMessage()
		if err != nil {
			return
		}
		select {
		case s.Inbound <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Stream) Write(p []byte) (int, error) {

	if len(p) == 0 {
		return 0, nil
	}

	n, err := s.hashedWriter.Write(p)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (s *Stream) WriteStreamMessage(msg *ShortMessage) error {

	buf := make([]byte, 5, 5+proto.Size(msg.Message))
	buf[4] = msg.Type
	buf, err := proto.MarshalOptions{}.MarshalAppend(buf, msg.Message)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-5))
	_, err = s.conn.Write(buf)

	return err
}

func (s *Stream) WriteChecksum() error {
	var t [4]byte
	binary.BigEndian.PutUint32(t[:], s.wHash.Sum32())
	s.wHash.Reset()
	_, err := s.conn.Write(t[:])
	return err
}

func (s *Stream) Close() error {

	if s.stopAfterFunc != nil {
		s.stopAfterFunc()
	}

	return s.conn.Close()
}

type ProtocolErr interface {
	error
	payload() []byte
	code() ProtocolErrCode
}

type ErrInvalidProtocol struct{}

func (e *ErrInvalidProtocol) Error() string {
	return "invalid protocol"
}

func (e *ErrInvalidProtocol) payload() []byte { return []byte{} }

func (e *ErrInvalidProtocol) code() ProtocolErrCode { return InvalidProtocol }

func (e *ErrInvalidProtocol) Is(target error) bool {
	_, ok := target.(*ErrInvalidProtocol)
	return ok
}

type ErrUnsupportedVersion struct{}

func (e *ErrUnsupportedVersion) Error() string {
	return "unsupported protocol version"
}

func (e *ErrUnsupportedVersion) payload() []byte { return []byte{} }

func (e *ErrUnsupportedVersion) code() ProtocolErrCode { return UnsupportedVersion }

func (e *ErrUnsupportedVersion) Is(target error) bool {
	_, ok := target.(*ErrUnsupportedVersion)
	return ok
}

type ErrPayloadTooLarge struct{}

func (e *ErrPayloadTooLarge) Error() string {
	return fmt.Sprintf("payload too large, max payload size : %d \n", maxPayloadSize)
}

func (e *ErrPayloadTooLarge) payload() []byte { return []byte{} }

func (e *ErrPayloadTooLarge) code() ProtocolErrCode { return PayloadTooLarge }

func (e *ErrPayloadTooLarge) Is(target error) bool {
	_, ok := target.(*ErrPayloadTooLarge)
	return ok
}

var ErrSizeMismatch = errors.New("size field and payload length are unequal")

func makeHeader(msg *Message) []byte {

	buf := make([]byte, headerSize)
	copy(buf[offMagic:offMagic+magicByteSize], magicByte[:])

	buf[offVersion] = protocolVersion
	buf[offFrameFlags] = msg.frameFlags
	binary.BigEndian.PutUint32(buf[offReqID:offReqID+reqIDSize], msg.reqID)
	buf[offMsgFlags] = msg.Flags
	buf[offType] = msg.Type
	binary.BigEndian.PutUint32(buf[offLength:offLength+lengthSize], msg.Size)

	return buf
}

func writeFrame(conn io.Writer, msg *Message) error {

	if uint32(len(msg.Payload)) > maxPayloadSize {
		return &ErrPayloadTooLarge{}
	}

	if msg.Size != uint32(len(msg.Payload)) {
		return ErrSizeMismatch
	}

	if _, err := conn.Write(makeHeader(msg)); err != nil {
		return err
	}

	if _, err := conn.Write(msg.Payload); err != nil {
		return err
	}

	return nil
}

func validateProtocol(conn io.Reader, msg *Message) error {

	buf := make([]byte, offReqID+reqIDSize)
	_, err := io.ReadFull(conn, buf)

	if err != nil {
		return err
	}

	if !bytes.Equal(buf[offMagic:offMagic+magicByteSize], magicByte) {
		return &ErrInvalidProtocol{}
	}

	if buf[offVersion] != protocolVersion {
		return &ErrUnsupportedVersion{}
	}

	msg.frameFlags = buf[offFrameFlags]
	msg.reqID = binary.BigEndian.Uint32(buf[offReqID : offReqID+reqIDSize])

	return nil
}

func readPayload(conn io.Reader, msg *Message) error {

	off := offMsgFlags

	buf := make([]byte, headerSize-off)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		return err
	}

	payloadSize := binary.BigEndian.Uint32(buf[offLength-off : offLength-off+lengthSize])

	if payloadSize > maxPayloadSize {
		return &ErrPayloadTooLarge{}
	}

	payload := make([]byte, payloadSize)
	_, err = io.ReadFull(conn, payload)

	if err != nil {
		return err
	}

	msg.Flags = buf[offMsgFlags-off]
	msg.Type = buf[offType-off]
	msg.Payload = payload
	msg.Size = payloadSize

	return nil
}
