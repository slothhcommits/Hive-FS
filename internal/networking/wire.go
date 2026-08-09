package networking

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

type Stream struct {
	Metadata
	Pipe io.Reader
	io.Closer
}

type streamWriter struct {
	w         io.WriteCloser
	remaining int
	header    []byte
}

func (sw *streamWriter) Write(p []byte) (int, error) {

	if len(p) == 0 {
		return 0, nil
	}

	if sw.remaining < len(p) {
		return 0, errors.New("stream overrun")
	}

	if sw.header != nil {
		_, err := sw.w.Write(sw.header)
		if err != nil {
			return 0, err
		}
		sw.header = nil
	}

	n, err := sw.w.Write(p)
	sw.remaining -= n

	return n, err
}

func (sw *streamWriter) Close() error {
	if sw.remaining != 0 {
		sw.w.Close()
		return errors.New("stream underrun")
	}
	return sw.w.Close()
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

type ErrUnsupportedVersion struct{}

func (e *ErrUnsupportedVersion) Error() string {
	return "unsupported protocol version"
}

func (e *ErrUnsupportedVersion) payload() []byte { return []byte{} }

func (e *ErrUnsupportedVersion) code() ProtocolErrCode { return UnsupportedVersion }

type ErrPayloadTooLarge struct{}

func (e *ErrPayloadTooLarge) Error() string {
	return fmt.Sprintf("payload too large, max payload size : %d \n", maxPayloadSize)
}

func (e *ErrPayloadTooLarge) payload() []byte { return []byte{} }

func (e *ErrPayloadTooLarge) code() ProtocolErrCode { return PayloadTooLarge }

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

func writeFrame(conn io.ReadWriter, msg *Message) error {

	if uint32(len(msg.Payload)) > maxPayloadSize {
		return &ErrPayloadTooLarge{}
	}

	if msg.Size != uint32(len(msg.Payload)) {
		return errors.New("size does not match payload length")
	}

	if _, err := conn.Write(makeHeader(msg)); err != nil {
		return err
	}

	if _, err := conn.Write(msg.Payload); err != nil {
		return err
	}

	return nil
}

func validateProtocol(conn io.ReadCloser, msg *Message) error {

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

func getStream(conn io.ReadCloser) (*Stream, error) {

	off := offMsgFlags

	buf := make([]byte, headerSize-off)
	_, err := io.ReadFull(conn, buf)

	if err != nil {
		return nil, err
	}

	s := &Stream{}
	s.Size = binary.BigEndian.Uint32(buf[offLength-off : offLength-off+lengthSize])
	s.Flags = buf[offMsgFlags-off]
	s.Type = buf[offType-off]
	s.Pipe = io.LimitReader(conn, int64(s.Size))
	s.Closer = conn

	return s, nil
}

func readPayload(conn io.ReadCloser, msg *Message) error {

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
