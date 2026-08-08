package networking

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

/*
 					8bytes	   1byte    2bytes 4bytes     1byte 4bytes
 frame structure : [magicByte][version][flag][requestId][type][length]Payload[checksum]
 			       [0 1 2 3 4 5 6 7, 8, 9 10, 11 12 13 14, 15, 16 17 18 19]
*/

var magicByte = []byte{'0', 'x', 'H', 'i', 'v', 'e', 'F', 'S'} // 0xHiveFS

const (

	// sizes in bytes
	headerSize    uint8 = 20
	magicByteSize uint8 = 8
	versionSize   uint8 = 1
	flagSize      uint8 = 2
	reqIDSize     uint8 = 4
	typeSize      uint8 = 1
	lengthSize    uint8 = 4
	// total = 20

	// header offsets
	offMagic   uint8 = 0
	offVersion uint8 = 8
	offFlags   uint8 = 9
	offReqID   uint8 = 11
	offType    uint8 = 15
	offLength  uint8 = 16

	flagZero   uint16 = 0
	flagStream uint16 = 1

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

type Message struct {
	reqID   uint32
	Flags   uint16
	Type    uint8
	Payload []byte
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
	binary.BigEndian.PutUint16(buf[offFlags:offFlags+flagSize], msg.Flags)
	binary.BigEndian.PutUint32(buf[offReqID:offReqID+reqIDSize], msg.reqID)
	buf[offType] = msg.Type
	binary.BigEndian.PutUint32(buf[offLength:offLength+lengthSize], uint32(len(msg.Payload)))

	return buf
}

func writeFrame(conn io.ReadWriter, msg *Message) error {

	if uint32(len(msg.Payload)) > maxPayloadSize {
		return &ErrPayloadTooLarge{}
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

	flags := binary.BigEndian.Uint16(buf[offFlags : offFlags+flagSize])
	reqID := binary.BigEndian.Uint32(buf[offReqID : offReqID+reqIDSize])

	msg.Flags = flags
	msg.reqID = reqID

	return nil
}

func readPayload(conn io.ReadCloser, msg *Message) error {

	off := offType

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
	_, err = io.ReadFull(conn, payload); 
	
	if err != nil {
		return err
	}

	msg.Type = buf[offType-off]
	msg.Payload = payload

	return nil
}
