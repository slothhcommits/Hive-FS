package hive

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"testing"

	"github.com/slothhcommits/Hive-FS/internal/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func VersionOneMessageToBytes(msg *Message) []byte {

	if msg == nil {
		return []byte{}
	}

	buf := bytes.Buffer{}

	buf.WriteString("HVFS")                         // magic Byte
	buf.WriteByte(1)                                // protocol version
	buf.WriteByte(msg.frameFlags)                   // frameFlags
	binary.Write(&buf, binary.BigEndian, msg.reqID) // request ID
	buf.WriteByte(msg.Metadata.Flags)               // message Flags
	buf.WriteByte(msg.Type)                         // Type
	binary.Write(&buf, binary.BigEndian, msg.Size)  // length
	buf.Write(msg.Payload)                          // payload

	return buf.Bytes()
}

func randomPayload(n uint32) []byte {
	buf := make([]byte, n)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	return buf
}

func TestMakeHeader(t *testing.T) {

	cases := []struct {
		name  string
		input *Message
	}{
		{
			name: "ordinary",
			input: &Message{
				reqID:      16777217,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type:  211,
					Flags: flagZero,
					Size:  1024 * 1024,
				},
			},
		},
		{
			name: "stream",
			input: &Message{
				reqID:      0,
				frameFlags: flagStream,
				Metadata: Metadata{
					Type:  211,
					Flags: flagZero,
					Size:  1024 * 1024 * 250,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			res := makeHeader(tc.input)
			expected := VersionOneMessageToBytes(tc.input)
			if !bytes.Equal(res, expected) {
				t.Errorf("got = %v \n want = %v \n", res, expected)
				return
			}
		})
	}
}

func TestWriteFrame(t *testing.T) {

	cases := []struct {
		name    string
		input   *Message
		wantErr error
	}{
		{
			name: "ordinary",
			input: &Message{
				reqID:      16777217,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type:  211,
					Flags: flagZero,
					Size:  maxPayloadSize,
				},
				Payload: randomPayload(maxPayloadSize),
			},
			wantErr: nil,
		},
		{
			name: "exceed max payload size",
			input: &Message{
				reqID:      16777217,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type:  211,
					Flags: flagZero,
					Size:  maxPayloadSize + 1,
				},
				Payload: randomPayload(maxPayloadSize + 1),
			},
			wantErr: &ErrPayloadTooLarge{},
		},
		{
			name: "mismatch payload length and size",
			input: &Message{
				reqID:      16777217,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type:  211,
					Flags: flagZero,
					Size:  1024,
				},
				Payload: randomPayload(1025),
			},
			wantErr: ErrSizeMismatch,
		},
	}

	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {

			pipe := &bytes.Buffer{}
			err := writeFrame(pipe, tc.input)
			assert.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			expected := VersionOneMessageToBytes(tc.input)

			result := make([]byte, len(expected))

			_, err = io.ReadFull(pipe, result)
			assert.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			assert.Equal(t, expected, result)
		})
	}

}

func TestValidateProtocol(t *testing.T) {

	cases := []struct {
		name    string
		input   []byte
		want    *Message
		wantErr error
	}{
		{
			name: "ordinary",
			want: &Message{
				reqID:      1024,
				frameFlags: flagZero,
			},
			input: VersionOneMessageToBytes(&Message{
				reqID:      1024,
				frameFlags: flagZero,
			}),
			wantErr: nil,
		},
		{
			name: "bad magic byte",
			want: &Message{
				reqID:      1024,
				frameFlags: flagZero,
			},
			input: func() []byte {
				buf := VersionOneMessageToBytes(&Message{
					reqID:      1024,
					frameFlags: flagZero,
				})
				buf[0] = byte('B')
				buf[1] = byte('A')
				buf[2] = byte('D')
				buf[3] = byte('M')

				return buf
			}(),
			wantErr: &ErrInvalidProtocol{},
		},
		{
			name: "unsupported protocol",
			want: &Message{
				reqID:      1024,
				frameFlags: flagZero,
			},
			input: func() []byte {
				buf := VersionOneMessageToBytes(&Message{
					reqID:      1024,
					frameFlags: flagZero,
				})
				buf[4] = 5
				return buf
			}(),

			wantErr: &ErrUnsupportedVersion{},
		},
		{
			name: "short header",
			want: &Message{
				reqID:      1024,
				frameFlags: flagZero,
			},
			input: func() []byte {
				buf := VersionOneMessageToBytes(&Message{
					reqID:      1024,
					frameFlags: flagZero,
				})

				buf = buf[:5]

				return buf
			}(),
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {

			pipe := &bytes.Buffer{}

			_, err := pipe.Write(tc.input)
			require.NoError(t, err)

			got := &Message{}

			validationErr := validateProtocol(pipe, got)

			assert.ErrorIs(t, validationErr, tc.wantErr)
			if tc.wantErr != nil {
				return
			}

			assert.Equal(t, tc.want, got)
		})
	}

}

func TestReadPayload(t *testing.T) {

	cases := []struct {
		name    string
		want    *Message
		wantErr error
	}{
		{
			name: "ordinary",
			want: &Message{
				reqID:      99999,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type: 225,
					Size: 1024,
				},
				Payload: randomPayload(1024),
			},
			wantErr: nil,
		},

		{
			name: "exceed max size",
			want: &Message{
				reqID:      99999,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type: 225,
					Size: maxPayloadSize + 1,
				},
				Payload: randomPayload(maxPayloadSize + 1),
			},
			wantErr: &ErrPayloadTooLarge{},
		},

		{
			name:    "clean close, no bytes sent",
			want:    nil,
			wantErr: io.EOF,
		},
		{
			name: "truncated read",
			want: &Message{
				reqID:      99999,
				frameFlags: flagZero,
				Metadata: Metadata{
					Type: 225,
					Size: 1024,
				},
				Payload: randomPayload(512),
			},
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {

			pipe := &bytes.Buffer{}

			_, err := pipe.Write(VersionOneMessageToBytes(tc.want))
			require.NoError(t, err)

			got := &Message{}

			err = validateProtocol(pipe, got)
			if err != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}

			err = readPayload(pipe, got)
			if err != nil {
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}

			assert.ErrorIs(t, nil, tc.wantErr)

			if tc.want == nil {
				got = nil
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWireE2E(t *testing.T) {

	cases := []struct {
		name    string
		input   []*Message
		wantErr error
	}{
		{
			name: "ordinary",
			input: []*Message{
				{
					reqID:      44444,
					frameFlags: flagStream,
					Metadata: Metadata{
						Type:  255,
						Flags: 255,
						Size:  1024,
					},
					Payload: randomPayload(1024),
				},
				{
					reqID:      9999,
					frameFlags: flagStream,
					Metadata: Metadata{
						Type:  108,
						Flags: 108,
						Size:  108,
					},
					Payload: randomPayload(108),
				},
			},
		},
	}

	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {

			pipe := &bytes.Buffer{}

			for _, msg := range tc.input {
				err := writeFrame(pipe, msg)
				assert.ErrorIs(t, err, tc.wantErr)
				if tc.wantErr != nil {
					return
				}
			}

			for i := range tc.input {

				got := &Message{}
				err := validateProtocol(pipe, got)
				assert.ErrorIs(t, err, tc.wantErr)
				if tc.wantErr != nil {
					return
				}

				err = readPayload(pipe, got)
				assert.ErrorIs(t, err, tc.wantErr)
				if tc.wantErr != nil {
					return
				}

				assert.Equal(t, tc.input[i], got)
			}
		})
	}
}

func TestStream(t *testing.T) {

	t.Run("ordinary", func(t *testing.T) {

		conn1, conn2 := net.Pipe()

		metadata := &Message{}
		peer1 := newStream(conn1, metadata, func() bool { return true })
		peer2 := newStream(conn2, metadata, func() bool { return true })

		t.Run("stream and verify checksums", func(t *testing.T) {

			buf := randomPayload(2 << 20 * 50)
			hash := crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli))

			errCh := make(chan error, 1)

			go func() {
				_, err := peer1.Write(buf)
				if err != nil {
					errCh <- err
				}
				err = peer1.WriteChecksum()
				errCh <- err
			}()

			readBuf := make([]byte, 2<<20*50)
			_, readErr := peer2.Read(readBuf)
			rHash := peer2.rHash.Sum32()
			verificationFailed := peer2.ValidateRead()

			require.NoError(t, <-errCh)
			require.NoError(t, readErr)
			require.NoError(t, verificationFailed)

			if rHash != hash {
				t.Errorf("hash mismatch")
			}

		})

		t.Run("multi streams", func(t *testing.T) {

			streams := []struct {
				ack *messages.Ack
				buf []byte
			}{
				{
					ack: &messages.Ack{
						Success: true,
						Data:    binary.BigEndian.AppendUint32(nil, 10),
					},
					buf: randomPayload(10),
				},
				{
					ack: &messages.Ack{
						Success: true,
						Data:    binary.BigEndian.AppendUint32(nil, 100),
					},
					buf: randomPayload(100),
				},
				{
					ack: &messages.Ack{
						Success: true,
						Data:    binary.BigEndian.AppendUint32(nil, 1000),
					},
					buf: randomPayload(1000),
				},
				{
					ack: &messages.Ack{
						Success: true,
						Data:    binary.BigEndian.AppendUint32(nil, 5000),
					},
					buf: randomPayload(5000),
				},
			}

			writeErr := make(chan error, 1)

			go func() {
				var err error
				for _, stream := range streams {

					err = peer1.WriteStreamMessage(&ShortMessage{
						Type:    uint8(messages.MessageType_ACK),
						Message: stream.ack,
					})
					if err != nil {
						peer1.Close()
						break
					}

					_, err = peer1.Write(stream.buf)
					if err != nil {
						peer1.Close()
						break
					}

					err = peer1.WriteChecksum()
					if err != nil {
						peer1.Close()
						break
					}
				}
				writeErr <- err
			}()


			var (
				readErr error
				msg     *Message
			)
			
			for i := range len(streams) {

				msg, readErr = peer2.ReadStreamMessage()
				if readErr != nil {
					peer2.Close()
					break
				}

				ack := &messages.Ack{}
				readErr = proto.Unmarshal(msg.Payload, ack)
				if readErr != nil {
					peer2.Close()
					break
				}

				if !ack.Success || ack.Data == nil {
					readErr = fmt.Errorf("corrupted ack")
					peer2.Close()
					break
				}

				length := binary.BigEndian.Uint32(ack.Data)

				buf := make([]byte, length)
				_, readErr = io.ReadFull(peer2, buf)
				if readErr != nil {
					peer2.Close()
					break
				}

				readErr = peer2.ValidateRead()
				if readErr != nil {
					peer2.Close()
					break
				}

				assert.True(t, proto.Equal(streams[i].ack, ack))
				assert.Equal(t, streams[i].buf, buf)
			}

			require.NoError(t, errors.Join(<-writeErr, readErr))
		})

		t.Run("read write short messages", func(t *testing.T) {

			ack := &messages.Ack{
				Success: true,
				Data:    randomPayload(5),
			}

			writeErr := make(chan error, 1)
			go func() {
				err := peer1.WriteStreamMessage(&ShortMessage{
					Type:    uint8(messages.MessageType_ACK),
					Message: ack,
				})
				writeErr <- err
			}()

			msg, readErr := peer2.ReadStreamMessage()
			if msg.Type != uint8(messages.MessageType_ACK) {
				t.Errorf("invalid message type received")
			}
			require.NoError(t, <-writeErr)
			require.NoError(t, readErr)

			res := &messages.Ack{}
			err := proto.Unmarshal(msg.Payload, res)
			require.NoError(t, err)

			assert.True(t, proto.Equal(ack, res))
		})
	})
}
