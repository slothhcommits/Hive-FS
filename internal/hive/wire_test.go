package hive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// readFrame mirrors what the transport read loops do: validate the fixed
// prefix, then pull the rest of the header plus the payload.
func readFrame(r io.Reader) (*Message, error) {

	rc := io.NopCloser(r)
	msg := &Message{}

	if err := validateProtocol(rc, msg); err != nil {
		return nil, err
	}

	if err := readPayload(rc, msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func TestMakeHeader(t *testing.T) {

	t.Run("golden layout", func(t *testing.T) {

		got := makeHeader(&Message{
			reqID:      42,
			frameFlags: flagZero,
			Metadata: Metadata{
				Type:  7,
				Flags: 3,
				Size:  4,
			},
		})

		want := []byte{
			'H', 'V', 'F', 'S', // magic
			1,           // version
			0,           // frame flags
			0, 0, 0, 42, // request id, big endian
			3,          // msg flags
			7,          // type
			0, 0, 0, 4, // length, big endian
		}

		if !bytes.Equal(got, want) {
			t.Fatalf("header layout changed\n got: % x\nwant: % x", got, want)
		}
	})

	t.Run("size", func(t *testing.T) {
		if got := len(makeHeader(&Message{})); got != int(headerSize) {
			t.Fatalf("header is %d bytes, want %d", got, headerSize)
		}
	})

	t.Run("stream flag", func(t *testing.T) {
		got := makeHeader(&Message{frameFlags: flagStream})
		if got[offFrameFlags] != flagStream {
			t.Fatalf("frame flags = %d, want %d", got[offFrameFlags], flagStream)
		}
	})
}

func TestFrameRoundTrip(t *testing.T) {

	cases := []struct {
		name string
		msg  *Message
	}{
		{
			name: "typical",
			msg: &Message{
				reqID:    99,
				Metadata: Metadata{Type: 12, Flags: 1, Size: 5},
				Payload:  []byte("hello"),
			},
		},
		{
			name: "empty payload",
			msg: &Message{
				reqID:    7,
				Metadata: Metadata{Type: 3, Size: 0},
				Payload:  []byte{},
			},
		},
		{
			name: "no reply request id",
			msg: &Message{
				reqID:    noReplyReqID,
				Metadata: Metadata{Type: 1, Size: 2},
				Payload:  []byte{0xff, 0x00},
			},
		},
		{
			name: "max payload",
			msg: &Message{
				reqID:    1,
				Metadata: Metadata{Type: 2, Size: maxPayloadSize},
				Payload:  bytes.Repeat([]byte{0xab}, int(maxPayloadSize)),
			},
		},
		{
			name: "binary payload with magic inside",
			msg: &Message{
				reqID:    1234,
				Metadata: Metadata{Type: 9, Size: 8},
				Payload:  []byte("HVFSHVFS"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			var buf bytes.Buffer

			if err := writeFrame(&buf, tc.msg); err != nil {
				t.Fatalf("writeFrame: %v", err)
			}

			if got, want := buf.Len(), int(headerSize)+len(tc.msg.Payload); got != want {
				t.Fatalf("wrote %d bytes, want %d", got, want)
			}

			got, err := readFrame(&buf)
			if err != nil {
				t.Fatalf("readFrame: %v", err)
			}

			if got.reqID != tc.msg.reqID {
				t.Errorf("reqID = %d, want %d", got.reqID, tc.msg.reqID)
			}
			if got.frameFlags != tc.msg.frameFlags {
				t.Errorf("frameFlags = %d, want %d", got.frameFlags, tc.msg.frameFlags)
			}
			if got.Type != tc.msg.Type {
				t.Errorf("Type = %d, want %d", got.Type, tc.msg.Type)
			}
			if got.Flags != tc.msg.Flags {
				t.Errorf("Flags = %d, want %d", got.Flags, tc.msg.Flags)
			}
			if got.Size != tc.msg.Size {
				t.Errorf("Size = %d, want %d", got.Size, tc.msg.Size)
			}
			if !bytes.Equal(got.Payload, tc.msg.Payload) {
				t.Errorf("payload mismatch: got %d bytes, want %d", len(got.Payload), len(tc.msg.Payload))
			}

			if buf.Len() != 0 {
				t.Errorf("%d bytes left unread, frame boundary is wrong", buf.Len())
			}
		})
	}

	t.Run("back to back frames", func(t *testing.T) {

		var buf bytes.Buffer

		first := &Message{reqID: 1, Metadata: Metadata{Type: 1, Size: 3}, Payload: []byte("one")}
		second := &Message{reqID: 2, Metadata: Metadata{Type: 2, Size: 3}, Payload: []byte("two")}

		if err := writeFrame(&buf, first); err != nil {
			t.Fatalf("writeFrame first: %v", err)
		}
		if err := writeFrame(&buf, second); err != nil {
			t.Fatalf("writeFrame second: %v", err)
		}

		for i, want := range []*Message{first, second} {
			got, err := readFrame(&buf)
			if err != nil {
				t.Fatalf("frame %d: %v", i, err)
			}
			if got.reqID != want.reqID || !bytes.Equal(got.Payload, want.Payload) {
				t.Fatalf("frame %d = {%d, %q}, want {%d, %q}",
					i, got.reqID, got.Payload, want.reqID, want.Payload)
			}
		}
	})
}

func TestWriteFrame(t *testing.T) {

	t.Run("size disagrees with payload", func(t *testing.T) {

		var buf bytes.Buffer

		err := writeFrame(&buf, &Message{
			Metadata: Metadata{Size: 10},
			Payload:  []byte("short"),
		})

		if err == nil {
			t.Fatal("expected an error when Size does not match len(Payload)")
		}
		if buf.Len() != 0 {
			t.Errorf("wrote %d bytes despite the mismatch", buf.Len())
		}
	})

	t.Run("payload over max", func(t *testing.T) {

		var buf bytes.Buffer

		payload := bytes.Repeat([]byte{1}, int(maxPayloadSize)+1)
		err := writeFrame(&buf, &Message{
			Metadata: Metadata{Size: uint32(len(payload))},
			Payload:  payload,
		})

		var target *ErrPayloadTooLarge
		if !errors.As(err, &target) {
			t.Fatalf("err = %v, want *ErrPayloadTooLarge", err)
		}
		if buf.Len() != 0 {
			t.Errorf("wrote %d bytes despite the oversize payload", buf.Len())
		}
	})
}

func TestValidateProtocol(t *testing.T) {

	// a well formed header we can corrupt one field at a time
	valid := func() []byte {
		return makeHeader(&Message{
			reqID:      0xDEADBEEF,
			frameFlags: flagStream,
			Metadata:   Metadata{Type: 5, Flags: 2, Size: 0},
		})
	}

	t.Run("accepts a valid header", func(t *testing.T) {

		msg := &Message{}
		err := validateProtocol(io.NopCloser(bytes.NewReader(valid())), msg)
		if err != nil {
			t.Fatalf("validateProtocol: %v", err)
		}

		if msg.reqID != 0xDEADBEEF {
			t.Errorf("reqID = %#x, want %#x", msg.reqID, 0xDEADBEEF)
		}
		if msg.frameFlags != flagStream {
			t.Errorf("frameFlags = %d, want %d", msg.frameFlags, flagStream)
		}
	})

	t.Run("consumes only the fixed prefix", func(t *testing.T) {

		buf := bytes.NewReader(valid())
		if err := validateProtocol(io.NopCloser(buf), &Message{}); err != nil {
			t.Fatalf("validateProtocol: %v", err)
		}

		want := int(headerSize) - int(offReqID+reqIDSize)
		if buf.Len() != want {
			t.Fatalf("%d bytes left, want %d", buf.Len(), want)
		}
	})

	t.Run("bad magic", func(t *testing.T) {

		h := valid()
		h[0] = 'X'

		err := validateProtocol(io.NopCloser(bytes.NewReader(h)), &Message{})

		var target *ErrInvalidProtocol
		if !errors.As(err, &target) {
			t.Fatalf("err = %v, want *ErrInvalidProtocol", err)
		}
	})

	t.Run("unsupported version", func(t *testing.T) {

		h := valid()
		h[offVersion] = protocolVersion + 1

		err := validateProtocol(io.NopCloser(bytes.NewReader(h)), &Message{})

		var target *ErrUnsupportedVersion
		if !errors.As(err, &target) {
			t.Fatalf("err = %v, want *ErrUnsupportedVersion", err)
		}
	})

	t.Run("closed connection", func(t *testing.T) {

		err := validateProtocol(io.NopCloser(bytes.NewReader(nil)), &Message{})

		if !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want io.EOF", err)
		}
	})

	t.Run("truncated prefix", func(t *testing.T) {

		err := validateProtocol(io.NopCloser(bytes.NewReader(valid()[:5])), &Message{})

		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

func TestReadPayload(t *testing.T) {

	// tail returns the bytes readPayload expects, i.e. the header from
	// offMsgFlags onward followed by the payload.
	tail := func(msgFlags, msgType uint8, length uint32, payload []byte) []byte {
		buf := make([]byte, headerSize-offMsgFlags)
		buf[offMsgFlags-offMsgFlags] = msgFlags
		buf[offType-offMsgFlags] = msgType
		binary.BigEndian.PutUint32(buf[offLength-offMsgFlags:], length)
		return append(buf, payload...)
	}

	t.Run("reads the declared length", func(t *testing.T) {

		msg := &Message{}
		err := readPayload(io.NopCloser(bytes.NewReader(tail(1, 9, 3, []byte("abc")))), msg)
		if err != nil {
			t.Fatalf("readPayload: %v", err)
		}

		if msg.Flags != 1 || msg.Type != 9 || msg.Size != 3 {
			t.Errorf("got {Flags:%d Type:%d Size:%d}, want {1 9 3}", msg.Flags, msg.Type, msg.Size)
		}
		if string(msg.Payload) != "abc" {
			t.Errorf("payload = %q, want %q", msg.Payload, "abc")
		}
	})

	t.Run("stops at the declared length", func(t *testing.T) {

		r := bytes.NewReader(tail(0, 1, 3, []byte("abcXYZ")))

		msg := &Message{}
		if err := readPayload(io.NopCloser(r), msg); err != nil {
			t.Fatalf("readPayload: %v", err)
		}

		if string(msg.Payload) != "abc" {
			t.Fatalf("payload = %q, want %q", msg.Payload, "abc")
		}
		if r.Len() != 3 {
			t.Fatalf("%d bytes left, want 3 — payload read past its length", r.Len())
		}
	})

	t.Run("zero length", func(t *testing.T) {

		msg := &Message{}
		if err := readPayload(io.NopCloser(bytes.NewReader(tail(0, 4, 0, nil))), msg); err != nil {
			t.Fatalf("readPayload: %v", err)
		}

		if len(msg.Payload) != 0 {
			t.Errorf("payload = %q, want empty", msg.Payload)
		}
		if msg.Size != 0 {
			t.Errorf("Size = %d, want 0", msg.Size)
		}
	})

	t.Run("declared length over max", func(t *testing.T) {

		// no payload follows: the guard must fire before any allocation
		err := readPayload(io.NopCloser(bytes.NewReader(tail(0, 1, maxPayloadSize+1, nil))), &Message{})

		var target *ErrPayloadTooLarge
		if !errors.As(err, &target) {
			t.Fatalf("err = %v, want *ErrPayloadTooLarge", err)
		}
	})

	t.Run("length at max is allowed", func(t *testing.T) {

		payload := bytes.Repeat([]byte{7}, int(maxPayloadSize))

		msg := &Message{}
		err := readPayload(io.NopCloser(bytes.NewReader(tail(0, 1, maxPayloadSize, payload))), msg)
		if err != nil {
			t.Fatalf("readPayload: %v", err)
		}

		if uint32(len(msg.Payload)) != maxPayloadSize {
			t.Fatalf("read %d bytes, want %d", len(msg.Payload), maxPayloadSize)
		}
	})

	t.Run("truncated header tail", func(t *testing.T) {

		err := readPayload(io.NopCloser(bytes.NewReader([]byte{0, 1, 0})), &Message{})

		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("payload shorter than declared", func(t *testing.T) {

		err := readPayload(io.NopCloser(bytes.NewReader(tail(0, 1, 16, []byte("short")))), &Message{})

		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}
