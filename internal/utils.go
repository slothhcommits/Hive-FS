package internal

import (
	"errors"
	"io"

	"github.com/PraveenC2002/gfs/internal/hive"
	"github.com/PraveenC2002/gfs/internal/messages"
	"google.golang.org/protobuf/proto"
)

type ChunkReader struct {
	src io.ReadSeekCloser
	io.Reader
	start int64
	size  uint32
}

func NewChunkReader(size uint32, r io.ReadSeekCloser) (*ChunkReader, error) {

	start, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	return &ChunkReader{
		src:    r,
		Reader: io.LimitReader(r, int64(size)),
		start:  start,
		size:   size,
	}, nil
}

func (cr *ChunkReader) Read(p []byte) (int, error) {
	return cr.Reader.Read(p)
}

func (cr *ChunkReader) Close() error {
	return cr.src.Close()
}

func (cr *ChunkReader) Reset() error {

	if _, err := cr.src.Seek(cr.start, io.SeekStart); err != nil {
		return err
	}

	cr.Reader = io.LimitReader(cr.src, int64(cr.size))
	return nil
}

func MakeErrMessage(code uint8, err error) *hive.Message {

	errPl, marshalErr := proto.Marshal(&messages.Error{
		Code:    messages.Error_Code(code),
		Message: err.Error(),
	})
	if marshalErr != nil {
		panic(marshalErr)
	}

	return &hive.Message{
		Payload: errPl,
		Metadata: hive.Metadata{
			Type: uint8(messages.Error_MESSAGE_TYPE_ERROR),
			Size: uint32(len(errPl)),
		},
	}
}

var ErrRequestTimeout = errors.New("request timed out")
var ErrUnavailable = errors.New("unavailable")
var ErrOpFailed = errors.New("operation failed")
var ErrProtoUnmarshal = errors.New("proto unmarshal error")
var ErrBusy = errors.New("busy")
var ErrRequestCancelled = errors.New("cancelled")