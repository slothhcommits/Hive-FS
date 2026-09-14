package client

import (
	"errors"
	"io"
	"time"

	"github.com/PraveenC2002/gfs/internal"
	"github.com/PraveenC2002/gfs/internal/hive"
	"google.golang.org/protobuf/proto"
)

type Data struct {
	Size uint32
	Pipe io.ReadSeekCloser
}

/*
	0 based chunk indexing
*/

const (
	chunkSize uint32 = 1024 * 1024 * 64 // 64mb
)

const (
	masterAddr     string = ""
	requestTimeout        = 30 * time.Second
)

var transport = hive.NewTCPTransport()

func decode() {

}

func request(addr string, md *hive.Metadata, payload proto.Message, timeout time.Duration) (*hive.Message, error) {

	buf, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	md.Size = uint32(len(buf))

	msg := &hive.Message{
		Metadata: *md,
		Payload:  buf,
	}

	peer, err := transport.Dial(masterAddr)
	if err != nil {
		return nil, err
	}

	ch, err := peer.Request(msg)
	if err != nil {
		return nil, err
	}

	select {
	case res := <-ch:
		return res, nil
	case <-time.After(timeout):
		peer.Unregister(msg)
		return nil, errors.New("request timeout")
	}
}

func Write(path string, offset uint32, d *Data) error {

	chunkIdx := offset / chunkSize
	cursor := offset % chunkSize

	n := d.Size

	for n > 0 {

		sz := min(n, chunkSize-cursor)
		cr, err := internal.NewChunkReader(sz, d.Pipe)
		if err != nil {
			return err
		}

		err = writeChunk(path, chunkIdx, cursor, cr)
		if err != nil {
			return err
		}

		chunkIdx++
		n -= sz
		cursor = 0
	}

	return nil
}

func writeChunk(path string, chunkIdx uint32, cursor uint32, r *internal.ChunkReader) error {

	// res, err := request(
	// 	masterAddr,
	// 	&hive.Metadata{Type: },
	// 	&messages.Client_GetChunkReplicas{
	// 		Path:     path,
	// 		ChunkIdx: chunkIdx,
	// 	},
	// 	requestTimeout,
	// )
	// if err != nil {
	// 	return err
	// }

	// var data proto.Message
	// if err := proto.Unmarshal(res.Payload, data); err != nil {
	// 	return err
	// }

	return nil
}
