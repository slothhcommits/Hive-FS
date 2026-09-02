package chunkserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/PraveenC2002/gfs/internal"
	"github.com/PraveenC2002/gfs/internal/hive"
	"github.com/PraveenC2002/gfs/internal/messages"
	"google.golang.org/protobuf/proto"
)

const (
	concurrentWriteLimit = 64
	pingTimeout          = 2 * time.Second
)

type chunkServerOpts struct {
	listenAddr string
}

type chunkserver struct {
	*chunkServerOpts

	transport hive.Transport

	*bufferMap
	*chunkLeaseSet
	*chunkHandleSet

	*persistentState

	closeCh chan struct{}
}

func NewChunkServer() *chunkserver {
	return &chunkserver{
		transport: hive.NewTCPTransport(),
		bufferMap: &bufferMap{},
	}
}

func (cs *chunkserver) boot(ctx context.Context) error {

	go cs.handleRequests(ctx)

	go cs.handleStreams(ctx)

	go func() {
		<-ctx.Done()
		close(cs.closeCh)
		cs.transport.Close()
	}()

	return cs.listenAndAccept(ctx)
}

func (cs *chunkserver) listenAndAccept(ctx context.Context) error {

	err := cs.transport.ListenAndAccept(cs.listenAddr)
	if err != nil {
		return err
	}
	return nil
}

func (cs *chunkserver) handleRequests(ctx context.Context) {

	reqCh := cs.transport.Consume()

	for {
		select {

		case <-ctx.Done():
			return
		case req := <-reqCh:
			cs.requestRouter(req)
		}
	}
}

func (cs *chunkserver) handleStreams(ctx context.Context) {

	streamsCh := cs.transport.Streams()

	for {
		select {
		case <-ctx.Done():
			return
		case stream := <-streamsCh:
			cs.streamRouter(stream)
		}
	}
}

func (cs *chunkserver) streamRouter(stream *hive.Stream) {

	switch stream.Type {
	case uint8(messages.Client_MESSAGE_TYPE_DATA_STREAM):
		cs.clientStreamHandler(stream)
	case uint8(messages.Chunkserver_MESSAGE_TYPE_DATA_STREAM):
		cs.peerStreamHandler(stream)
	}

}

func (cs *chunkserver) requestRouter(req hive.Request) {
	switch req.Message().Type {

	case uint8(messages.Chunkserver_MESSAGE_TYPE_PING):
		req.Respond(cs.pong(req.Message()))
	}
}

func (cs *chunkserver) sendRequest(ctx context.Context, addr string, msg *hive.Message) (*hive.Message, error) {

	peer, err := cs.transport.Dial(addr)
	if err != nil {
		return nil, err
	}

	if ctx == nil {
		return nil, peer.Send(msg)
	}

	ch, err := peer.Request(msg)
	if err != nil {
		return nil, err
	}

	var res *hive.Message

	select {

	case <-ctx.Done():
		peer.Unregister(msg)
		return nil, nil

	case res = <-ch:
	}

	return res, nil
}

func (cs *chunkserver) ping(ctx context.Context, addr string) error {

	req := &messages.Ack{
		Success: true,
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	msg := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Chunkserver_MESSAGE_TYPE_PING),
			Size: uint32(len(payload)),
		},
	}
	
	res, err := cs.sendRequest(ctx, addr, msg)
	if err != nil {
		return err
	}

	if res.Type != uint8(messages.MessageType_ACK) {
		return internal.ErrOpFailed
	}

	resAck := &messages.Ack{}
	err = proto.Unmarshal(res.Payload, resAck)
	if err != nil {
		return err
	}

	if !resAck.Success {
		return internal.ErrOpFailed
	}

	return nil
}

func (cs *chunkserver) pong(req *hive.Message) *hive.Message {

	msg := &messages.Ack{}
	err := proto.Unmarshal(req.Payload, msg)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	if !msg.Success {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), errors.New("success false"))
	}

	res := &messages.Ack{
		Success: true,
	}

	payload, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.MessageType_ACK),
			Size: uint32(len(payload)),
		},
	}
}

func (cs *chunkserver) validateStream(stream *hive.Stream) *messages.Client_StreamHeader {

	// read the stream ID
	headerLenBuf := make([]byte, 4)

	_, err := io.ReadFull(stream.Pipe, headerLenBuf)
	if err != nil {
		return nil
	}

	headerLen := binary.BigEndian.Uint32(headerLenBuf)

	headerBuf := make([]byte, headerLen)
	_, err = io.ReadFull(stream.Pipe, headerBuf)
	if err != nil {
		return nil
	}

	header := &messages.Client_StreamHeader{}
	err = proto.Unmarshal(headerBuf, header)
	if err != nil {
		return nil
	}

	cs.chunkHandleSet.Lock()
	_, ok := cs.chunkHandleSet.entries[header.ChunkId]
	cs.chunkHandleSet.Unlock()
	if !ok {
		cs.SendStreamError(
			header,
			uint8(messages.Error_CODE_CHUNKSERVER_CHUNK_UNAVAILABLE),
			fmt.Errorf("chunk unavailable"),
		)
		return nil
	}

	cs.bufferMap.Lock()
	_, ok = cs.bufferMap.entries[header.SeqId]
	if ok || len(cs.bufferMap.entries) == concurrentWriteLimit {
		cs.bufferMap.Unlock()
		cs.SendStreamError(
			header,
			uint8(messages.Error_CODE_CHUNKSERVER_BUSY),
			err,
		)
		return nil
	}

	buf := make([]byte, stream.Size)
	cs.bufferMap.entries[header.SeqId] = buf
	cs.bufferMap.Unlock()

	return header
}

func (cs *chunkserver) forwardStream(ctx context.Context, header *messages.Client_StreamHeader, stream *hive.Stream) {

	for i, addr := range header.Locations {
		if addr == cs.listenAddr {
			header.Locations = append(header.Locations[:i], header.Locations[i+1:]...)
			break
		}
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, pingTimeout)

	ackedCh := make(chan string)
	for _, addr := range header.Locations {
		go func(addr string) {
			if err := cs.ping(pingCtx, addr); err == nil {
				select {
				case ackedCh <- addr:
				case <-pingCtx.Done():
				}
			}
		}(addr)
	}

	var peer string

	select {
	case peer = <-ackedCh:
		cancelPing()
	case <-pingCtx.Done():
	}

	if len(peer) == 0 {
		return
	}

	// stream

}

func (cs *chunkserver) clientStreamHandler(stream *hive.Stream) {

	header := cs.validateStream(stream)
	if header == nil {
		return
	}

	cs.chunkLeaseSet.Lock()
	_, ok := cs.chunkLeaseSet.entries[header.ChunkId]
	cs.chunkLeaseSet.Unlock()
	if !ok {
		cs.SendStreamError(
			header,
			uint8(messages.Error_CODE_CHUNKSERVER_LEASE_UNAVAILABLE),
			fmt.Errorf("lease unavailable"),
		)
		return
	}

	forwardCtx, cancelForward := context.WithCancel(context.Background())

	go func() {
		cs.forwardStream(forwardCtx, header, stream)
	}()

	// handle stream

}

func (cs *chunkserver) peerStreamHandler(stream *hive.Stream) {

	header := cs.validateStream(stream)
	if header == nil {
		return
	}

	forwardCtx, cancelForward := context.WithCancel(context.Background())

	go func() {
		cs.forwardStream(forwardCtx, header, stream)
	}()

}

func (cs *chunkserver) SendStreamError(header *messages.Client_StreamHeader, code uint8, err error) {

	res := &messages.Chunkserver_StreamFailed{
		SeqId:   header.SeqId,
		ChunkId: header.ChunkId,
		Error:   err.Error(),
	}

	payload, err := proto.Marshal(res)

	cs.sendRequest(
		context.Background(),
		header.ClientListenAddr,
		&hive.Message{
			Payload: payload,
			Metadata: hive.Metadata{
				Type: code,
				Size: uint32(len(payload)),
			},
		},
	)
}
