package chunkserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/slothhcommits/Hive-FS/internal"
	"github.com/slothhcommits/Hive-FS/internal/hive"
	"github.com/slothhcommits/Hive-FS/internal/messages"
	"google.golang.org/protobuf/proto"
)

const (
	concurrentWriteLimit = 64

	// timeouts
	pingTimeout               = 2 * time.Second
	leaseTimeout              = 60 * time.Second
	replicationRequestTimeout = 10 * time.Second
	sencondaryWriteTimeout    = 10 * time.Second
	heartbeatTimeout          = 4 * time.Second

	// intervals
	heartbeatInterval   = 10 * time.Second
	bufferTTL           = 60 * time.Second
	bufferSweepInterval = 5 * time.Second
	scrubInterval       = 30 * time.Second
	scrubThrottle       = 5 * time.Second
)

type ChunkServerOpts struct {
	ListenAddr string
	MasterAddr string
	StorePath  string // defaults to ~/.hiveStore
}

type chunkserver struct {
	*ChunkServerOpts

	*store

	transport hive.Transport

	*persistentState

	*bufferMap
	*leaseMap

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeCh   chan struct{}
}

func NewChunkServer(opts *ChunkServerOpts) *chunkserver {

	if opts.StorePath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		opts.StorePath = filepath.Join(home, ".hiveStore")
	}

	return &chunkserver{

		ChunkServerOpts: opts,

		store: newStore(opts.StorePath),

		transport: hive.NewTCPTransport(),

		bufferMap: &bufferMap{
			entries: make(map[uint32]*bufferEntry),
		},

		leaseMap: &leaseMap{
			entries: make(map[uint64]time.Time),
		},

		closeCh: make(chan struct{}),
	}
}

func (cs *chunkserver) Boot(ctx context.Context) error {

	// nothing may serve before the on disk state is known
	cs.persistentState = cs.store.buildState()

	cs.wg.Add(5)
	go cs.handleRequests(ctx)
	go cs.handleStreams(ctx)
	go cs.heartbeatLoop()
	go cs.bufferSweepLoop()
	go cs.scrub()

	go func() {
		<-ctx.Done()
		cs.Close()
	}()

	err := cs.listenAndAccept(ctx)

	// ListenAndAccept returns once the transport is down, which is also how a
	// listen failure surfaces, so tear down either way and let the loops drain
	cs.Close()
	cs.wg.Wait()

	return err
}

func (cs *chunkserver) Close() error {

	cs.closeOnce.Do(func() {
		close(cs.closeCh)
		cs.transport.Close()
	})

	return nil
}

func (cs *chunkserver) listenAndAccept(ctx context.Context) error {

	err := cs.transport.ListenAndAccept(cs.ListenAddr)
	if err != nil {
		return err
	}
	return nil
}

func (cs *chunkserver) handleRequests(ctx context.Context) {

	defer cs.wg.Done()

	reqCh := cs.transport.Consume()

	for {
		select {

		case <-ctx.Done():
			return
		case req, ok := <-reqCh:
			if !ok {
				return
			}
			cs.requestRouter(req)
		}
	}
}

func (cs *chunkserver) handleStreams(ctx context.Context) {

	defer cs.wg.Done()

	streamsCh := cs.transport.Streams()

	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-streamsCh:
			if !ok {
				return
			}
			cs.wg.Add(1)
			go func() {
				defer cs.wg.Done()
				// the transport hands the conn over and stops tracking it, and
				// clears its read deadline, so on shutdown nothing else can
				// unblock a router parked on a read. closing the stream here is
				// the only thing that will.
				done := make(chan struct{})
				go func() {
					select {
					case <-done:
					case <-cs.closeCh:
						stream.Close()
					}
				}()
				cs.streamRouter(stream)
				close(done)
			}()
		}
	}
}

func (cs *chunkserver) streamRouter(stream *hive.Stream) {

	switch stream.Metadata.Type {

	// client
	case uint8(messages.Client_MESSAGE_TYPE_READ_STREAM):
		cs.readHandler(stream)

	case uint8(messages.Client_MESSAGE_TYPE_WRITE_DATA_STREAM):
		cs.streamHandler(stream, true)

	// chunkserver
	case uint8(messages.Chunkserver_MESSAGE_TYPE_DATA_STREAM):
		cs.streamHandler(stream, false)

	case uint8(messages.Chunkserver_MESSAGE_TYPE_REPLICATE_CHUNK):
		cs.serveChunkHandler(stream)
	}

}

func (cs *chunkserver) requestRouter(req hive.Request) {

	switch req.Message().Type {

	// master

	case uint8(messages.Master_MESSAGE_TYPE_TAKE_LEASE):
		req.Respond(cs.leaseGrantHandler(req.Message()))

	case uint8(messages.Master_MESSAGE_TYPE_UPDATE_VERSION):
		req.Respond(cs.updateVersionHandler(req.Message()))

	case uint8(messages.Master_MESSAGE_TYPE_REVOKE_LEASE):
		req.Respond(cs.revokeLease(req.Message()))

	case uint8(messages.Master_MESSAGE_TYPE_LOCAL_COPY_CHUNK):
		req.Respond(cs.localCopyHandler(req.Message()))

	case uint8(messages.Master_MESSAGE_TYPE_REPLICATE_CHUNK):
		req.Respond(cs.replicationHandler(req.Message()))

	case uint8(messages.Master_MESSAGE_TYPE_DELETE_CHUNK):
		cs.deleteChunkHandler(req.Message())

	// client
	case uint8(messages.Client_MESSAGE_TYPE_COMMIT_WRITE):
		req.Respond(cs.primaryWriteHandler(req.Message()))

	// chunkserver
	case uint8(messages.Chunkserver_MESSAGE_TYPE_PING):
		req.Respond(cs.pong(req.Message()))

	case uint8(messages.Chunkserver_MESSAGE_TYPE_DROP_BUFFER):
		cs.dropBufferHandler(req.Message())

	case uint8(messages.Chunkserver_MESSAGE_TYPE_COMMIT_WRITE):
		req.Respond(cs.secondaryWriteHandler(req.Message()))

	}
}

func (cs *chunkserver) sendRequest(ctx context.Context, addr string, msg *hive.Message) (*hive.Message, error) {

	var peer hive.Peer
	var ch <-chan *hive.Message
	var err error

	// a failed write evicts the peer, so the one retry dials a fresh connection
	for attempt := 0; attempt < 2; attempt++ {

		peer, err = cs.transport.Dial(ctx, addr)
		if err != nil {
			return nil, err
		}

		if ctx == nil {
			err = peer.Send(nil, msg)
			if err == nil {
				return nil, nil
			}
			continue
		}

		// a request the caller already gave up on must not go out
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		ch, err = peer.Request(ctx, msg)
		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	var res *hive.Message

	select {

	case <-ctx.Done():
		peer.Unregister(msg)
		return nil, ctx.Err()

	case res = <-ch:
	}

	return res, nil
}

func (cs *chunkserver) mintNewChunk(handle uint64) (*chunk, error) {

	cs.chunkSet.Lock()
	_, ok := cs.chunkSet.entries[handle]
	cs.chunkSet.Unlock()

	if ok {
		return nil, internal.ErrExists
	}

	err := cs.store.makeNewChunkFiles(handle)
	if err != nil {
		return nil, err
	}

	checksums := newChecksums(handle)
	chunk := newChunk(handle, checksums)

	cs.chunkSet.Lock()

	_, ok = cs.chunkSet.entries[handle]

	if ok {
		cs.chunkSet.Unlock()
		cs.store.deleteChunkFiles(handle)
		return nil, internal.ErrExists
	}

	cs.chunkSet.entries[handle] = chunk

	cs.versionMap.Lock()
	cs.versionMap.entries[handle] = 0
	cs.versionMap.Unlock()

	cs.chunkSet.Unlock()

	return chunk, nil
}

func (cs *chunkserver) deletechunk(handle uint64) error {

	cs.chunkSet.Lock()
	delete(cs.chunkSet.entries, handle)
	cs.chunkSet.Unlock()

	cs.leaseMap.Lock()
	delete(cs.leaseMap.entries, handle)
	cs.leaseMap.Unlock()

	cs.versionMap.Lock()
	delete(cs.versionMap.entries, handle)
	cs.versionMap.Unlock()

	return cs.store.deleteChunkFiles(handle)
}

func (cs *chunkserver) verifyChunk(chunk *chunk) error {

	f, err := cs.store.openFile(cs.makeChunkPath(chunk.handle))
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	err = chunk.verify(f, 0, uint32(fi.Size()))

	return err
}

func (cs *chunkserver) updateChunkVersion(handle uint64, version uint32) error {

	cs.versionMap.Lock()
	defer cs.versionMap.Unlock()

	v, ok := cs.versionMap.entries[handle]
	if !ok || (ok && v >= version) {
		if !ok {
			return internal.ErrUnavailable
		}
		if v > version {
			return internal.ErrBadRequest
		}
		return nil
	}

	err := cs.store.logVersion(handle, version)
	if err != nil {
		return err
	}

	cs.versionMap.entries[handle] = version

	// every lease grant is preceded by a version bump on every replica, so
	// this is the one point where all replicas of a chunk are touched
	// together before a primary exists. zeroing ctr here gives every lease
	// a common origin with no message, no disk state, and no master
	// involvement. ctr's lifetime is the lease, not the process.
	cs.chunkSet.Lock()
	chunk, chunkOk := cs.chunkSet.entries[handle]
	cs.chunkSet.Unlock()
	if chunkOk {
		chunk.mu.Lock()
		chunk.ctr = 0
		chunk.mu.Unlock()
	}

	return nil
}

func (cs *chunkserver) downdateChunkVersion(handle uint64, version uint32) error {

	cs.versionMap.Lock()
	defer cs.versionMap.Unlock()

	v, ok := cs.versionMap.entries[handle]
	if !ok {
		return internal.ErrUnavailable
	}

	if v > version {
		err := cs.store.logVersion(handle, version)
		if err != nil {
			return err
		}
		cs.versionMap.entries[handle] = version
	}

	return nil
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
