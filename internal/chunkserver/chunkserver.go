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

func (cs *chunkserver) scrub() {

	defer cs.wg.Done()

	for {

		cs.chunkSet.Lock()
		snap := maps.Clone(cs.chunkSet.entries)
		cs.chunkSet.Unlock()

		for _, chunk := range snap {

			if !chunk.mu.TryLock() {
				continue
			}

			if err := cs.verifyChunk(chunk); err != nil {
				if errors.Is(err, fs.ErrNotExist) || errors.Is(err, internal.ErrFileCorrupt) {
					cs.deletechunk(chunk.handle)
				}
			}

			chunk.mu.Unlock()

			select {
			case <-time.After(scrubThrottle):
			case <-cs.closeCh:
				return
			}
		}

		select {
		case <-time.After(scrubInterval):
		case <-cs.closeCh:
			return
		}
	}
}

func (cs *chunkserver) bufferSweepLoop() {

	defer cs.wg.Done()

	ticker := time.NewTicker(bufferSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-cs.closeCh:
			return
		}

		cs.bufferMap.Lock()
		for id, entry := range cs.bufferMap.entries {
			if !entry.pushedAt.IsZero() && time.Since(entry.pushedAt) > bufferTTL {
				delete(cs.bufferMap.entries, id)
			}
		}
		cs.bufferMap.Unlock()
	}
}

// master handlers

func (cs *chunkserver) heartbeatLoop() {

	defer cs.wg.Done()

	for {
		batch := []*messages.Chunkserver_ChunkInfo{}
		garbage := []*chunk{}

		cs.chunkSet.Lock()
		cs.versionMap.Lock()
		cs.leaseMap.Lock()

		for _, chunk := range cs.chunkSet.entries {
			handle := chunk.handle
			v, ok := cs.versionMap.entries[handle]
			if !ok {
				garbage = append(garbage, chunk)
				continue
			}
			_, ok = cs.leaseMap.entries[handle]
			batch = append(batch, &messages.Chunkserver_ChunkInfo{
				Id:          handle,
				Version:     v,
				ExtendLease: ok && time.Since(time.Unix(0, chunk.lastWrite.Load())) < heartbeatInterval,
			})
		}

		cs.leaseMap.Unlock()
		cs.versionMap.Unlock()
		cs.chunkSet.Unlock()

		for _, chunk := range garbage {
			cs.deletechunk(chunk.handle)
		}

		var st syscall.Statfs_t
		if err := syscall.Statfs(cs.StorePath, &st); err != nil {
			panic(err)
		}

		total := st.Blocks * uint64(st.Bsize)
		avail := st.Bavail * uint64(st.Bsize)
		util := uint32((total - avail) * 100 / total)

		heartbeat := &messages.Chunkserver_Heartbeat{
			Addr:     cs.ListenAddr,
			DiskUtil: uint32(util),
			Chunks:   batch,
		}

		payload, err := proto.Marshal(heartbeat)
		if err != nil {
			panic(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
		resMsg, err := cs.sendRequest(ctx, cs.MasterAddr, &hive.Message{
			Payload: payload,
			Metadata: hive.Metadata{
				Type: uint8(messages.Chunkserver_MESSAGE_TYPE_HEARTBEAT),
				Size: uint32(len(payload)),
			},
		})

		if err == nil {
			res := &messages.Master_HeartbeatRes{}
			err = proto.Unmarshal(resMsg.Payload, res)
			if err == nil {
				cs.heartbeatResponseHandler(res)
			}
		}

		cancel()

		select {
		case <-time.After(heartbeatInterval):
		case <-cs.closeCh:
			return
		}
	}
}

// a buffer whose commit never came, or failed, is never used again: a client
// retry pushes a fresh one. a buffer swept too early only costs a retry, the
// commit fails on the missing buffer

func (cs *chunkserver) heartbeatResponseHandler(res *messages.Master_HeartbeatRes) {

	cs.leaseMap.Lock()
	for _, handle := range res.RevokeLease {
		// be careful here
		delete(cs.leaseMap.entries, handle)
	}
	cs.leaseMap.Unlock()

	for _, handle := range res.Delete {
		cs.chunkSet.Lock()
		chunk, ok := cs.chunkSet.entries[handle]
		cs.chunkSet.Unlock()
		if ok {
			chunk.mu.Lock()
			cs.deletechunk(handle)
			chunk.mu.Unlock()
		}
	}

	for _, uv := range res.UpdateVersion {
		handle := uv.Id
		version := uv.Version
		err := cs.downdateChunkVersion(handle, version)
		if err != nil {
			cs.chunkSet.Lock()
			chunk, ok := cs.chunkSet.entries[handle]
			cs.chunkSet.Unlock()
			if ok {
				chunk.mu.Lock()
				cs.deletechunk(handle)
				chunk.mu.Unlock()
			}
		}
	}

	for _, handle := range res.ExtendLease {
		cs.chunkSet.Lock()
		chunk, ok := cs.chunkSet.entries[handle]
		cs.chunkSet.Unlock()
		if ok {
			cs.leaseMap.Lock()
			_, ok := cs.leaseMap.entries[handle]
			if ok {
				cs.leaseMap.entries[chunk.handle] = time.Now()
			}
			cs.leaseMap.Unlock()
		}
	}

}

func (cs *chunkserver) updateVersionHandler(msg *hive.Message) *hive.Message {

	req := &messages.Master_UpdateVersion{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	version := req.Version
	handle := req.Id

	cs.chunkSet.Lock()
	_, chunkOk := cs.chunkSet.entries[handle]
	cs.chunkSet.Unlock()

	cs.versionMap.Lock()
	_, versionOK := cs.versionMap.entries[handle]
	cs.versionMap.Unlock()

	if !chunkOk && !versionOK {
		if version != 1 {
			return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, internal.ErrBadRequest)
		}
		_, err = cs.mintNewChunk(handle)
		if err != nil {
			return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, internal.ErrOpFailed)
		}
	}

	if (!chunkOk && versionOK) || (chunkOk && !versionOK) {

		if chunkOk {
			cs.chunkSet.Lock()
			chunk, chunkOk := cs.chunkSet.entries[handle]
			cs.chunkSet.Unlock()
			if chunkOk {
				chunk.mu.Lock()
				cs.deletechunk(handle)
				chunk.mu.Unlock()
			}
		}

		if versionOK {
			cs.versionMap.Lock()
			delete(cs.versionMap.entries, handle)
			cs.versionMap.Unlock()
		}

		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, internal.ErrUnavailable)
	}

	err = cs.updateChunkVersion(handle, version)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	ack := &messages.Ack{
		Success: true,
	}

	payload, err := proto.Marshal(ack)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Size: uint32(len(payload)),
			Type: uint8(messages.MessageType_ACK),
		},
	}
}

func (cs *chunkserver) leaseGrantHandler(msg *hive.Message) *hive.Message {

	req := &messages.Master_GrantLease{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	cs.chunkSet.Lock()
	chunk, ok := cs.chunkSet.entries[req.Id]
	cs.chunkSet.Unlock()

	if !ok {
		return internal.MakeErrMessage(messages.Error_CODE_UNAVAILABLE, internal.ErrUnavailable)
	}

	if !chunk.mu.TryLock() {
		return internal.MakeErrMessage(messages.Error_CODE_CHUNKSERVER_BUSY, internal.ErrBusy)
	}
	defer chunk.mu.Unlock()

	cs.leaseMap.Lock()
	cs.leaseMap.entries[chunk.handle] = time.Now()
	cs.leaseMap.Unlock()

	ack := &messages.Ack{
		Success: true,
	}
	payload, err := proto.Marshal(ack)
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

func (cs *chunkserver) revokeLease(msg *hive.Message) *hive.Message {

	req := &messages.Master_RevokeLease{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	cs.leaseMap.Lock()
	for _, handle := range req.Handles {
		// be careful here
		delete(cs.leaseMap.entries, handle)
	}
	cs.leaseMap.Unlock()

	ack := &messages.Ack{
		Success: true,
	}

	payload, err := proto.Marshal(ack)
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

func (cs *chunkserver) localCopyHandler(msg *hive.Message) *hive.Message {

	req := &messages.Master_LocalCopy{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	srcId := req.SrcId
	dstId := req.DstId

	// check src and dst, if dst does not exist create one
	cs.chunkSet.Lock()

	src, ok := cs.chunkSet.entries[srcId]
	if !ok {
		cs.chunkSet.Unlock()
		return internal.MakeErrMessage(messages.Error_CODE_UNAVAILABLE, internal.ErrUnavailable)
	}

	if !src.mu.TryLock() {
		cs.chunkSet.Unlock()
		return internal.MakeErrMessage(messages.Error_CODE_CHUNKSERVER_BUSY, internal.ErrBusy)
	}
	defer src.mu.Unlock()

	_, ok = cs.chunkSet.entries[dstId]
	if ok {
		cs.chunkSet.Unlock()
		return internal.MakeErrMessage(messages.Error_CODE_EXISTS, fmt.Errorf("chunk exists"))
	}
	cs.chunkSet.Unlock()

	dst, err := cs.mintNewChunk(dstId)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	dst.mu.Lock()
	defer dst.mu.Unlock()

	// clean up on error
	defer func() {
		if err != nil {
			cs.deletechunk(dstId)
		}
	}()

	// open source file and get information
	srcChunkFile, err := cs.store.openFile(cs.store.makeChunkPath(srcId))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	defer srcChunkFile.Close()

	srcInfo, err := srcChunkFile.Stat()
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	err = src.checksums.verify(srcChunkFile, 0, uint32(srcInfo.Size()))
	if err != nil {
		if errors.Is(err, internal.ErrFileCorrupt) {
			cs.deletechunk(src.handle)
		}
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	_, err = srcChunkFile.Seek(0, io.SeekStart)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	// open dst files
	dstChecksumFile, err := cs.store.openFile(cs.store.makeChecksumPath(dstId))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	defer dstChecksumFile.Close()

	dstChunkFile, err := cs.store.openFile(cs.store.makeChunkPath(dstId))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	defer dstChunkFile.Close()

	// copy, update checksums, flush checksums, ack
	_, err = io.CopyN(dstChunkFile, srcChunkFile, srcInfo.Size())
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	err = dstChunkFile.Sync()
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	// inherit the source's checksums and verify the copy against them, so the
	// checksum proves the copy matches the source instead of describing itself
	dst.checksums.table = slices.Clone(src.table)

	err = dst.checksums.verify(dstChunkFile, 0, uint32(srcInfo.Size()))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	err = dst.checksums.flush(dstChecksumFile)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	ack := &messages.Ack{
		Success: true,
	}

	payload, err := proto.Marshal(ack)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Size: uint32(len(payload)),
			Type: uint8(messages.MessageType_ACK),
		},
	}
}

func (cs *chunkserver) replicationHandler(msg *hive.Message) *hive.Message {

	req := &messages.Master_Replicate{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	handle := req.Handle
	version := req.Version

	chunk, err := cs.mintNewChunk(handle)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	chunk.mu.Lock()
	defer chunk.mu.Unlock()

	defer func() {
		if err != nil {
			cs.deletechunk(handle)
		}
	}()

	metadata := &messages.Chunkserver_ChunkReplicationRequest{
		Handle:  handle,
		Version: version,
	}

	metaBuf, err := proto.Marshal(metadata)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), replicationRequestTimeout)
	defer cancel()

	stream, err := cs.transport.OpenStream(
		ctx,
		req.Src,
		&hive.Message{
			Payload: metaBuf,
			Metadata: hive.Metadata{
				Size: uint32(len(metaBuf)),
				Type: uint8(messages.Chunkserver_MESSAGE_TYPE_REPLICATE_CHUNK),
			},
		},
	)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	defer stream.Close()

	peerMsg, err := stream.ReadStreamMessage()
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	// the source answers with either the stream header or the reason it cannot serve
	if peerMsg.Type != uint8(messages.Chunkserver_MESSAGE_TYPE_CHUNK_REPLICATION_STREAM) {

		if peerMsg.Type != uint8(messages.MessageType_ACK) {
			err = internal.ErrOpFailed
			return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
		}

		peerAck := &messages.Ack{}
		err = proto.Unmarshal(peerMsg.Payload, peerAck)
		if err != nil {
			return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
		}

		if peerAck.Err == nil {
			err = internal.ErrBadRequest
			return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
		}

		err = fmt.Errorf("%s", peerAck.Err.Message)
		return internal.MakeErrMessage(peerAck.Err.Code, err)
	}

	streamMeta := &messages.Chunkserver_ChunkReplicationStream{}
	err = proto.Unmarshal(peerMsg.Payload, streamMeta)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	if streamMeta.Length > maxChunkSize {
		err = fmt.Errorf("stream length greater than max supported chunk size %w", internal.ErrBadRequest)
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	chunkFile, err := cs.store.openFile(cs.store.makeChunkPath(handle))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	defer chunkFile.Close()

	checksumFile, err := cs.store.openFile(cs.store.makeChecksumPath(handle))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}
	defer checksumFile.Close()

	_, err = io.CopyN(chunkFile, stream, int64(streamMeta.Length))
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	// the trailing crc covers everything that just came off the wire
	err = stream.ValidateRead()
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_CHUNKSERVER_STREAM_FAILED, err)
	}

	err = chunkFile.Sync()
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	err = chunk.update(chunkFile, 0, streamMeta.Length)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	err = chunk.checksums.flush(checksumFile)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	// update version
	chunk.mu.Unlock()
	err = cs.updateChunkVersion(handle, version)
	chunk.mu.Lock()
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, err)
	}

	ack := &messages.Ack{
		Success: true,
	}

	payload, err := proto.Marshal(ack)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Size: uint32(len(payload)),
			Type: uint8(messages.MessageType_ACK),
		},
	}
}

func (cs *chunkserver) deleteChunkHandler(msg *hive.Message) {

	req := &messages.Master_DeleteChunk{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return
	}

	handle := req.Handle

	cs.chunkSet.Lock()
	chunk, ok := cs.chunkSet.entries[handle]
	cs.chunkSet.Unlock()

	if ok {
		chunk.mu.Lock()
		defer chunk.mu.Unlock()
	}

	cs.deletechunk(handle)
}

// client handlers

func (cs *chunkserver) readHandler(stream *hive.Stream) {

	defer stream.Close()
	metadata := &messages.Client_ReadStreamMetadata{}

	err := proto.Unmarshal(stream.Metadata.Payload, metadata)
	if err != nil {
		cs.sendStreamError(stream, messages.Error_CODE_UNMARSHAL_FAILED, err)
		return
	}

	handle := metadata.ChunkId

	cs.chunkSet.Lock()
	chunk, ok := cs.chunkSet.entries[handle]
	cs.chunkSet.Unlock()
	if !ok {
		cs.sendStreamError(stream, messages.Error_CODE_UNAVAILABLE, internal.ErrUnavailable)
		return
	}

	chunk.mu.RLock()
	defer chunk.mu.RUnlock()

	cs.versionMap.Lock()
	if cs.versionMap.entries[chunk.handle] < metadata.Version {
		cs.versionMap.Unlock()
		cs.sendStreamError(stream, messages.Error_CODE_UNAVAILABLE, internal.ErrUnavailable)
		return
	}
	cs.versionMap.Unlock()

	f, err := cs.store.openFile(cs.makeChunkPath(handle))
	if err != nil {
		code := messages.Error_CODE_OP_FAILED
		if errors.Is(err, fs.ErrNotExist) {
			code = messages.Error_CODE_UNAVAILABLE
		}
		cs.sendStreamError(stream, code, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		cs.sendStreamError(stream, messages.Error_CODE_OP_FAILED, err)
		return
	}

	if metadata.Offset < 0 || metadata.Offset > fi.Size() {
		cs.sendStreamError(stream, messages.Error_CODE_BAD_REQUEST, internal.ErrBadRequest)
		return
	}

	metadata.Length = min(metadata.Length, uint32(fi.Size()-metadata.Offset))

	err = chunk.checksums.verify(f, metadata.Offset, metadata.Length)
	if err != nil {
		cs.sendStreamError(stream, messages.Error_CODE_OP_FAILED, err)
		return
	}

	_, err = f.Seek(metadata.Offset, io.SeekStart)
	if err != nil {
		cs.sendStreamError(stream, messages.Error_CODE_OP_FAILED, err)
		return
	}

	err = stream.WriteStreamMessage(&hive.ShortMessage{
		Type: uint8(messages.Chunkserver_MESSAGE_TYPE_READ_STREAM),
		Message: &messages.Chunkserver_ReadStream{
			Length: metadata.Length,
		},
	})
	if err != nil {
		return
	}

	_, err = io.CopyN(stream, f, int64(metadata.Length))
	if err != nil {
		return
	}

	err = stream.WriteChecksum()
	if err != nil {
		return
	}
}

func (cs *chunkserver) stream(ctx context.Context, peer *forwardStreamPeer) error {

	metadata, err := proto.Marshal(peer.metadata)
	if err != nil {
		panic(err)
	}

	stream, err := cs.transport.OpenStream(
		ctx,
		peer.addr,
		&hive.Message{
			Payload: metadata,
			Metadata: hive.Metadata{
				Type: uint8(messages.Chunkserver_MESSAGE_TYPE_DATA_STREAM),
				Size: uint32(len(metadata)),
			},
		},
	)
	if err != nil {
		return err
	}
	defer stream.Close()

	if peer.size > 0 {
		_, err = io.CopyN(stream, peer.src, int64(peer.size))
	} else {
		_, err = io.Copy(stream, peer.src)
	}

	if err != nil {
		return err
	}

	if err := stream.WriteChecksum(); err != nil {
		return err
	}

	inb, err := stream.ReadStreamMessage()
	if err != nil {
		return err
	}

	if inb != nil {
		if inb.Metadata.Type == uint8(messages.MessageType_ACK) {
			ack := &messages.Ack{}

			err = proto.Unmarshal(inb.Payload, ack)
			if err != nil {
				return err
			}
			if !ack.Success {
				if ack.Err != nil {
					err = fmt.Errorf("%s", ack.Err.Message)
				} else {
					err = internal.ErrOpFailed
				}
			}
		} else {
			err = fmt.Errorf("unknown message received")
		}
	}

	return err
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

func (cs *chunkserver) validateStream(stream *hive.Stream) *messages.Client_WriteStreamMetadata {

	metadata := &messages.Client_WriteStreamMetadata{}
	err := proto.Unmarshal(stream.Metadata.Payload, metadata)
	if err != nil {
		return nil
	}

	cs.chunkSet.Lock()
	_, ok := cs.chunkSet.entries[metadata.ChunkId]
	cs.chunkSet.Unlock()
	if !ok {
		cs.sendStreamError(
			stream,
			messages.Error_CODE_UNAVAILABLE,
			fmt.Errorf("chunk unavailable"),
		)
		return nil
	}

	if metadata.Size > maxChunkSize/4 {
		cs.sendStreamError(stream, messages.Error_CODE_BAD_REQUEST, internal.ErrBadRequest)
		return nil
	}

	if metadata.Offset == -1 {

		f, err := cs.store.openFile(cs.store.makeChunkPath(metadata.ChunkId))
		if err != nil {
			cs.sendStreamError(stream, messages.Error_CODE_OP_FAILED, internal.ErrOpFailed)
			return nil
		}

		fi, err := f.Stat()
		if err != nil {
			f.Close()
			cs.sendStreamError(stream, messages.Error_CODE_OP_FAILED, internal.ErrOpFailed)
			return nil
		}

		metadata.Offset = fi.Size()
		f.Close()
	}

	if metadata.Offset < 0 {
		cs.sendStreamError(stream, messages.Error_CODE_BAD_REQUEST, internal.ErrBadRequest)
		return nil
	}

	cs.bufferMap.Lock()
	if entry, ok := cs.bufferMap.entries[metadata.BufferId]; ok {
		cs.bufferMap.Unlock()
		// same id on the same chunk is a repeat of a push this server already
		// holds, same id on another chunk is two writers that picked one id
		if entry.chunkHandle == metadata.ChunkId {
			cs.sendStreamError(stream, messages.Error_CODE_EXISTS, internal.ErrExists)
		} else {
			cs.sendStreamError(
				stream,
				messages.Error_CODE_COLLISION,
				fmt.Errorf("buffer id collision"),
			)
		}
		return nil
	}
	if len(cs.bufferMap.entries) == concurrentWriteLimit {
		cs.bufferMap.Unlock()
		cs.sendStreamError(
			stream,
			messages.Error_CODE_CHUNKSERVER_BUSY,
			fmt.Errorf("busy"),
		)
		return nil
	}

	if metadata.Offset+int64(metadata.Size) > maxChunkSize {
		cs.bufferMap.Unlock()
		cs.sendStreamError(stream, messages.Error_CODE_OVERFLOW, internal.ErrBadRequest)
		return nil
	}

	buf := make([]byte, metadata.Size)
	cs.bufferMap.entries[metadata.BufferId] = &bufferEntry{
		chunkHandle: metadata.ChunkId,
		buf:         buf,
	}
	cs.bufferMap.Unlock()

	return metadata
}

func (cs *chunkserver) forwardStream(ctx context.Context, peer *forwardStreamPeer) error {

	pingCtx, cancelPing := context.WithTimeout(ctx, pingTimeout)
	defer cancelPing()

	ackedCh := make(chan string)
	for _, addr := range peer.metadata.ForwardList {
		go func(addr string) {
			if err := cs.ping(pingCtx, addr); err == nil {
				select {
				case ackedCh <- addr:
				case <-pingCtx.Done():
				}
			}
		}(addr)
	}

	var peerAddr string

	select {
	case peerAddr = <-ackedCh:
		cancelPing()
	case <-pingCtx.Done():
	}

	if len(peerAddr) == 0 {
		return internal.ErrOpFailed
	}

	peer.addr = peerAddr
	// stream

	if err := cs.stream(ctx, peer); err != nil && ctx.Err() == nil {
		return err
	}

	return nil
}

func (cs *chunkserver) streamHandler(stream *hive.Stream, isClient bool) {

	defer stream.Close()

	metadata := cs.validateStream(stream)
	if metadata == nil {
		return
	}

	var once sync.Once

	onError := func(err error) {
		once.Do(func() {
			cs.bufferMap.Lock()
			delete(cs.bufferMap.entries, metadata.BufferId)
			cs.bufferMap.Unlock()

			cs.sendStreamError(
				stream,
				messages.Error_CODE_CHUNKSERVER_STREAM_FAILED,
				err,
			)
		})
	}

	if isClient {
		cs.leaseMap.Lock()
		t, ok := cs.leaseMap.entries[metadata.ChunkId]
		expired := ok && time.Since(t) > leaseTimeout
		if expired {
			// an expired entry is inert, drop it here rather than leaving it
			// for a sweep that may never look at an idle chunk
			delete(cs.leaseMap.entries, metadata.ChunkId)
		}
		cs.leaseMap.Unlock()
		if !ok || expired {
			onError(fmt.Errorf("lease unavailable"))
			return
		}
	}

	for i, addr := range metadata.ForwardList {
		if addr == cs.ListenAddr {
			metadata.ForwardList = append(metadata.ForwardList[:i], metadata.ForwardList[i+1:]...)
			break
		}
	}

	cs.bufferMap.Lock()
	entry := cs.bufferMap.entries[metadata.BufferId]
	cs.bufferMap.Unlock()

	if len(metadata.ForwardList) != 0 {

		forwardCtx, cancelForward := context.WithCancel(context.Background())
		defer cancelForward()

		forwardReader, forwardWriter := io.Pipe()
		defer forwardWriter.Close()

		streamClient := &forwardStreamPeer{
			metadata: &messages.Client_WriteStreamMetadata{
				BufferId:    metadata.BufferId,
				ChunkId:     metadata.ChunkId,
				Offset:      metadata.Offset,
				Size:        metadata.Size,
				OriginAddr:  metadata.OriginAddr,
				PeerAddr:    cs.ListenAddr,
				ForwardList: metadata.ForwardList,
			},
			size: int64(metadata.Size),
			src:  forwardReader,
		}

		reader := io.TeeReader(stream, forwardWriter)
		done := make(chan error, 1)

		go func() {
			err := cs.forwardStream(forwardCtx, streamClient)
			if err != nil {
				stream.Close()
			}
			done <- err
		}()

		_, err := io.ReadFull(reader, entry.buf)
		if err != nil {
			cancelForward()
			forwardWriter.Close()
			err = cmp.Or(<-done, err)
			onError(err)
			return
		}

		err = <-done
		if err != nil {
			onError(err)
			return
		}

	} else {

		_, err := io.ReadFull(stream, entry.buf)
		if err != nil {
			onError(err)
			return
		}

	}

	if err := stream.ValidateRead(); err != nil {
		if len(metadata.ForwardList) != 0 {
			cs.streamErrorFanout(metadata.ForwardList, metadata.BufferId, metadata.ChunkId)
		}
		onError(err)
		return
	}

	// the ttl runs from here, a stream still filling buf is never swept
	cs.bufferMap.Lock()
	entry.pushedAt = time.Now()
	cs.bufferMap.Unlock()

	// the pusher is parked on its read lane waiting for this
	if err := cs.sendStreamAck(stream); err != nil {
		onError(err)
		return
	}

}

func (cs *chunkserver) streamErrorFanout(list []string, bufferId uint32, chunkId uint64) {

	wg := &sync.WaitGroup{}

	for _, member := range list {

		wg.Add(1)
		go func(member string) {

			defer wg.Done()

			req := &messages.Chunkserver_DropBuffer{
				BufferId: bufferId,
				ChunkId:  chunkId,
			}

			payload, err := proto.Marshal(req)
			if err != nil {
				panic(err)
			}

			cs.sendRequest(nil, member, &hive.Message{
				Payload: payload,
				Metadata: hive.Metadata{
					Type: uint8(messages.Chunkserver_MESSAGE_TYPE_DROP_BUFFER),
					Size: uint32(len(payload)),
				},
			})

		}(member)
	}
}

func (cs *chunkserver) sendStreamAck(stream *hive.Stream) error {

	return stream.WriteStreamMessage(&hive.ShortMessage{
		Type:    uint8(messages.MessageType_ACK),
		Message: &messages.Ack{Success: true},
	})
}

func (cs *chunkserver) sendStreamError(stream *hive.Stream, code messages.Error_Code, err error) error {
	return stream.WriteStreamMessage(internal.MakeShortErrMessage(code, err))
}

// chunkserver handlers

func (cs *chunkserver) pong(req *hive.Message) *hive.Message {

	msg := &messages.Ack{}
	err := proto.Unmarshal(req.Payload, msg)
	if err != nil {
		return internal.MakeErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err)
	}

	if !msg.Success {
		return internal.MakeErrMessage(messages.Error_CODE_OP_FAILED, errors.New("success false"))
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

func (cs *chunkserver) dropBufferHandler(msg *hive.Message) {

	req := &messages.Chunkserver_DropBuffer{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return
	}

	cs.bufferMap.Lock()
	entry, ok := cs.bufferMap.entries[req.BufferId]
	if ok && entry.chunkHandle == req.ChunkId {
		delete(cs.bufferMap.entries, req.BufferId)
	}
	cs.bufferMap.Unlock()
}

func (cs *chunkserver) serveChunkHandler(stream *hive.Stream) {

	defer stream.Close()

	metadata := &messages.Chunkserver_ChunkReplicationRequest{}

	err := proto.Unmarshal(stream.Metadata.Payload, metadata)
	if err != nil {
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_UNMARSHAL_FAILED, err))
		return
	}

	handle := metadata.Handle
	version := metadata.Version

	cs.chunkSet.Lock()
	chunk, ok := cs.chunkSet.entries[handle]
	cs.chunkSet.Unlock()
	if !ok {
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_UNAVAILABLE, internal.ErrUnavailable))
		return
	}

	if !chunk.mu.TryLock() {
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_CHUNKSERVER_BUSY, internal.ErrBusy))
		return
	}
	defer chunk.mu.Unlock()

	cs.versionMap.Lock()
	if v, ok := cs.versionMap.entries[handle]; !ok || v != version {
		cs.versionMap.Unlock()
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_OP_FAILED, internal.ErrBadRequest))
		return
	}
	cs.versionMap.Unlock()

	// open files
	chunkFile, err := cs.store.openFile(cs.store.makeChunkPath(handle))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			cs.deletechunk(handle)
		}
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_OP_FAILED, err))
		return
	}
	defer chunkFile.Close()

	chunkInfo, err := chunkFile.Stat()
	if err != nil {
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_OP_FAILED, err))
		return
	}

	// verify chunk
	err = chunk.checksums.verify(chunkFile, 0, uint32(chunkInfo.Size()))
	if err != nil {
		// maybe delete chunk
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_OP_FAILED, err))
		return
	}

	_, err = chunkFile.Seek(0, io.SeekStart)
	if err != nil {
		stream.WriteStreamMessage(internal.MakeShortErrMessage(messages.Error_CODE_OP_FAILED, err))
		return
	}

	err = stream.WriteStreamMessage(&hive.ShortMessage{
		Type: uint8(messages.Chunkserver_MESSAGE_TYPE_CHUNK_REPLICATION_STREAM),
		Message: &messages.Chunkserver_ChunkReplicationStream{
			Length: uint32(chunkInfo.Size()),
		},
	})
	if err != nil {
		return
	}

	_, err = io.CopyN(stream, chunkFile, chunkInfo.Size())
	if err != nil {
		return
	}
	stream.WriteChecksum()
}
