package master

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/slothhcommits/Hive-FS/internal"
	"github.com/slothhcommits/Hive-FS/internal/hive"
	"github.com/slothhcommits/Hive-FS/internal/messages"
	"google.golang.org/protobuf/proto"
)

const (
	versionAckTimeout     time.Duration = 15 * time.Second
	grantLeaseTimeout     time.Duration = 15 * time.Second
	copyAckTimeout        time.Duration = 15 * time.Second
	gcInterval            time.Duration = 15 * time.Second
	nodeExpiryWindow      time.Duration = 30 * time.Second
	reReplicationInterval time.Duration = 30 * time.Second
)

type MasterOpts struct {
	IsShadow    bool
	ListenAddr  string
	ShadowAddrs []string
	HivePath    string // defaults to ~/.hiveFS
}

type master struct {
	*MasterOpts

	transport hive.Transport

	*persistentState

	csr *chunkServerRegistry

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeCh   chan struct{}
}

func NewMaster(opts *MasterOpts) *master {

	if opts.HivePath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		opts.HivePath = filepath.Join(home, ".hiveFS")
	}

	return &master{

		MasterOpts: opts,

		transport: hive.NewTCPTransport(),

		persistentState: newPersistentState(opts.HivePath),

		csr: newChunkServerRegistry(),

		closeCh: make(chan struct{}),
	}
}

func (master *master) Boot(ctx context.Context) error {

	master.logger.open()

	// replay last checkpoint
	master.buildState()

	if !master.IsShadow {
		master.wg.Add(2)
		go master.garbageCollector()
		go master.reReplicate()
	}

	master.wg.Add(1)
	go master.handleRequests(ctx)

	go func() {
		<-ctx.Done()
		master.Close()
	}()

	// ListenAndAccept blocks until the transport is shut down
	err := master.transport.ListenAndAccept(master.ListenAddr)

	// also covers a listen failure, where nothing else would tear the loops down
	master.Close()
	master.wg.Wait()

	return err
}

func (master *master) Close() error {

	master.closeOnce.Do(func() {
		close(master.closeCh)
		master.transport.Close()
	})

	return nil
}

func (master *master) handleRequests(ctx context.Context) {

	defer master.wg.Done()

	reqCh := master.transport.Consume()
	for {
		select {
		case req, ok := <-reqCh:
			if !ok {
				return
			}
			master.wg.Add(1)
			go func() {
				defer master.wg.Done()
				master.requestRouter(req)
			}()
		case <-ctx.Done():
			return
		}
	}
}

// uninitiated requests
func (master *master) requestRouter(req hive.Request) {

	switch req.Message().Type {

	// client
	case uint8(messages.Client_MESSAGE_TYPE_READ_FILE):
		req.Respond(master.readFileHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_CREATE_FILE):
		req.Respond(master.createFileHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_CHUNK_REPLICAS):
		req.Respond(master.writeHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_SNAPSHOT):
		req.Respond(master.snapshotHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_MOVE):
		req.Respond(master.moveHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_LIST):
		req.Respond(master.listHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_DELETE):
		req.Respond(master.deleteHandler(req.Message()))

	// chunkserver
	case uint8(messages.Chunkserver_MESSAGE_TYPE_HEARTBEAT):
		req.Respond(master.heartbeatHandler(req.Message()))
	}

}

func (master *master) sendRequest(ctx context.Context, addr string, msg *hive.Message) (*hive.Message, error) {

	var peer hive.Peer
	var ch <-chan *hive.Message
	var err error

	// a failed write evicts the peer, so the one retry dials a fresh connection
	for attempt := 0; attempt < 2; attempt++ {

		peer, err = master.transport.Dial(ctx, addr)
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

func (master *master) readFileHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_Read{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := newTransaction()

	file, err := master.namespace.lookup(tx, req.Path)
	defer master.persistentState.commitTransaction(tx)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}
	file.RLock()
	defer file.RUnlock()

	if req.Idx >= uint32(len(file.chunkHandles)) {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), fmt.Errorf("index out of bounds"))
	}

	chunks := []*chunk{}

	master.chunkTable.mu.RLock()
	for _, handle := range file.chunkHandles[int(req.Idx):min(int(req.Idx)+int(req.Count), len(file.chunkHandles))] {
		chunk, ok := master.chunkTable.entries[handle]
		if ok {
			chunks = append(chunks, chunk)
		}
	}
	master.chunkTable.mu.RUnlock()

	res := &messages.Master_File{}
	for _, chunk := range chunks {
		chunk.Lock()
		res.Chunks = append(res.Chunks, &messages.Master_Chunk{
			ChunkId:   chunk.id,
			Version:   chunk.version,
			Locations: chunk.getLocations(nil),
		})
		chunk.Unlock()
	}

	payload, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_FILE),
			Size: uint32(len(payload)),
		},
	}
}

func (master *master) createFileHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_CreateFile{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := newTransaction()
	if err := master.namespace.create(tx, req.Path); err != nil {
		tx.rollback()
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}
	master.persistentState.commitTransaction(tx)

	payload, err := proto.Marshal(&messages.Ack{
		Success: true,
	})
	if err != nil {
		panic(err)
	}

	res := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.MessageType_ACK),
			Size: uint32(len(payload)),
		},
	}

	return res
}

func (master *master) addNewChunk(path string, file *node) error {

	tx := newTransaction()

	chunk := newChunk(master.newChunkHandle())

	master.chunkTable.stagedCreateMu.Lock()
	master.chunkTable.stagedCreate[chunk.id] = struct{}{}
	master.chunkTable.stagedCreateMu.Unlock()
	defer func() {
		master.chunkTable.stagedCreateMu.Lock()
		delete(master.chunkTable.stagedCreate, chunk.id)
		master.chunkTable.stagedCreateMu.Unlock()
	}()

	servers, err := master.csr.getServers(chunk.repFactor, []string{})
	if err != nil {
		return err
	}

	for _, server := range servers {
		chunk.locations[server] = time.Time{}
	}

	err = master.grantLease(chunk)
	if err != nil {
		return err
	}

	chunk.version++
	chunk.ref++
	file.appendChunkHandle(tx, path, chunk.id)
	master.chunkTable.addChunk(tx, chunk)

	master.persistentState.commitTransaction(tx)

	return nil
}

func (master *master) copyOnWrite(oldChunk *chunk) (*chunk, error) {

	newChunk := newChunk(master.newChunkHandle())
	newChunk.version = oldChunk.version
	newChunk.ref = 1
	newChunk.repFactor = oldChunk.repFactor

	req := &messages.Master_LocalCopy{
		SrcId: oldChunk.id,
		DstId: newChunk.id,
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	master.chunkTable.stagedCreateMu.Lock()
	master.chunkTable.stagedCreate[newChunk.id] = struct{}{}
	master.chunkTable.stagedCreateMu.Unlock()

	wg := sync.WaitGroup{}

	for _, loc := range oldChunk.getLocations(nil) {

		wg.Add(1)
		go func(loc string) {
			defer wg.Done()
			msg := hive.Message{
				Payload: payload,
				Metadata: hive.Metadata{
					Type: uint8(messages.Master_MESSAGE_TYPE_LOCAL_COPY_CHUNK),
					Size: uint32(len(payload)),
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), copyAckTimeout)
			defer cancel()

			ack, err := master.sendRequest(ctx, loc, &msg)
			if err == nil {

				res := &messages.Ack{}
				err := proto.Unmarshal(ack.Payload, res)

				if err == nil && res.Success {
					newChunk.Lock()
					newChunk.locations[loc] = time.Now()
					newChunk.Unlock()
				}
			}
		}(loc)
	}

	wg.Wait()

	if len(newChunk.locations) == 0 {
		master.chunkTable.stagedCreateMu.Lock()
		delete(master.chunkTable.stagedCreate, newChunk.id)
		master.chunkTable.stagedCreateMu.Unlock()
		return nil, internal.ErrUnavailable
	}
	return newChunk, nil
}

func (master *master) setWriteResponse(chunk *chunk) *hive.Message {

	sendReplicas := &messages.Master_Chunk{
		ChunkId:     chunk.id,
		Version:     chunk.version,
		Primary:     chunk.primary,
		LeaseExpiry: chunk.leaseGrantTime.Add(leaseDeadline).Unix(),
		Locations:   chunk.getLocations(nil),
	}

	payload, err := proto.Marshal(sendReplicas)
	if err != nil {
		panic(err)
	}

	res := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_CHUNK_REPLICAS),
			Size: uint32(len(payload)),
		},
	}

	return res
}

func (master *master) writeHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_GetChunkReplicas{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	unlockNamespace := newTransaction()

	file, err := master.namespace.lookup(unlockNamespace, req.Path)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}
	defer master.persistentState.commitTransaction(unlockNamespace)
	file.Lock()
	defer file.Unlock()

	if req.LastChunk {
		if len(file.chunkHandles) == 0 {
			req.ChunkIdx = 0
		} else {
			req.ChunkIdx = uint32(len(file.chunkHandles) - 1)
		}
	}

	// sparse write, length exceeded
	if uint32(len(file.chunkHandles)) < req.ChunkIdx {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), fmt.Errorf("length exceeded"))
	}

	// add new chunk
	if uint32(len(file.chunkHandles)) == req.ChunkIdx {

		if err := master.addNewChunk(req.Path, file); err != nil {
			return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}

		master.chunkTable.mu.RLock()
		chunk := master.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
		master.chunkTable.mu.RUnlock()

		chunk.Lock()
		defer chunk.Unlock()
		return master.setWriteResponse(chunk)
	}

	master.chunkTable.mu.RLock()
	chunk := master.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
	master.chunkTable.mu.RUnlock()

	chunk.Lock()
	defer func() {
		chunk.Unlock()
	}()

	// COW
	if chunk.ref > 1 {
		newChunk, err := master.copyOnWrite(chunk)
		if err != nil {
			return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}

		defer func() {
			master.chunkTable.stagedCreateMu.Lock()
			delete(master.chunkTable.stagedCreate, newChunk.id)
			master.chunkTable.stagedCreateMu.Unlock()
		}()

		if err := master.grantLease(newChunk); err != nil {
			return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}

		newChunk.version++
		tx := newTransaction()
		chunk.updateRef(tx, chunk.ref-1)
		master.chunkTable.addChunk(tx, newChunk)
		file.metadata.replaceChunkHandle(tx, req.Path, int(req.ChunkIdx), chunk.id, newChunk.id)
		master.persistentState.commitTransaction(tx)

		chunk.Unlock()

		chunk = newChunk
		chunk.Lock()

	} else if time.Since(chunk.leaseGrantTime) > leaseDeadline { // lease expired
		err := master.grantLease(chunk)
		if err != nil {
			return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}
		tx := newTransaction()
		chunk.updateVersion(tx, chunk.version+1)
		master.persistentState.commitTransaction(tx)
	}

	return master.setWriteResponse(chunk)
}

func (master *master) snapshotHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_Snapshot{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := newTransaction()

	nodes, err := master.namespace.snapshot(tx, req.Src, req.Dst)
	if err != nil {
		tx.rollback()
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}

	primaryChunkMap := make(map[string][]uint64)

	chunkLocks := []*sync.Mutex{}
	chunkSet := make(map[uint64]uint32)

	for _, node := range nodes {
		if !node.dir {
			for _, handle := range node.chunkHandles {
				chunkSet[handle]++
			}
		}
	}

	master.chunkTable.mu.RLock()

	chunks := []*chunk{}

	for handle, _ := range chunkSet {
		if c, ok := master.chunkTable.entries[handle]; ok {
			chunks = append(chunks, c)
		}
	}

	master.chunkTable.mu.RUnlock()

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].id < chunks[j].id
	})

	for _, chunk := range chunks {

		chunk.Lock()
		if time.Since(chunk.leaseGrantTime) < leaseDeadline {
			primaryChunkMap[chunk.primary] = append(primaryChunkMap[chunk.primary], chunk.id)
		}
		chunk.updateRef(tx, chunk.ref+chunkSet[chunk.id])
		chunkLocks = append(chunkLocks, &chunk.Mutex)
	}

	tx.addFunc(func() {
		for _, lock := range chunkLocks {
			lock.Unlock()
		}
	})

	wg := sync.WaitGroup{}

	for primary, handles := range primaryChunkMap {
		wg.Add(1)
		go func(primary string, handles []uint64) {
			defer wg.Done()
			master.revokeLease(primary, handles)
		}(primary, handles)
	}

	wg.Wait()

	master.persistentState.commitTransaction(tx)

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

func (master *master) updateVersion(chunk *chunk) (map[string]time.Time, error) {

	payload, err := proto.Marshal(&messages.Master_UpdateVersion{
		Id:      chunk.id,
		Version: chunk.version+1,
	})

	if err != nil {
		panic(fmt.Errorf("update marshal error : %s", err.Error()))
	}

	wg := sync.WaitGroup{}

	acked := make(map[string]time.Time)
	ackedCh := make(chan string)
	done := make(chan struct{})

	go func() {
		for server := range ackedCh {
			acked[server] = time.Now()
		}
		close(done)
	}()

	servers := chunk.getLocations(nil)
	
	for _, server := range servers {

		msg := &hive.Message{
			Metadata: hive.Metadata{
				Type: uint8(messages.Master_MESSAGE_TYPE_UPDATE_VERSION),
				Size: uint32(len(payload)),
			},
			Payload: payload,
		}

		wg.Add(1)
		go func(server string, msg *hive.Message) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), versionAckTimeout)
			defer cancel()

			ackMsg, err := master.sendRequest(ctx, server, msg)
			
			if err == nil && ackMsg.Type == uint8(messages.MessageType_ACK) {

				ack := &messages.Ack{}

				err = proto.Unmarshal(ackMsg.Payload, ack)

				if err == nil && ack.Success {
					ackedCh <- server
				}
			}
			
		}(server, msg)
	}

	wg.Wait()
	close(ackedCh)
	<-done

	if len(acked) == 0 {
		return nil, internal.ErrOpFailed
	}

	return acked, nil
}

func (master *master) grantLease(c *chunk) error {

	payload, err := proto.Marshal(&messages.Master_GrantLease{
		Id: c.id,
	})
	if err != nil {
		panic(fmt.Errorf("grant lease : %s", err.Error()))
	}

	// locations := c.getLocations(nil)

	acked, err := master.updateVersion(c)
	if err != nil {
		return err
	}

	// TODO: Think about what happens to acked chunks in chunkservers if lease grant fails
	var primary struct{ addr string ; time time.Time}
	for addr, _ := range acked {

		msg := &hive.Message{
			Payload: payload,
			Metadata: hive.Metadata{
				Type: uint8(messages.Master_MESSAGE_TYPE_TAKE_LEASE),
				Size: uint32(len(payload)),
			},
		}
		
		ctx, cancel := context.WithTimeout(context.Background(), grantLeaseTimeout)
		ackMsg, err := master.sendRequest(ctx, addr, msg)
		cancel()
		if err == nil && ackMsg.Type == uint8(messages.MessageType_ACK) {

			ack := &messages.Ack{}

			err = proto.Unmarshal(ackMsg.Payload, ack)

			if err == nil && ack.Success {
				primary = struct{addr string; time time.Time}{ addr: addr , time: time.Now() }
				break
			}

		}
	}

	
	if len(primary.addr) == 0 {
		return internal.ErrOpFailed
	}

	c.primary = primary.addr
	c.leaseGrantTime = primary.time

	for addr, _ := range c.locations {

		if _, ok := acked[addr]; !ok { 
			delete(c.locations, addr)
			continue
		}
		
		if addr == primary.addr {
			c.locations[primary.addr] = primary.time
			continue
		}

		c.locations[addr] = acked[addr]
	}

	return nil
}

func (master *master) revokeLease(primary string, handles []uint64) {

	req := &messages.Master_RevokeLease{
		Handles: handles,
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	msg := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_REVOKE_LEASE),
			Size: uint32(len(payload)),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), leaseDeadline)
	defer cancel()

	_, err = master.sendRequest(ctx, primary, msg)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		time.Sleep(leaseDeadline)
	}
}

func (master *master) moveHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_Move{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := newTransaction()
	err = master.namespace.move(tx, req.Src, req.Dst)
	if err != nil {
		tx.rollback()
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}

	master.persistentState.commitTransaction(tx)

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
			Size: uint32(len(payload)),
			Type: uint8(messages.MessageType_ACK),
		},
	}
}

func (master *master) listHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_List{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	contents, err := master.namespace.list(req.Path)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}

	res := &messages.Master_Subtree{
		Contents: contents,
	}

	payload, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Size: uint32(len(payload)),
			Type: uint8(messages.Master_MESSAGE_TYPE_SUBTREE),
		},
	}
}

func (master *master) deleteHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_Delete{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := newTransaction()
	err = master.namespace.delete(tx, req.Path)
	if err != nil {
		tx.rollback()
		return internal.MakeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}

	master.persistentState.commitTransaction(tx)

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

func (master *master) heartbeatHandler(msg *hive.Message) *hive.Message {

	hb := &messages.Chunkserver_Heartbeat{}
	err := proto.Unmarshal(msg.Payload, hb)
	if err != nil {
		return internal.MakeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	master.csr.update(hb)
	res := master.chunkTable.processHeartbeat(hb)

	payload, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Size: uint32(len(payload)),
			Type: uint8(messages.Master_MESSAGE_TYPE_HEARTBEAT_RES),
		},
	}
}

func (master *master) garbageCollector() {

	defer master.wg.Done()

	for {

		txLimit := 10
		opLimit := 1000

		opsLeft := txLimit * opLimit

		type entry struct {
			key       string
			node      *deletedNode
			length    int
			processed int
		}

		entries := []*entry{}
		handleSet := make(map[uint64]uint32)

		master.namespace.deletedMu.Lock()
		for key, deletedNode := range master.namespace.deleted {

			if opsLeft <= 0 {
				break
			}

			if time.Since(time.Unix(0, deletedNode.deletedAt)) > nodeExpiryWindow {

				master.chunkTable.mu.RLock()
				i := 0
				for _, handle := range deletedNode.chunkHandles {

					if opsLeft <= 0 {
						break
					}

					if _, ok := master.chunkTable.entries[handle]; ok {
						_, ok := handleSet[handle]
						if ok {
							handleSet[handle]++
						} else {
							handleSet[handle] = 1
							opsLeft--
						}
					}
					i++
				}
				master.chunkTable.mu.RUnlock()

				entries = append(entries, &entry{key: key, node: deletedNode, length: len(deletedNode.chunkHandles), processed: i})
				opsLeft--
			}
		}
		master.namespace.deletedMu.Unlock() // TODO: could have been RW lock

		tx := newTransaction()

		for _, entry := range entries {

			tx.addOperation(&operation{
				t:    DELETE,
				args: []string{entry.key, strconv.Itoa(entry.length), strconv.Itoa(entry.processed)},
			})

			tx.addFunc(func() {
				master.namespace.deletedMu.Lock()
				if entry.processed >= len(entry.node.chunkHandles) {
					delete(master.namespace.deleted, entry.key)
				} else {
					entry.node.Lock()
					entry.node.chunkHandles = entry.node.chunkHandles[entry.processed:]
					entry.node.Unlock()
				}
				master.namespace.deletedMu.Unlock()
			})
		}

		locks := []*sync.Mutex{}
		unlock := func() {
			for _, lock := range locks {
				lock.Unlock()
			}
		}

		handles := []uint64{}
		for handle, _ := range handleSet {
			handles = append(handles, handle)
		}

		slices.Sort(handles)

		for _, handle := range handles {
			master.chunkTable.mu.RLock()
			chunk, ok := master.chunkTable.entries[handle]
			master.chunkTable.mu.RUnlock()

			if ok {
				chunk.Lock()
				locks = append(locks, &chunk.Mutex)
				if chunk.ref >= handleSet[handle] {
					chunk.updateRef(tx, chunk.ref-handleSet[handle])
				} else {
					chunk.updateRef(tx, 0)
				}
			}
		}

		tx.addFunc(unlock)
		master.persistentState.commitTransaction(tx)

		opsLeft = txLimit * opLimit

		tx = newTransaction()
		locks = []*sync.Mutex{}

		master.chunkTable.mu.RLock()
		chunks := []*chunk{}
		for _, chunk := range master.chunkTable.entries {
			chunks = append(chunks, chunk)
		}
		master.chunkTable.mu.RUnlock()

		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].id < chunks[j].id
		})

		for _, chunk := range chunks {

			if opsLeft <= 0 {
				break
			}

			chunk.Lock()
			if chunk.ref == 0 {
				master.chunkTable.remove(tx, chunk.id)
				locks = append(locks, &chunk.Mutex)
				opsLeft--
			} else {
				chunk.Unlock()
			}
		}

		tx.addFunc(unlock)
		master.persistentState.commitTransaction(tx)

		select {
		case <-master.closeCh:
			return
		case <-time.After(gcInterval):

		}
	}
}

func (master *master) reReplicate() {

	defer master.wg.Done()

	for {

		master.csr.prune()

		master.chunkTable.repRequestsMu.Lock()
		for handle, reqs := range master.chunkTable.repRequests {
			for addr, issuedAt := range reqs {
				if time.Since(issuedAt) > repRequestExpiry {
					delete(reqs, addr)
				}
			}
			if len(reqs) == 0 {
				delete(master.chunkTable.repRequests, handle)
			}
		}
		master.chunkTable.repRequestsMu.Unlock()

		master.chunkTable.mu.RLock()
		chunks := []*chunk{}
		for _, chunk := range master.chunkTable.entries {
			chunks = append(chunks, chunk)
		}
		master.chunkTable.mu.RUnlock()

		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].id < chunks[j].id
		})

		under := []*chunkMetadata{}
		over := []*chunkMetadata{}

		for _, chunk := range chunks {

			chunk.Lock()
			if chunk.ref == 0 {
				chunk.Unlock()
				continue
			}

			rem := uint32(len(chunk.getLocations(nil)))
			if rem == chunk.repFactor {
				chunk.Unlock()
				continue
			}

			srcs := chunk.getLocations(nil)
			if len(srcs) == 0 {
				chunk.Unlock()
				continue
			}

			cm := &chunkMetadata{
				handle:    chunk.id,
				repFactor: chunk.repFactor,
				version:   chunk.version,
				primary:   chunk.primary,
				srcs:      srcs,
			}

			if rem > chunk.repFactor {
				over = append(over, cm)
			} else {
				under = append(under, cm)
			}

			chunk.Unlock()
		}

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() { defer wg.Done(); master.handleOverReplicated(over) }()
		go func() { defer wg.Done(); master.handleUnderReplicated(under) }()

		wg.Wait()

		select {
		case <-master.closeCh:
			return
		case <-time.After(reReplicationInterval):
		}
	}
}

func (master *master) handleOverReplicated(candidates []*chunkMetadata) {

	wg := &sync.WaitGroup{}
	for _, cand := range candidates {
		n := uint32(len(cand.srcs)) - cand.repFactor

		var addrs []string
		for _, addr := range cand.srcs {
			if uint32(len(addrs)) == n {
				break
			}
			if addr != cand.primary {
				addrs = append(addrs, addr)
			}
		}

		for _, addr := range addrs {

			wg.Add(1)
			go func(o *chunkMetadata, addr string) {
				defer wg.Done()
				req := &messages.Master_DeleteChunk{
					Handle: o.handle,
				}

				payload, err := proto.Marshal(req)
				if err != nil {
					panic(err)
				}

				msg := &hive.Message{
					Payload: payload,
					Metadata: hive.Metadata{
						Size: uint32(len(payload)),
						Type: uint8(messages.Master_MESSAGE_TYPE_DELETE_CHUNK),
					},
				}

				master.sendRequest(nil, addr, msg)

				master.chunkTable.mu.RLock()
				chunk, ok := master.chunkTable.entries[o.handle]
				master.chunkTable.mu.RUnlock()

				if ok {
					chunk.Lock()
					delete(chunk.locations, addr)
					chunk.Unlock()
				}
			}(cand, addr)
		}
	}

	wg.Wait()
}

func (master *master) handleUnderReplicated(candidates []*chunkMetadata) {

	wg := sync.WaitGroup{}

	sort.Slice(candidates, func(i, j int) bool {
		return len(candidates[i].srcs) < len(candidates[j].srcs)
	})

	intervalLimit := 20

	candidates = candidates[:min(len(candidates), intervalLimit)]

	for _, cand := range candidates {

		// servers already asked to pull this chunk, excluded from placement
		// so we don't ask the same one twice while its copy is in flight
		master.chunkTable.repRequestsMu.Lock()
		pending := slices.Collect(maps.Keys(master.chunkTable.repRequests[cand.handle]))
		master.chunkTable.repRequestsMu.Unlock()

		needed := int(cand.repFactor) - len(cand.srcs) - len(pending)
		if needed <= 0 {
			continue
		}

		servers, err := master.csr.getServers(uint32(needed), append(slices.Clone(cand.srcs), pending...))
		if err == nil {

			srcIdx := 0

			for _, server := range servers {
				wg.Add(1)
				go func(srcIdx int, server string) {
					defer wg.Done()
					req := &messages.Master_Replicate{
						Handle:  cand.handle,
						Version: cand.version,
						Src:     cand.srcs[srcIdx%len(cand.srcs)],
					}

					payload, err := proto.Marshal(req)
					if err != nil {
						panic(err)
					}

					msg := &hive.Message{
						Payload: payload,
						Metadata: hive.Metadata{
							Size: uint32(len(payload)),
							Type: uint8(messages.Master_MESSAGE_TYPE_REPLICATE_CHUNK),
						},
					}

					master.sendRequest(nil, server, msg)

					master.chunkTable.repRequestsMu.Lock()
					if _, ok := master.chunkTable.repRequests[cand.handle]; !ok {
						master.chunkTable.repRequests[cand.handle] = make(map[string]time.Time)
					}
					master.chunkTable.repRequests[cand.handle][server] = time.Now()
					master.chunkTable.repRequestsMu.Unlock()
				}(srcIdx, server)
				srcIdx++
			}
		}
	}
	wg.Wait()
}

func nsErrCode(err error) messages.Error_Code {
	switch {
	case errors.As(err, new(*ErrInvalidPath)), errors.As(err, new(*ErrIsFile)):
		return messages.Error_CODE_BAD_REQUEST
	case errors.As(err, new(*ErrObjectExist)):
		return messages.Error_CODE_EXISTS
	case errors.As(err, new(*ErrNotFound)):
		return messages.Error_CODE_NOT_FOUND
	default:
		return messages.Error_CODE_OP_FAILED
	}
}
