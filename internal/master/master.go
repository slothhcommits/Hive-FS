package master

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PraveenC2002/gfs/internal/hive"
	"github.com/PraveenC2002/gfs/internal/messages"
	"google.golang.org/protobuf/proto"
)

const (
	versionAckTimeout time.Duration = 15 * time.Second
	grantLeaseTimeout time.Duration = 15 * time.Second
	copyAckTimeout    time.Duration = 15 * time.Second
)

type MasterOpts struct {
	isShadow    bool
	listenAddr  string
	shadowAddrs []string
}

type master struct {
	*MasterOpts

	transport hive.Transport

	*persistentState

	csr *chunkServerRegistry
}

func newMaster(opts *MasterOpts) *master {
	return &master{

		MasterOpts: opts,

		transport: hive.NewTCPTransport(),

		persistentState: newPersistentState(),

		csr: newChunkServerRegistry(),
	}
}

func (m *master) Boot(ctx context.Context) error {

	// replay last checkpoint
	if err := m.transport.ListenAndAccept(m.listenAddr); err != nil {
		return err
	}
	m.transport.Close()
	return nil
}

func (m *master) handleRequests(ctx context.Context) {

	reqCh := m.transport.Consume()
	for {
		select {
		case <-reqCh:
		case <-ctx.Done():
			return
		}
	}
}

// uninitiated requests
func (m *master) requestRouter(req hive.Request) {

	switch req.Message().Type {

	// client
	case uint8(messages.Client_MESSAGE_TYPE_CREATE_REQ):
		req.Respond(m.createHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_GET_CHUNK_REPLICAS_REQ):
		req.Respond(m.writeHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_SNAPSHOT_REQ):

	// chunkserver
	case uint8(messages.Chunkserver_MESSAGE_TYPE_HEARTBEAT):
		req.Respond(m.snapshotHandler(req.Message()))

	}

}

func (m *master) sendRequest(addr string, msg *hive.Message, timeout time.Duration) (*hive.Message, error) {

	peer, err := m.transport.Dial(addr)
	if err != nil {
		return nil, err
	}

	ch, err := peer.Request(msg)
	if err != nil {
		return nil, err
	}

	var res *hive.Message

	select {
	case res = <-ch:
	case <-time.After(timeout):
		peer.Unregister(msg)
		return nil, ErrRequestTimeout
	}

	return res, nil
}

func (m *master) createHandler(msg *hive.Message) *hive.Message {

	req := &messages.Create{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := &transaction{}
	if err := m.namespace.create(tx, req.Path); err != nil {
		tx.rollback()
		return makeErrMessage(uint8(messages.Error_CODE_CREATE_FILE_FAILED), err)
	}
	tx.commit()

	payload, err := proto.Marshal(&messages.CreateFileAck{
		Success: true,
	})
	if err != nil {
		panic(err)
	}

	res := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_CREATE_FILE_ACK),
			Size: uint32(len(payload)),
		},
	}

	return res
}

func (m *master) addNewChunk(path string, file *node) error {

	tx := &transaction{}

	chunk := newChunk()

	servers, err := m.csr.getServers(chunk.repFactor, []string{})
	if err != nil {
		return err
	}

	for _, server := range servers {
		chunk.locations[server] = time.Time{}
	}

	err = m.grantLease(chunk)
	if err != nil {
		return err
	}

	chunk.version++
	chunk.ref++
	file.appendChunkHandle(tx, path, chunk.id)
	m.chunkTable.addChunk(tx, chunk)

	tx.commit()

	return nil
}

func (m *master) setWriteResponse(chunk *chunk) *hive.Message {

	sendReplicas := &messages.SendChunkReplicas{
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
			Type: uint8(messages.Master_MESSAGE_TYPE_SEND_CHUNK_REPLICAS),
			Size: uint32(len(payload)),
		},
	}

	return res
}

func (m *master) writeHandler(msg *hive.Message) *hive.Message {

	req := &messages.GetChunkReplicas{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	unlockNamespace := &transaction{}

	file, err := m.namespace.lookup(unlockNamespace, req.Path)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_LOOKUP_FAILED), err)
	}
	defer unlockNamespace.commit()
	file.Lock()
	defer file.Unlock()

	// sparse write, length exceeded
	if uint32(len(file.chunkHandles)) < req.ChunkIdx {
		return makeErrMessage(uint8(messages.Error_CODE_LENGTH_EXCEEDED), fmt.Errorf("length exceeded"))
	}

	// add new chunk
	if uint32(len(file.chunkHandles)) == req.ChunkIdx {

		if err := m.addNewChunk(req.Path, file); err != nil {
			return makeErrMessage(uint8(messages.Error_CODE_CHUNKSERVERS_UNAVAILABLE), err)
		}

		m.chunkTable.mu.RLock()
		chunk := m.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
		m.chunkTable.mu.RUnlock()

		chunk.RLock()
		defer chunk.RUnlock()
		return m.setWriteResponse(chunk)
	}

	m.chunkTable.mu.RLock()
	chunk := m.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
	m.chunkTable.mu.RUnlock()

	chunk.Lock()
	defer func() {
		chunk.Unlock()
	}()

	// COW
	if chunk.ref > 1 {
		newChunk, err := m.copyOnWrite(chunk)
		if err != nil {
			return makeErrMessage(uint8(messages.Error_CODE_CHUNKSERVERS_UNAVAILABLE), err)
		}
		if err := m.grantLease(newChunk); err != nil {
			return makeErrMessage(uint8(messages.Error_CODE_CHUNKSERVERS_UNAVAILABLE), err)
		}

		newChunk.version++
		tx := &transaction{}
		chunk.updateRef(tx, chunk.ref-1)
		m.chunkTable.addChunk(tx, newChunk)
		file.metadata.replaceChunkHandle(tx, req.Path, int(req.ChunkIdx), chunk.id, newChunk.id)
		tx.commit()

		chunk.Unlock()

		chunk = newChunk
		chunk.Lock()

	} else if time.Since(chunk.leaseGrantTime) > leaseDeadline { // lease expired
		err := m.grantLease(chunk)
		if err != nil {
			return makeErrMessage(uint8(messages.Error_CODE_CHUNKSERVERS_UNAVAILABLE), err)
		}
		tx := &transaction{}
		chunk.updateVersion(tx, chunk.version+1)
		tx.commit()
	}

	return m.setWriteResponse(chunk)
}

func (m *master) copyOnWrite(oldChunk *chunk) (*chunk, error) {

	newChunk := newChunk()
	newChunk.version = oldChunk.version
	newChunk.ref = 1
	newChunk.repFactor = oldChunk.repFactor

	req := &messages.CopyChunk{
		SrcId: oldChunk.id,
		DstId: newChunk.id,
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	msg := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_COPY_CHUNK_REQ),
			Size: uint32(len(payload)),
		},
	}

	wg := sync.WaitGroup{}

	for _, loc := range oldChunk.getLocations(nil) {

		wg.Add(1)
		go func(loc string) {
			defer wg.Done()
			ack, err := m.sendRequest(loc, msg, copyAckTimeout)
			if err == nil {

				res := &messages.ChunkserverOpAck{}
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
		return nil, ErrUnavailable
	}
	return newChunk, nil
}

func (m *master) snapshotHandler(msg *hive.Message) *hive.Message {

	req := &messages.Snapshot{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := &transaction{}

	nodes, err := m.namespace.snapshot(tx, req.Src, req.Dst)
	if err != nil {
		tx.rollback()
		return makeErrMessage(uint8(messages.Error_CODE_SNAPSHOT_FAILED), err)
	}

	primaryChunkMap := make(map[string][]uint64)

	chunkLocks := []*sync.RWMutex{}
	chunkSet := make(map[uint64]struct{})

	m.chunkTable.mu.RLock()
	for _, node := range nodes {
		if !node.dir {

			for _, handle := range node.chunkHandles {

				if _, ok := chunkSet[handle]; !ok {
					if chunk, ok := m.chunkTable.entries[handle]; ok {
						chunk.Lock()
						if time.Since(chunk.leaseGrantTime) < leaseDeadline {
							primaryChunkMap[chunk.primary] = append(primaryChunkMap[chunk.primary], handle)
						}
						chunk.updateRef(tx, chunk.ref+1)
						chunkLocks = append(chunkLocks, &chunk.RWMutex)
						chunkSet[handle] = struct{}{}
					}
				}
			}
		}
	}

	tx.addFunc(func() {
		for _, lock := range chunkLocks {
			lock.Unlock()
		}
	})

	m.chunkTable.mu.RUnlock()

	wg := sync.WaitGroup{}

	for primary, handles := range primaryChunkMap {
		wg.Add(1)
		go func(primary string, handles []uint64) {
			defer wg.Done()
			m.revokeLease(primary, handles)
		}(primary, handles)
	}

	wg.Wait()

	tx.commit()

	res := &messages.ClientOpAck{
		Success: true,
	}

	payload, err := proto.Marshal(res)
	if err != nil {
		panic(err)
	}

	return &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Client_MESSAGE_TYPE_OP_ACK),
			Size: uint32(len(payload)),
		},
	}

}

func (m *master) updateVersion(chunkID uint64, v uint32, servers []string) ([]string, error) {

	payload, err := proto.Marshal(&messages.UpdateVersion{
		Id:      chunkID,
		Version: v,
	})
	if err != nil {
		panic(fmt.Errorf("update marshal error : %s", err.Error()))
	}

	wg := sync.WaitGroup{}

	var acked []string
	ackedCh := make(chan string)
	done := make(chan struct{})

	go func() {
		for server := range ackedCh {
			acked = append(acked, server)
		}
		close(done)
	}()

	for _, server := range servers {

		msg := &hive.Message{
			Metadata: hive.Metadata{
				Type: uint8(messages.Master_MESSAGE_TYPE_UPDATE_VERSION_REQ),
				Size: uint32(len(payload)),
			},
			Payload: payload,
		}

		wg.Add(1)
		go func(server string, msg *hive.Message) {
			defer wg.Done()

			ackMsg, err := m.sendRequest(server, msg, versionAckTimeout)

			if err == nil && ackMsg.Type == uint8(messages.Chunkserver_MESSAGE_TYPE_OP_ACK) {

				ack := &messages.ChunkserverOpAck{}

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
		return nil, ErrOpFailed
	}

	return acked, nil
}

func (m *master) grantLease(c *chunk) error {

	payload, err := proto.Marshal(&messages.GrantLease{
		Id: c.id,
	})
	if err != nil {
		panic(fmt.Errorf("grant lease : %s", err.Error()))
	}

	locations := c.getLocations(nil)

	acked, err := m.updateVersion(c.id, c.version+1, locations)
	if err != nil {
		return err
	}

	msg := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_GRANT_LEASE_REQ),
			Size: uint32(len(payload)),
		},
	}

	// TODO: Think about what happens to acked chunks in chunkservers if lease grant fails
	var primary string
	for _, addr := range acked {

		ackMsg, err := m.sendRequest(addr, msg, grantLeaseTimeout)
		if err == nil && ackMsg.Type == uint8(messages.Chunkserver_MESSAGE_TYPE_OP_ACK) {

			ack := &messages.ChunkserverOpAck{}

			err = proto.Unmarshal(ackMsg.Payload, ack)

			if err == nil && ack.Success {
				primary = addr
				break
			}

		}
	}

	if len(primary) == 0 {
		return ErrOpFailed
	}

	c.primary = primary
	c.leaseGrantTime = time.Now()

	for _, server := range acked {
		c.locations[server] = time.Now()
	}

	return nil
}

func (m *master) revokeLease(primary string, handles []uint64) {

	req := &messages.RevokeLease{
		Handles: handles,
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}

	msg := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_REVOKE_LEASE_REQ),
			Size: uint32(len(payload)),
		},
	}

	_, err = m.sendRequest(primary, msg, leaseDeadline)
	if err != nil && !errors.Is(err, ErrRequestTimeout) {
		time.Sleep(leaseDeadline)
	}
}

func makeErrMessage(code uint8, err error) *hive.Message {

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

var ErrRequestTimeout = errors.New("request timedout")
var ErrUnavailable = errors.New("unavailable")
var ErrOpFailed = errors.New("operation failed")
var ErrProtoMarshal = errors.New("proto marshal error")
var ErrProtoUnmarshal = errors.New("proto unmarshal error")
