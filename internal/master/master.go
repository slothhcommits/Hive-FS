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
		
		transport:  hive.NewTCPTransport(),

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

	f := &node{
		dir:      false,
		metadata: metadata{},
	}

	if err := m.namespace.create(req.Path, f); err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_CREATE_FILE_FAILED), err)
	}

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

func (m *master) addNewChunk(file *node) error {

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

	m.chunkTable.mu.Lock()
	m.chunkTable.entries[chunk.id] = chunk
	m.chunkTable.mu.Unlock()

	file.chunkHandles = append(file.chunkHandles, chunk.id)
	chunk.ref++

	return nil
}

func (m *master) writeHandler(msg *hive.Message) *hive.Message {

	req := &messages.GetChunkReplicas{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	file, unlock, err := m.namespace.lookup(req.Path)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_LOOKUP_FAILED), err)
	}
	file.RLock()
	unlock()

	if uint32(len(file.chunkHandles)) < req.ChunkIdx {
		file.RUnlock()
		return makeErrMessage(uint8(messages.Error_CODE_LENGTH_EXCEEDED), fmt.Errorf("length exceeded"))
	}

	// TODO: Handle ref check
	var chunk *chunk

	if uint32(len(file.chunkHandles)) == req.ChunkIdx {

		file.RUnlock()
		file.Lock()

		if uint32(len(file.chunkHandles)) == req.ChunkIdx {
			if err := m.addNewChunk(file); err != nil {
				file.Unlock()
				return makeErrMessage(uint8(messages.Error_CODE_CHUNKSERVERS_UNAVAILABLE), err)
			}

		}

		m.chunkTable.mu.RLock()
		chunk = m.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
		m.chunkTable.mu.RUnlock()
		chunk.RLock()
		file.Unlock()

	} else {

		m.chunkTable.mu.RLock()
		chunk = m.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
		m.chunkTable.mu.RUnlock()

		chunk.RLock()
		file.RUnlock()

		if time.Since(chunk.leaseGrantTime) > leaseDeadline {

			chunk.RUnlock()

			chunk.Lock()
			if time.Since(chunk.leaseGrantTime) > leaseDeadline {
				err := m.grantLease(chunk)
				if err != nil {
					chunk.Unlock()
					return makeErrMessage(uint8(messages.Error_CODE_CHUNKSERVERS_UNAVAILABLE), err)
				}
			}
			chunk.Unlock()

			chunk.RLock()
		}
	}

	sendReplicas := &messages.SendChunkReplicas{
		ChunkId:     chunk.id,
		Version:     chunk.version,
		Primary:     chunk.primary,
		LeaseExpiry: chunk.leaseGrantTime.Add(leaseDeadline).Unix(),
		Locations:   chunk.getLocations(nil),
	}

	chunk.RUnlock()
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

func (m *master) snapshotHandler(msg *hive.Message) *hive.Message {

	req := &messages.Snapshot{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	return nil
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

			if err == nil && ackMsg.Type == uint8(messages.Chunkserver_MESSAGE_TYPE_UPDATE_VERSION_ACK) {

				ack := &messages.UpdateVersionAck{}

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
		if err == nil && ackMsg.Type == uint8(messages.Chunkserver_MESSAGE_TYPE_LEASE_GRANT_ACK) {

			ack := messages.GrantLeaseAck{}

			err = proto.Unmarshal(ackMsg.Payload, &ack)

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
	c.version++

	return nil
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
