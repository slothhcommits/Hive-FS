package master

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/PraveenC2002/gfs/internal/hive"
	"github.com/PraveenC2002/gfs/internal/messages"
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
	isShadow    bool
	listenAddr  string
	shadowAddrs []string
}

type master struct {
	*MasterOpts

	transport hive.Transport

	*persistentState

	csr *chunkServerRegistry

	closeCh chan struct{}
}

func newMaster(opts *MasterOpts) *master {
	return &master{

		MasterOpts: opts,

		transport: hive.NewTCPTransport(),

		persistentState: newPersistentState(),

		csr: newChunkServerRegistry(),

		closeCh: make(chan struct{}),
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
		req.Respond(m.snapshotHandler(req.Message()))

	case uint8(messages.Client_MESSAGE_TYPE_DELETE):
		req.Respond(m.deleteHandler(req.Message()))

	// chunkserver
	case uint8(messages.Chunkserver_MESSAGE_TYPE_HEARTBEAT):
		req.Respond(m.heartbeatHandler(req.Message()))
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

	req := &messages.Client_Create{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := &transaction{}
	if err := m.namespace.create(tx, req.Path); err != nil {
		tx.rollback()
		return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}
	tx.commit()

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

func (m *master) addNewChunk(path string, file *node) error {

	tx := &transaction{}

	chunk := newChunk()

	m.chunkTable.stagedCreateMu.Lock()
	m.chunkTable.stagedCreate[chunk.id] = struct{}{}
	m.chunkTable.stagedCreateMu.Unlock()
	defer func() {
		m.chunkTable.stagedCreateMu.Lock()
		delete(m.chunkTable.stagedCreate, chunk.id)
		m.chunkTable.stagedCreateMu.Unlock()
	}()

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

func (m *master) copyOnWrite(oldChunk *chunk) (*chunk, error) {

	newChunk := newChunk()
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

	msg := &hive.Message{
		Payload: payload,
		Metadata: hive.Metadata{
			Type: uint8(messages.Master_MESSAGE_TYPE_LOCAL_COPY_CHUNK),
			Size: uint32(len(payload)),
		},
	}

	m.chunkTable.stagedCreateMu.Lock()
	m.chunkTable.stagedCreate[newChunk.id] = struct{}{}
	m.chunkTable.stagedCreateMu.Unlock()

	wg := sync.WaitGroup{}

	for _, loc := range oldChunk.getLocations(nil) {

		wg.Add(1)
		go func(loc string) {
			defer wg.Done()
			ack, err := m.sendRequest(loc, msg, copyAckTimeout)
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
		m.chunkTable.stagedCreateMu.Lock()
		delete(m.chunkTable.stagedCreate, newChunk.id)
		m.chunkTable.stagedCreateMu.Unlock()
		return nil, ErrUnavailable
	}
	return newChunk, nil
}

func (m *master) setWriteResponse(chunk *chunk) *hive.Message {

	sendReplicas := &messages.Master_SendChunkReplicas{
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

	req := &messages.Client_GetChunkReplicas{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	unlockNamespace := &transaction{}

	file, err := m.namespace.lookup(unlockNamespace, req.Path)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}
	defer unlockNamespace.commit()
	file.Lock()
	defer file.Unlock()

	// sparse write, length exceeded
	if uint32(len(file.chunkHandles)) < req.ChunkIdx {
		return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), fmt.Errorf("length exceeded"))
	}

	// add new chunk
	if uint32(len(file.chunkHandles)) == req.ChunkIdx {

		if err := m.addNewChunk(req.Path, file); err != nil {
			return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}

		m.chunkTable.mu.RLock()
		chunk := m.chunkTable.entries[file.chunkHandles[req.ChunkIdx]]
		m.chunkTable.mu.RUnlock()

		chunk.Lock()
		defer chunk.Unlock()
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
			return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}

		defer func() {
			m.chunkTable.stagedCreateMu.Lock()
			delete(m.chunkTable.stagedCreate, newChunk.id)
			m.chunkTable.stagedCreateMu.Unlock()
		}()

		if err := m.grantLease(newChunk); err != nil {
			return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
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
			return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
		}
		tx := &transaction{}
		chunk.updateVersion(tx, chunk.version+1)
		tx.commit()
	}

	return m.setWriteResponse(chunk)
}

func (m *master) snapshotHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_Snapshot{}
	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := &transaction{}

	nodes, err := m.namespace.snapshot(tx, req.Src, req.Dst)
	if err != nil {
		tx.rollback()
		return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}

	primaryChunkMap := make(map[string][]uint64)

	chunkLocks := []*sync.Mutex{}
	chunkSet := make(map[uint64]struct{})

	m.chunkTable.mu.RLock()

	handles := []uint64{}
	for _, node := range nodes {
		if !node.dir {
			for _, handle := range node.chunkHandles {
				handles = append(handles, handle)
			}
		}
	}

	slices.Sort(handles)

	chunks := []*chunk{}
	for _, handle := range handles {
		if _, ok := chunkSet[handle]; !ok {
			if chunk, ok := m.chunkTable.entries[handle]; ok {
				chunks = append(chunks, chunk)
				chunkSet[handle] = struct{}{}
			}
		}
	}

	m.chunkTable.mu.RUnlock()

	for _, chunk := range chunks {
		chunk.Lock()
		if time.Since(chunk.leaseGrantTime) < leaseDeadline {
			primaryChunkMap[chunk.primary] = append(primaryChunkMap[chunk.primary], chunk.id)
		}
		chunk.updateRef(tx, chunk.ref+1)
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
			m.revokeLease(primary, handles)
		}(primary, handles)
	}

	wg.Wait()

	tx.commit()

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

func (m *master) updateVersion(chunkID uint64, v uint32, servers []string) ([]string, error) {

	payload, err := proto.Marshal(&messages.Master_UpdateVersion{
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
		return nil, ErrOpFailed
	}

	return acked, nil
}

func (m *master) grantLease(c *chunk) error {

	payload, err := proto.Marshal(&messages.Master_GrantLease{
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
		if err == nil && ackMsg.Type == uint8(messages.MessageType_ACK) {

			ack := &messages.Ack{}

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
			Type: uint8(messages.Master_MESSAGE_TYPE_REVOKE_LEASE_REQ),
			Size: uint32(len(payload)),
		},
	}

	_, err = m.sendRequest(primary, msg, leaseDeadline)
	if err != nil && !errors.Is(err, ErrRequestTimeout) {
		time.Sleep(leaseDeadline)
	}
}

func (m *master) deleteHandler(msg *hive.Message) *hive.Message {

	req := &messages.Client_Delete{}

	err := proto.Unmarshal(msg.Payload, req)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	tx := &transaction{}
	err = m.namespace.delete(tx, req.Path)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_OP_FAILED), err)
	}

	tx.commit()

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

func (m *master) heartbeatHandler(msg *hive.Message) *hive.Message {

	hb := &messages.Chunkserver_Heartbeat{}
	err := proto.Unmarshal(msg.Payload, hb)
	if err != nil {
		return makeErrMessage(uint8(messages.Error_CODE_UNMARSHAL_FAILED), err)
	}

	m.csr.update(hb)
	res := m.chunkTable.processHeartbeat(hb)

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

func (m *master) garbageCollector() {

	for {

		txLimit := 10
		opLimit := 1000

		opsLeft := txLimit * opLimit

		type entry struct {
			key       string
			node      *deletedNode
			processed int
		}

		entries := []*entry{}
		handleSet := make(map[uint64]uint32)

		m.namespace.deletedMu.Lock()
		for key, deletedNode := range m.namespace.deleted {

			if opsLeft <= 0 {
				break
			}

			if time.Since(time.Unix(0, deletedNode.deletedAt)) > nodeExpiryWindow {

				m.chunkTable.mu.RLock()
				i := 0
				for _, handle := range deletedNode.chunkHandles {

					if opsLeft <= 0 {
						break
					}

					if _, ok := m.chunkTable.entries[handle]; ok {
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
				m.chunkTable.mu.RUnlock()

				entries = append(entries, &entry{key: key, node: deletedNode, processed: i})
				opsLeft--
			}
		}
		m.namespace.deletedMu.Unlock() // TODO: could have been RW lock

		tx := &transaction{}

		for _, entry := range entries {

			tx.addOp(&operation{
				t:    DELETE,
				args: []string{entry.key, strconv.FormatInt(int64(entry.processed), 10)},
			})

			tx.addFunc(func() {
				m.namespace.deletedMu.Lock()
				if entry.processed >= len(entry.node.chunkHandles) {
					delete(m.namespace.deleted, entry.key)
				} else {
					entry.node.Lock()
					entry.node.chunkHandles = entry.node.chunkHandles[entry.processed:]
					entry.node.Unlock()
				}
				m.namespace.deletedMu.Unlock()
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
			m.chunkTable.mu.RLock()
			chunk, ok := m.chunkTable.entries[handle]
			m.chunkTable.mu.RUnlock()

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
		tx.commit()

		opsLeft = txLimit * opLimit

		tx = &transaction{}
		locks = []*sync.Mutex{}

		m.chunkTable.mu.RLock()
		chunks := []*chunk{}
		for _, chunk := range m.chunkTable.entries {
			chunks = append(chunks, chunk)
		}
		m.chunkTable.mu.RUnlock()

		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].id < chunks[j].id
		})

		for _, chunk := range chunks {

			if opsLeft == 0 {
				break
			}

			chunk.Lock()
			if chunk.ref == 0 {
				tx.addOp(&operation{
					t:    DELETE_CHUNK,
					args: []string{strconv.FormatUint(chunk.id, 10)},
				})
				tx.addFunc(func() {
					m.chunkTable.mu.Lock()
					delete(m.chunkTable.entries, chunk.id)
					m.chunkTable.mu.Unlock()
				})
				locks = append(locks, &chunk.Mutex)
				opsLeft--
			} else {
				chunk.Unlock()
			}
		}

		tx.addFunc(unlock)
		tx.commit()

		select {
		case <-m.closeCh:
			return
		case <-time.After(gcInterval):

		}
	}
}

func (m *master) reReplicate() {

	for {

		m.chunkTable.repRequestsMu.Lock()
		for handle, reqs := range m.chunkTable.repRequests {
			for addr, issuedAt := range reqs {
				if time.Since(issuedAt) > repRequestExpiry {
					delete(reqs, addr)
				}
			}
			if len(reqs) == 0 {
				delete(m.chunkTable.repRequests, handle)
			}
		}
		m.chunkTable.repRequestsMu.Unlock()

		m.chunkTable.mu.RLock()
		chunks := []*chunk{}
		for _, chunk := range m.chunkTable.entries {
			chunks = append(chunks, chunk)
		}
		m.chunkTable.mu.RUnlock()

		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].id < chunks[j].id
		})

		type chunkMetadata struct {
			handle    uint64
			repFactor uint32
			version   uint32
			primary   string
			srcs      []string
		}

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

		wg := &sync.WaitGroup{}
		for _, o := range over {
			n := uint32(len(o.srcs)) - o.repFactor

			var addrs []string
			for _, addr := range o.srcs {
				if uint32(len(addrs)) == n {
					break
				}
				if addr != o.primary {
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

					m.sendRequest(addr, msg, 0)

					m.chunkTable.mu.RLock()
					chunk, ok := m.chunkTable.entries[o.handle]
					m.chunkTable.mu.RUnlock()

					if ok {
						chunk.Lock()
						delete(chunk.locations, addr)
						chunk.Unlock()
					}
				}(o, addr)
			}
		}

		sort.Slice(under, func(i, j int) bool {
			return len(under[i].srcs) < len(under[j].srcs)
		})

		intervalLimit := 20

		under = under[:min(len(under), intervalLimit)]

		for _, s := range under {

			// servers already asked to pull this chunk, excluded from placement
			// so we don't ask the same one twice while its copy is in flight
			m.chunkTable.repRequestsMu.Lock()
			pending := slices.Collect(maps.Keys(m.chunkTable.repRequests[s.handle]))
			m.chunkTable.repRequestsMu.Unlock()

			needed := int(s.repFactor) - len(s.srcs) - len(pending)
			if needed <= 0 {
				continue
			}

			servers, err := m.csr.getServers(uint32(needed), append(slices.Clone(s.srcs), pending...))
			if err == nil {

				srcIdx := 0

				for _, server := range servers {
					wg.Add(1)
					go func(srcIdx int, server string) {
						defer wg.Done()
						req := &messages.Master_Replicate{
							Handle:  s.handle,
							Version: s.version,
							Src:     s.srcs[srcIdx%len(s.srcs)],
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

						m.sendRequest(server, msg, 0)

						m.chunkTable.repRequestsMu.Lock()
						if _, ok := m.chunkTable.repRequests[s.handle]; !ok {
							m.chunkTable.repRequests[s.handle] = make(map[string]time.Time)
						}
						m.chunkTable.repRequests[s.handle][server] = time.Now()
						m.chunkTable.repRequestsMu.Unlock()
					}(srcIdx, server)
					srcIdx++
				}

			}
		}

		wg.Wait()

		select {
		case <-m.closeCh:
			return
		case <-time.After(reReplicationInterval):
		}
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

// var ErrProtoMarshal = errors.New("proto marshal error")
var ErrProtoUnmarshal = errors.New("proto unmarshal error")
