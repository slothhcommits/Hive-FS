package master

import (
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type metadata struct {
	chunkHandles []uint64
}

func (m *metadata) appendChunkHandle(tx *transaction, path string, handle uint64) {

	tx.addOp(&operation{
		t:    APPEND_CHUNK,
		args: []string{path, strconv.FormatUint(handle, 10)},
	})

	tx.addFunc(func() {
		m.chunkHandles = append(m.chunkHandles, handle)
	})
}

// all validation to be done in caller
func (m *metadata) replaceChunkHandle(tx *transaction, path string, idx int, oldHandle uint64, newHandle uint64) {

	tx.addOp(&operation{
		t: REPLACE_CHUNK,
		args: []string{
			path,
			strconv.FormatUint(uint64(idx), 10),
			strconv.FormatUint(oldHandle, 10),
			strconv.FormatUint(newHandle, 10),
		},
	})

	tx.addFunc(func() {
		m.chunkHandles[idx] = newHandle
	})
}

type node struct {
	sync.RWMutex
	dir bool
	metadata
}

type deletedNode struct {
	deletedAt int64
	*node
}

type lock struct {
	ref uint
	sync.RWMutex
}

type lockManager struct {
	sync.Mutex
	locks map[string]*lock
}

func newLockManager() *lockManager {
	return &lockManager{
		Mutex: sync.Mutex{},
		locks: make(map[string]*lock),
	}
}

type lockReq struct {
	path string
	read bool
}

const (
	maxPathlen = 256
)

type namespace struct {
	lm *lockManager

	mu      sync.RWMutex
	entries map[string]*node

	deletedMu sync.Mutex
	deleted   map[string]*deletedNode
}

func newNamesapce() *namespace {
	return &namespace{
		lm:      newLockManager(),
		mu:      sync.RWMutex{},
		entries: make(map[string]*node),
		deleted: make(map[string]*deletedNode),
	}
}

func (ns *namespace) allowedChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
}

func (ns *namespace) validatePath(path string) bool {

	if len(path) == 0 || len(path) > maxPathlen || path[0] != '/' {
		return false
	}

	segs := strings.Split(path[1:], "/")

	for i := 0; i < len(segs); i++ {

		if segs[i] == "" {
			return false
		}

		for _, c := range segs[i] {
			if c == '.' && i != len(segs)-1 {
				return false
			}
			if !ns.allowedChar(c) {
				return false
			}
		}
	}

	return true
}

func (ns *namespace) pathDepth(path string) uint {
	return uint(strings.Count(path, "/"))
}

func (ns *namespace) expand(lockReqs []*lockReq) []*lockReq {

	lockReqs = append(lockReqs, &lockReq{path: "/", read: true})

	set := make(map[string]struct{})
	set["/"] = struct{}{}

	for _, p := range lockReqs {
		set[p.path] = struct{}{}
	}

	sort.Slice(lockReqs, func(i, j int) bool {
		return len(lockReqs[i].path) < len(lockReqs[j].path)
	})

	n := len(lockReqs)
	for i := 0; i < n; i++ {
		pl := lockReqs[i]

		segs := strings.Split(pl.path, "/")
		currStr := ""
		for j, seg := range segs[1:] {
			currStr = strings.Join([]string{currStr, seg}, "/")
			if _, ok := set[currStr]; !ok {
				if j != len(segs)-2 {
					lockReqs = append(lockReqs, &lockReq{path: currStr, read: true})
				}
				set[currStr] = struct{}{}
			}
		}
	}

	sort.Slice(lockReqs, func(i, j int) bool {
		di := ns.pathDepth(lockReqs[i].path)
		dj := ns.pathDepth(lockReqs[j].path)
		if di != dj {
			return di < dj
		}
		return strings.Compare(lockReqs[i].path, lockReqs[j].path) < 0
	})

	return lockReqs
}

func (ns *namespace) acquireLocks(lockReqs []*lockReq) (func(), error) {

	release := func(lockReqs []*lockReq) {

		locks := []*lock{}
		ns.lm.Lock()
		for _, p := range lockReqs {
			l := ns.lm.locks[p.path]
			l.ref--
			if l.ref == 0 {
				delete(ns.lm.locks, p.path)
			}
			locks = append(locks, l)
		}
		ns.lm.Unlock()

		for i, l := range locks {
			if lockReqs[i].read {
				l.RUnlock()
			} else {
				l.Unlock()
			}
		}
	}

	for _, req := range lockReqs {

		var l *lock
		var ok bool

		ns.lm.Lock()
		l, ok = ns.lm.locks[req.path]
		if !ok {
			l = &lock{}
			ns.lm.locks[req.path] = l
		}
		l.ref++
		ns.lm.Unlock()

		if req.read {
			l.RLock()
		} else {
			l.Lock()
		}

		// TODO: look out for transaction rollback
		// if !req.noOp {
		// 	if err := req.op(req.path); err != nil {
		// 		release(lockReqs[:i+1])
		// 		return nil, err
		// 	}
		// }
	}

	return func() {
		release(lockReqs)
	}, nil
}

func (ns *namespace) mkdirP(tx *transaction, path string) error {

	ns.mu.RLock()
	f, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {

		tx.addOp(&operation{
			t:    CREATE_DIR,
			args: []string{path},
		})
		tx.addFunc(func() {
			ns.mu.Lock()
			ns.entries[path] = &node{dir: true}
			ns.mu.Unlock()
		})

	} else if !f.dir {
		return &ErrInvalidPath{}
	}

	return nil
}

func (ns *namespace) create(tx *transaction, path string) error {

	if !ns.validatePath(path) {
		return &ErrInvalidPath{}
	}

	lockReqs := []*lockReq{{path: path, read: false}}

	lockReqs = ns.expand(lockReqs)

	unlock, err := ns.acquireLocks(lockReqs)
	if err != nil {
		return err
	}

	tx.addRB(unlock)

	for _, req := range lockReqs[:len(lockReqs)-1] {
		if err := ns.mkdirP(tx, req.path); err != nil {
			return err
		}
	}

	ns.mu.RLock()
	_, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {

		tx.addOp(&operation{
			t:    CREATE_FILE,
			args: []string{path},
		})

		tx.addFunc(func() {
			ns.mu.Lock()
			ns.entries[path] = &node{
				dir:      false,
				metadata: metadata{},
			}
			ns.mu.Unlock()
			unlock()
		})

	} else {
		return &ErrObjectExist{}
	}

	return nil
}

func (ns *namespace) checkAncestor(path string) error {
	ns.mu.RLock()
	node, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {
		return &ErrNotFound{}
	}
	if !node.dir {
		return &ErrInvalidPath{}
	}
	return nil
}

func (ns *namespace) lookup(tx *transaction, path string) (*node, error) {

	if !ns.validatePath(path) {
		return nil, &ErrInvalidPath{}
	}

	lockReqs := []*lockReq{
		{path: path, read: true},
	}
	lockReqs = ns.expand(lockReqs)

	unlock, err := ns.acquireLocks(lockReqs)
	if err != nil {
		unlock()
		return nil, err
	}

	var f *node
	ns.mu.RLock()
	f, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {
		unlock()
		return nil, &ErrNotFound{}
	}

	tx.addFunc(unlock)

	return f, nil
}

func (ns *namespace) isAncestor(p1, p2 string) bool {

	if p1 == "/" {
		return true
	}

	segs1 := strings.Split(p1, "/")
	segs2 := strings.Split(p2, "/")

	if len(segs1) > len(segs2) {
		return false
	}

	flag := true

	for i, seg := range segs1 {
		if seg != segs2[i] {
			flag = false
			break
		}
	}

	return flag
}

func (ns *namespace) prefixScan(prefix string) []string {

	nodes := []string{}

	ns.mu.RLock()
	for path, _ := range ns.entries {
		if ns.isAncestor(prefix, path) {
			nodes = append(nodes, path)
		}
	}
	ns.mu.RUnlock()

	return nodes
}

func (ns *namespace) moveNodes(tx *transaction, list []*struct {
	src string
	dst string
}) {

	for _, ele := range list {
		tx.addOp(&operation{
			t:    MOVE,
			args: []string{ele.src, ele.dst},
		})
	}

	tx.addFunc(func() {
		ns.mu.Lock()
		for _, ele := range list {
			srcNode := ns.entries[ele.src]
			delete(ns.entries, ele.src)
			ns.entries[ele.dst] = srcNode
		}
		ns.mu.Unlock()
	})
}

func (ns *namespace) move(tx *transaction, src, dst string) error {

	if !ns.validatePath(src) || !ns.validatePath(dst) || ns.isAncestor(src, dst) {
		return &ErrInvalidPath{}
	}

	fullSet := []*lockReq{
		{path: src, read: false},
		{path: dst, read: false},
	}

	fullSet = ns.expand(fullSet)

	unlock, err := ns.acquireLocks(fullSet)
	if err != nil {
		return err
	}
	tx.addRB(unlock)

	srcSet := []*lockReq{
		{path: src, read: false},
	}
	srcSet = ns.expand(srcSet)

	dstSet := []*lockReq{
		{path: dst, read: false},
	}

	dstSet = ns.expand(dstSet)

	for _, ele := range srcSet[:len(src)-1] {
		if err := ns.checkAncestor(ele.path); err != nil {
			return err
		}
	}

	for _, ele := range dstSet {
		if err := ns.mkdirP(tx, ele.path); err != nil {
			return err
		}
	}

	var srcNode, dstNode *node

	ns.mu.RLock()

	srcNode, ok := ns.entries[src]
	if !ok {
		ns.mu.RUnlock()
		return &ErrNotFound{}
	}

	dstNode, dstExists := ns.entries[dst]
	if !dstExists {
		dstNode = srcNode
	}

	ns.mu.RUnlock()

	switch {

	case srcNode.dir && !dstNode.dir:
		return &ErrIsFile{}

	case !srcNode.dir && !dstNode.dir:

		ns.moveNodes(tx, []*struct {
			src string
			dst string
		}{{src: src, dst: dst}})

	case !srcNode.dir && dstNode.dir:

		newPath := dst + "/" + src[strings.LastIndex(src, "/")+1:]
		ns.moveNodes(tx, []*struct {
			src string
			dst string
		}{{src: src, dst: newPath}})

	case srcNode.dir && dstNode.dir:

		contents := ns.prefixScan(src)

		var newPrefix string
		if !dstExists {
			newPrefix = dst
		} else {
			newPrefix = dst + "/" + src[strings.LastIndex(src, "/")+1:]
		}

		list := []*struct {
			src string
			dst string
		}{}

		for _, path := range contents {
			list = append(list, &struct {
				src string
				dst string
			}{src: path, dst: newPrefix + strings.TrimPrefix(path, src)})
		}

		ns.moveNodes(tx, list)
	}

	tx.addFunc(unlock)

	return nil
}

func (ns *namespace) snapshotNodes(tx *transaction, list []*struct {
	src string
	dst string
}) {

	for _, ele := range list {
		tx.addOp(&operation{
			t:    SNAPSHOT,
			args: []string{ele.src, ele.dst},
		})
	}

	tx.addFunc(func() {
		ns.mu.Lock()
		for _, ele := range list {
			// TODO: Bump ref counts on chunk handles (ONCE! or double counting failure)
			// TODO: copy any metadata or node slices explicitly to avoid shallow copy
			srcNode := ns.entries[ele.src]
			srcNode.RLock()
			ns.entries[ele.dst] = &node{
				dir: srcNode.dir,
				metadata: metadata{
					chunkHandles: slices.Clone(srcNode.chunkHandles),
				},
			}
			srcNode.RUnlock()
		}
		ns.mu.Unlock()
	})
}

func (ns *namespace) snapshot(tx *transaction, src, dst string) ([]*node, error) {

	if !ns.validatePath(src) ||
		!ns.validatePath(dst) ||
		ns.isAncestor(src, dst) {
		return nil, &ErrInvalidPath{}
	}

	lockReqs := []*lockReq{
		{path: src, read: false},
		{path: dst, read: false},
	}

	lockReqs = ns.expand(lockReqs)

	unlock, err := ns.acquireLocks(lockReqs)
	if err != nil {
		return nil, err
	}
	tx.addRB(unlock)

	for _, req := range lockReqs {

		if req.path != src && req.path != dst {
			if err := ns.checkAncestor(req.path); err != nil {
				return nil, err
			}
		}
	}

	var srcNode *node

	ns.mu.RLock()

	srcNode, ok := ns.entries[src]
	if !ok {
		ns.mu.RUnlock()
		return nil, &ErrNotFound{}
	}

	_, ok = ns.entries[dst]
	if ok {
		ns.mu.RUnlock()
		return nil, &ErrObjectExist{}
	}

	ns.mu.RUnlock()

	var nodes []*node

	switch {

	case !srcNode.dir:

		// TODO: Bump ref counts on chunk handles (ONCE! or double counting failure)
		// TODO: copy any metadata or node slices explicitly to avoid shallow copy
		ns.snapshotNodes(tx, []*struct {
			src string
			dst string
		}{{src: src, dst: dst}})

		ns.mu.RLock()
		nodes = append(nodes, ns.entries[src])
		ns.mu.RUnlock()

	case srcNode.dir:

		// TODO: Bump ref counts on chunk handles (ONCE! or double counting failure)
		// TODO: copy any metadata or node slices explicitly to avoid shallow copy

		contents := ns.prefixScan(src)

		list := []*struct {
			src string
			dst string
		}{}

		ns.mu.RLock()
		for _, path := range contents {
			list = append(list, &struct {
				src string
				dst string
			}{src: path, dst: dst + strings.TrimPrefix(path, src)})
			nodes = append(nodes, ns.entries[path])
		}
		ns.mu.RUnlock()

		ns.snapshotNodes(tx, list)

	}

	tx.addFunc(unlock)

	return nodes, nil
}

func (ns *namespace) list(path string) ([]string, error) {

	lockReqs := []*lockReq{{path: path, read: true}}
	lockReqs = ns.expand(lockReqs)

	unlock, err := ns.acquireLocks(lockReqs)
	if err != nil {
		return nil, err
	}
	defer unlock()

	for _, req := range lockReqs[:len(lockReqs)-1] {
		if err := ns.checkAncestor(req.path); err != nil {
			return nil, err
		}
	}

	var contents []string

	paths := ns.prefixScan(path)
	ns.mu.RLock()
	for _, p := range paths {
		f, ok := ns.entries[p]
		if ok && !f.dir {
			contents = append(contents, p)
		}
	}
	ns.mu.RUnlock()

	return contents, nil
}

func (ns *namespace) delete(tx *transaction, path string) error {

	lockReqs := []*lockReq{{path: path, read: false}}

	lockReqs = ns.expand(lockReqs)

	unlock, err := ns.acquireLocks(lockReqs)
	if err != nil {
		return err
	}
	tx.addRB(unlock)

	for _, req := range lockReqs[:len(lockReqs)-1] {
		if err := ns.checkAncestor(req.path); err != nil {
			return err
		}
	}

	contents := ns.prefixScan(path)

	delTime := time.Now().UnixNano()
	for _, path := range contents {
		tx.addOp(&operation{
			t:    REMOVE,
			args: []string{path, strconv.FormatInt(delTime, 10)},
		})
	}

	tx.addFunc(func() {
		ns.mu.Lock()
		ns.deletedMu.Lock()
		for _, p := range contents {
			ns.deleted[p+"/"+strconv.FormatInt(delTime, 10)] = &deletedNode{
				deletedAt: delTime,
				node:      ns.entries[p],
			}
			delete(ns.entries, p)
		}
		ns.deletedMu.Unlock()
		ns.mu.Unlock()
		unlock()
	})

	return nil
}

type ErrInvalidPath struct{}

func (e *ErrInvalidPath) Error() string {
	return "invalid path"
}

type ErrObjectExist struct{}

func (e *ErrObjectExist) Error() string {
	return "object exist"
}

type ErrNotFound struct{}

func (e *ErrNotFound) Error() string {
	return "not found"
}

type ErrIsFile struct{}

func (e *ErrIsFile) Error() string {
	return "destination is file"
}
