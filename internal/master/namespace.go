package master

import (
	"slices"
	"sort"
	"strings"
	"sync"
)

type metadata struct {
	chunkHandles []uint64
}

type node struct {
	dir bool
	sync.RWMutex
	metadata
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
	noOp bool
	op   func(string) error
}

const (
	maxPathlen = 256
)

type namespace struct {
	lm *lockManager

	mu      sync.RWMutex
	entries map[string]*node
}

func newNamesapce() *namespace {
	return &namespace{
		lm:      newLockManager(),
		mu:      sync.RWMutex{},
		entries: make(map[string]*node),
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

func (ns *namespace) acquireLocks(lockReqs []*lockReq) (func(), error) {

	lockReqs = append(lockReqs, &lockReq{path: "/", read: true, noOp: false, op: ns.mkdirP})

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
					lockReqs = append(lockReqs, &lockReq{path: currStr, read: true, noOp: false, op: lockReqs[i].op})
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

	for i, req := range lockReqs {

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

		if !req.noOp {
			if err := req.op(req.path); err != nil {
				release(lockReqs[:i+1])
				return nil, err
			}
		}
	}

	return func() {
		release(lockReqs)
	}, nil
}

func (ns *namespace) mkdirP(path string) error {
	ns.mu.RLock()
	f, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {
		ns.mu.Lock()
		ns.entries[path] = &node{dir: true}
		ns.mu.Unlock()
	} else if !f.dir {
		return &ErrInvalidPath{}
	}

	return nil
}

func (ns *namespace) create(path string, f *node) error {

	if !ns.validatePath(path) {
		return &ErrInvalidPath{}
	}

	unlock, err := ns.acquireLocks([]*lockReq{{path: path, read: false, noOp: true, op: ns.mkdirP}})
	if err != nil {
		return err
	}
	defer unlock()

	ns.mu.RLock()
	_, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {
		ns.mu.Lock()
		ns.entries[path] = f
		ns.mu.Unlock()
	} else {
		return &ErrObjectExist{}
	}

	return nil
}

func (ns *namespace) checkExist(path string) error {
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

func (ns *namespace) lookup(path string) (*node, func(), error) {

	if !ns.validatePath(path) {
		return nil, nil, &ErrInvalidPath{}
	}

	unlock, err := ns.acquireLocks([]*lockReq{{path: path, read: true, noOp: true, op: ns.checkExist}})
	if err != nil {
		unlock()
		return nil, nil, err
	}

	var f *node
	ns.mu.RLock()
	f, ok := ns.entries[path]
	ns.mu.RUnlock()
	if !ok {
		unlock()
		return nil, nil, &ErrNotFound{}
	}

	return f, unlock, nil
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

func (ns *namespace) move(src, dst string) error {

	if !ns.validatePath(src) || !ns.validatePath(dst) || ns.isAncestor(src, dst) {
		return &ErrInvalidPath{}
	}

	unlock, err := ns.acquireLocks([]*lockReq{{path: src, read: false, noOp: true, op: ns.checkExist}, {path: dst, read: false, noOp: true, op: ns.mkdirP}})
	if err != nil {
		return err
	}
	defer unlock()

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

		ns.mu.Lock()
		delete(ns.entries, src)
		ns.entries[dst] = srcNode
		ns.mu.Unlock()

	case !srcNode.dir && dstNode.dir:

		newPath := dst + "/" + src[strings.LastIndex(src, "/")+1:]

		ns.mu.Lock()
		delete(ns.entries, src)
		ns.entries[newPath] = srcNode
		ns.mu.Unlock()

	case srcNode.dir && dstNode.dir:

		contents := ns.prefixScan(src)

		var newPrefix string
		if !dstExists {
			newPrefix = dst
		} else {
			newPrefix = dst + "/" + src[strings.LastIndex(src, "/")+1:]
		}

		ns.mu.Lock()
		for _, path := range contents {
			node := ns.entries[path]
			delete(ns.entries, path)
			ns.entries[newPrefix+strings.TrimPrefix(path, src)] = node
		}
		ns.mu.Unlock()
	}

	return nil
}

func (ns *namespace) snapshot(src, dst string) error {

	if !ns.validatePath(src) ||
		!ns.validatePath(dst) ||
		ns.isAncestor(src, dst) {
		return &ErrInvalidPath{}
	}

	unlock, err := ns.acquireLocks(
		[]*lockReq{
			{path: src, read: false, noOp: true, op: ns.checkExist},
			{path: dst, read: false, noOp: true, op: ns.checkExist},
		},
	)
	if err != nil {
		return err
	}
	defer unlock()

	var srcNode *node

	ns.mu.RLock()

	srcNode, ok := ns.entries[src]
	if !ok {
		ns.mu.RUnlock()
		return &ErrNotFound{}
	}

	_, ok = ns.entries[dst]
	if ok {
		ns.mu.RUnlock()
		return &ErrObjectExist{}
	}

	ns.mu.RUnlock()

	switch {

	case !srcNode.dir:

		ns.mu.Lock()
		// TODO: Bump ref counts on chunk handles (ONCE! or double counting failure)
		// TODO: copy any metadata or node slices explicitly to avoid shallow copy
		ns.entries[dst] = &node{
			dir:      srcNode.dir,
			metadata: srcNode.metadata,
		}
		ns.mu.Unlock()

	case srcNode.dir:

		contents := ns.prefixScan(src)

		ns.mu.Lock()
		for _, path := range contents {
			// TODO: Bump ref counts on chunk handles (ONCE! or double counting failure)
			// TODO: copy any metadata or node slices explicitly to avoid shallow copy
			pathNode := ns.entries[path]
			pathNode.RLock()
			ns.entries[dst+strings.TrimPrefix(path, src)] = &node{
				dir: pathNode.dir,
				metadata: metadata{
					chunkHandles: slices.Clone(pathNode.chunkHandles),
				},
			}
			pathNode.RUnlock()
		}
		ns.mu.Unlock()
	}

	return nil
}

func (ns *namespace) list(path string) ([]*node, error) {

	unlock, err := ns.acquireLocks([]*lockReq{{path: path, read: true, noOp: true, op: ns.checkExist}})
	if err != nil {
		return nil, err
	}
	defer unlock()

	var nodes []*node

	paths := ns.prefixScan(path)
	ns.mu.RLock()
	for _, p := range paths {
		f := ns.entries[p]
		if !f.dir {
			nodes = append(nodes, f)
		}
	}
	ns.mu.RUnlock()

	return nodes, nil
}

// TODO: tombstone and gc
func (ns *namespace) delete(path string) error {

	unlock, err := ns.acquireLocks([]*lockReq{{path: path, read: false, noOp: true, op: ns.checkExist}})
	if err != nil {
		return err
	}
	defer unlock()

	paths := ns.prefixScan(path)
	ns.mu.Lock()
	for _, p := range paths {
		delete(ns.entries, p)
	}
	ns.mu.Unlock()

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
