package master

// import (
// 	"errors"
// 	"fmt"
// 	"reflect"
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"testing"
// 	"time"
// )

// // ---------------------------------------------------------------------------
// // helpers
// // ---------------------------------------------------------------------------

// // namespace with the root dir already present
// func ntNew() *namespace {
// 	ns := newNamespace()
// 	ns.entries["/"] = &node{dir: true}
// 	return ns
// }

// func ntExec(tx *transaction) {
// 	for _, fn := range tx.exec {
// 		fn()
// 	}
// }

// // real create, exec run; parents are created implicitly
// func ntCreate(t *testing.T, ns *namespace, paths ...string) {
// 	t.Helper()
// 	for _, p := range paths {
// 		tx := newTransaction()
// 		if err := ns.create(tx, p); err != nil {
// 			t.Fatalf("setup create %s: %v", p, err)
// 		}
// 		ntExec(tx)
// 	}
// }

// // empty dirs, parents must already exist
// func ntDir(ns *namespace, paths ...string) {
// 	for _, p := range paths {
// 		ns.entries[p] = &node{dir: true}
// 	}
// }

// func ntKinds(ns *namespace) map[string]string {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	out := map[string]string{}
// 	for p, n := range ns.entries {
// 		if n.dir {
// 			out[p] = "dir"
// 		} else {
// 			out[p] = "file"
// 		}
// 	}
// 	return out
// }

// func ntNodes(ns *namespace) map[string]*node {
// 	ns.mu.RLock()
// 	defer ns.mu.RUnlock()
// 	out := map[string]*node{}
// 	for p, n := range ns.entries {
// 		out[p] = n
// 	}
// 	return out
// }

// // K("/", "dir", "/a", "file") -> map
// func ntK(pairs ...string) map[string]string {
// 	out := map[string]string{}
// 	for i := 0; i+1 < len(pairs); i += 2 {
// 		out[pairs[i]] = pairs[i+1]
// 	}
// 	return out
// }

// func ntAssertKinds(t *testing.T, label string, want, got map[string]string) {
// 	t.Helper()
// 	if !reflect.DeepEqual(want, got) {
// 		t.Fatalf("%s:\nwant %v\ngot  %v", label, want, got)
// 	}
// }

// func ntHeld(ns *namespace) map[string]*lock {
// 	ns.lm.Lock()
// 	defer ns.lm.Unlock()
// 	out := make(map[string]*lock, len(ns.lm.locks))
// 	for p, l := range ns.lm.locks {
// 		out[p] = l
// 	}
// 	return out
// }

// func ntNoLocks(t *testing.T, ns *namespace) {
// 	t.Helper()
// 	held := ntHeld(ns)
// 	if len(held) != 0 {
// 		paths := []string{}
// 		for p := range held {
// 			paths = append(paths, p)
// 		}
// 		sort.Strings(paths)
// 		t.Fatalf("locks still held: %v", paths)
// 	}
// }

// // probes with TryLock and TryRLock; every successful probe is released immediately
// func ntProbe(l *lock) string {
// 	canW := l.TryLock()
// 	if canW {
// 		l.Unlock()
// 	}
// 	canR := l.TryRLock()
// 	if canR {
// 		l.RUnlock()
// 	}
// 	switch {
// 	case canW && canR:
// 		return "none"
// 	case !canW && canR:
// 		return "read"
// 	case !canW && !canR:
// 		return "write"
// 	default:
// 		return "invalid"
// 	}
// }

// func ntKind(err error) string {
// 	switch {
// 	case err == nil:
// 		return "nil"
// 	case errors.As(err, new(*ErrInvalidPath)):
// 		return "invalid"
// 	case errors.As(err, new(*ErrObjectExist)):
// 		return "exists"
// 	case errors.As(err, new(*ErrNotFound)):
// 		return "notfound"
// 	case errors.As(err, new(*ErrIsFile)):
// 		return "isfile"
// 	default:
// 		return "other"
// 	}
// }

// func ntOp(typ operationType, args ...string) string {
// 	return fmt.Sprintf("%d %v", typ, args)
// }

// func ntOps(tx *transaction) []string {
// 	out := []string{}
// 	for _, o := range tx.operations {
// 		out = append(out, ntOp(o.t, o.args...))
// 	}
// 	return out
// }

// // fails the test instead of hanging when fn deadlocks; fn must not call t.Fatal
// func ntTimeout(t *testing.T, d time.Duration, fn func()) {
// 	t.Helper()
// 	done := make(chan struct{})
// 	go func() {
// 		defer close(done)
// 		fn()
// 	}()
// 	select {
// 	case <-done:
// 	case <-time.After(d):
// 		t.Fatal("timed out, possible deadlock")
// 	}
// }

// type ntLR struct {
// 	path string
// 	read bool
// }

// func ntReqs(in []ntLR) []*lockReq {
// 	out := make([]*lockReq, 0, len(in))
// 	for _, r := range in {
// 		out = append(out, &lockReq{path: r.path, read: r.read})
// 	}
// 	return out
// }

// // batch of 5 with reads and writes, same and different depths, overlapping ancestors
// var ntFiveBatch = []ntLR{
// 	{"/a/b/c", false},
// 	{"/a/b/d", true},
// 	{"/a/e", false},
// 	{"/a/f/g/h", true},
// 	{"/a/f/i", false},
// }

// // ---------------------------------------------------------------------------
// // validatePath
// // ---------------------------------------------------------------------------

// func TestNamespace_ValidatePath(t *testing.T) {

// 	ns := newNamespace()

// 	cases := []struct {
// 		name string
// 		path string
// 		want bool
// 	}{
// 		{"zero length", "", false},
// 		{"no leading slash", "a/b", false},
// 		{"dot in non-last segment", "/a/b.c/d", false},
// 		{"dot in last segment", "/a/b/c.d", true},
// 		{"plain", "/a/b/c", true},
// 		{"digits dash dot", "/a/b9/c-7.d", true},
// 		{"root", "/", false},
// 		{"double slash", "//", false},
// 		{"empty segment", "/a//b", false},
// 		{"disallowed char space", "/a/b c", false},
// 		{"disallowed char star", "/a/b*", false},
// 		{"longer than 256", "/" + strings.Repeat("a", 256), false},
// 		{"exactly 256", "/" + strings.Repeat("a", 255), true},
// 	}

// 	for _, tc := range cases {
// 		if got := ns.validatePath(tc.path); got != tc.want {
// 			t.Errorf("%s (%q): want %v, got %v", tc.name, tc.path, tc.want, got)
// 		}
// 	}
// }

// // ---------------------------------------------------------------------------
// // expand
// // ---------------------------------------------------------------------------

// func TestNamespace_Expand(t *testing.T) {

// 	ns := newNamespace()

// 	cases := []struct {
// 		name string
// 		in   []ntLR
// 		want []ntLR
// 	}{
// 		{
// 			name: "batch of 5, mixed depths and flags",
// 			in:   ntFiveBatch,
// 			want: []ntLR{
// 				{"/", true},
// 				{"/a", true},
// 				{"/a/b", true},
// 				{"/a/e", false},
// 				{"/a/f", true},
// 				{"/a/b/c", false},
// 				{"/a/b/d", true},
// 				{"/a/f/g", true},
// 				{"/a/f/i", false},
// 				{"/a/f/g/h", true},
// 			},
// 		},
// 		{
// 			name: "requested ancestors keep their own flag",
// 			in: []ntLR{
// 				{"/a", false},
// 				{"/a/b", true},
// 				{"/a/c/d", false},
// 			},
// 			want: []ntLR{
// 				{"/", true},
// 				{"/a", false},
// 				{"/a/b", true},
// 				{"/a/c", true},
// 				{"/a/c/d", false},
// 			},
// 		},
// 		{
// 			name: "same depth ties ordered lexicographically",
// 			in: []ntLR{
// 				{"/z/y", true},
// 				{"/b/c", false},
// 				{"/b/a", false},
// 				{"/m", true},
// 				{"/z/x", false},
// 			},
// 			want: []ntLR{
// 				{"/", true},
// 				{"/b", true},
// 				{"/m", true},
// 				{"/z", true},
// 				{"/b/a", false},
// 				{"/b/c", false},
// 				{"/z/x", false},
// 				{"/z/y", true},
// 			},
// 		},
// 	}

// 	for _, tc := range cases {
// 		t.Run(tc.name, func(t *testing.T) {
// 			got := []ntLR{}
// 			for _, r := range ns.expand(ntReqs(tc.in)) {
// 				got = append(got, ntLR{r.path, r.read})
// 			}
// 			if !reflect.DeepEqual(got, tc.want) {
// 				t.Fatalf("want %v\ngot  %v", tc.want, got)
// 			}
// 		})
// 	}
// }

// // ---------------------------------------------------------------------------
// // acquireLocks
// // ---------------------------------------------------------------------------

// func TestNamespace_AcquireLocks(t *testing.T) {

// 	cases := []struct {
// 		name string
// 		in   []ntLR
// 	}{
// 		{"write leaf", []ntLR{{"/a/b/c", false}}},
// 		{"read leaf", []ntLR{{"/a/b/c", true}}},
// 		{"batch of 5", ntFiveBatch},
// 	}

// 	for _, tc := range cases {
// 		t.Run(tc.name, func(t *testing.T) {

// 			ns := newNamespace()
// 			reqs := ns.expand(ntReqs(tc.in))

// 			expected := map[string]string{}
// 			for _, r := range reqs {
// 				if r.read {
// 					expected[r.path] = "read"
// 				} else {
// 					expected[r.path] = "write"
// 				}
// 			}

// 			release, err := ns.acquireLocks(reqs)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			// probe every lock the manager holds, not just the requested paths
// 			held := ntHeld(ns)
// 			for p, l := range held {
// 				want, ok := expected[p]
// 				if !ok {
// 					want = "none"
// 				}
// 				if got := ntProbe(l); got != want {
// 					t.Errorf("%s: want %s, got %s", p, want, got)
// 				}
// 			}
// 			for p := range expected {
// 				if _, ok := held[p]; !ok {
// 					t.Errorf("%s was requested but is not held", p)
// 				}
// 			}

// 			release()

// 			for p, l := range held {
// 				if got := ntProbe(l); got != "none" {
// 					t.Errorf("after release %s: want none, got %s", p, got)
// 				}
// 			}
// 			ntNoLocks(t, ns)
// 		})
// 	}
// }

// // ---------------------------------------------------------------------------
// // mkdir
// // ---------------------------------------------------------------------------

// func TestNamespace_Mkdir(t *testing.T) {

// 	t.Run("node is a file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		tx := newTransaction()
// 		if got := ntKind(ns.mkdir(tx, "/f")); got != "invalid" {
// 			t.Fatalf("want invalid, got %s", got)
// 		}
// 		if len(tx.operations) != 0 || len(tx.exec) != 0 {
// 			t.Fatalf("nothing should be staged, got %v", ntOps(tx))
// 		}
// 	})

// 	t.Run("node is a dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f")
// 		tx := newTransaction()
// 		if err := ns.mkdir(tx, "/d"); err != nil {
// 			t.Fatal(err)
// 		}
// 		if len(tx.operations) != 0 || len(tx.exec) != 0 {
// 			t.Fatalf("nothing should be staged, got %v", ntOps(tx))
// 		}
// 	})
// }

// // ---------------------------------------------------------------------------
// // create
// // ---------------------------------------------------------------------------

// func ntCreateErr(t *testing.T, ns *namespace, path, want string) {
// 	t.Helper()
// 	before := ntKinds(ns)
// 	tx := newTransaction()
// 	err := ns.create(tx, path)
// 	if got := ntKind(err); got != want {
// 		t.Fatalf("create %s: want %s, got %s (%v)", path, want, got, err)
// 	}
// 	tx.rollback()
// 	ntNoLocks(t, ns)
// 	ntAssertKinds(t, "entries after failed create", before, ntKinds(ns))
// }

// func TestNamespace_Create(t *testing.T) {

// 	t.Run("create root fails", func(t *testing.T) {
// 		ns := newNamespace()
// 		ntCreateErr(t, ns, "/", "invalid")
// 	})

// 	t.Run("create /file succeeds", func(t *testing.T) {
// 		ns := newNamespace()
// 		tx := newTransaction()
// 		if err := ns.create(tx, "/file"); err != nil {
// 			t.Fatal(err)
// 		}
// 		wantOps := []string{ntOp(CREATE_DIR, "/"), ntOp(CREATE_FILE, "/file")}
// 		if got := ntOps(tx); !reflect.DeepEqual(got, wantOps) {
// 			t.Fatalf("ops: want %v, got %v", wantOps, got)
// 		}
// 		ntExec(tx)
// 		ntNoLocks(t, ns)
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/file", "file"), ntKinds(ns))
// 	})

// 	t.Run("intermediate node is a file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		ntCreateErr(t, ns, "/a/b", "invalid")
// 	})

// 	t.Run("last node is a dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f")
// 		ntCreateErr(t, ns, "/d", "exists")
// 	})

// 	t.Run("create on existing file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntCreateErr(t, ns, "/f", "exists")
// 	})

// 	t.Run("concurrent creates, same ancestors, different files", func(t *testing.T) {
// 		ns := ntNew()
// 		const n = 8
// 		errs := make([]error, n)

// 		ntTimeout(t, 10*time.Second, func() {
// 			var wg sync.WaitGroup
// 			for i := 0; i < n; i++ {
// 				wg.Add(1)
// 				go func(i int) {
// 					defer wg.Done()
// 					tx := newTransaction()
// 					if err := ns.create(tx, fmt.Sprintf("/a/b/f%d", i)); err != nil {
// 						errs[i] = err
// 						tx.rollback()
// 						return
// 					}
// 					ntExec(tx)
// 				}(i)
// 			}
// 			wg.Wait()
// 		})

// 		want := ntK("/", "dir", "/a", "dir", "/a/b", "dir")
// 		for i := 0; i < n; i++ {
// 			if errs[i] != nil {
// 				t.Fatalf("create %d: %v", i, errs[i])
// 			}
// 			want[fmt.Sprintf("/a/b/f%d", i)] = "file"
// 		}
// 		ntAssertKinds(t, "entries", want, ntKinds(ns))
// 		ntNoLocks(t, ns)
// 	})

// 	t.Run("concurrent creates, same ancestors, same file", func(t *testing.T) {
// 		ns := ntNew()
// 		const n = 8
// 		errs := make([]error, n)

// 		ntTimeout(t, 10*time.Second, func() {
// 			var wg sync.WaitGroup
// 			for i := 0; i < n; i++ {
// 				wg.Add(1)
// 				go func(i int) {
// 					defer wg.Done()
// 					tx := newTransaction()
// 					if err := ns.create(tx, "/a/b/f"); err != nil {
// 						errs[i] = err
// 						tx.rollback()
// 						return
// 					}
// 					ntExec(tx)
// 				}(i)
// 			}
// 			wg.Wait()
// 		})

// 		wins, exists := 0, 0
// 		for i, err := range errs {
// 			switch ntKind(err) {
// 			case "nil":
// 				wins++
// 			case "exists":
// 				exists++
// 			default:
// 				t.Fatalf("create %d: unexpected %v", i, err)
// 			}
// 		}
// 		if wins != 1 || exists != n-1 {
// 			t.Fatalf("want 1 success and %d exists, got %d and %d", n-1, wins, exists)
// 		}
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/a", "dir", "/a/b", "dir", "/a/b/f", "file"), ntKinds(ns))
// 		ntNoLocks(t, ns)
// 	})

// 	t.Run("rollback releases every lock", func(t *testing.T) {
// 		ns := ntNew()
// 		before := ntKinds(ns)

// 		tx := newTransaction()
// 		if err := ns.create(tx, "/a/b/f"); err != nil {
// 			t.Fatal(err)
// 		}
// 		tx.rollback()

// 		ntNoLocks(t, ns)
// 		ntAssertKinds(t, "entries after rollback", before, ntKinds(ns))

// 		// the write lock must be free again
// 		ntTimeout(t, 10*time.Second, func() {
// 			tx := newTransaction()
// 			if err := ns.create(tx, "/a/b/f"); err != nil {
// 				t.Errorf("create after rollback: %v", err)
// 				return
// 			}
// 			ntExec(tx)
// 		})
// 		ntNoLocks(t, ns)
// 	})
// }

// // ---------------------------------------------------------------------------
// // lookup
// // ---------------------------------------------------------------------------

// func ntLookupOK(t *testing.T, ns *namespace, path string) *node {
// 	t.Helper()
// 	tx := newTransaction()
// 	n, err := ns.lookup(tx, path)
// 	if err != nil {
// 		t.Fatalf("lookup %s: %v", path, err)
// 	}
// 	ntExec(tx)
// 	ntNoLocks(t, ns)
// 	return n
// }

// func ntConcurrentLookups(t *testing.T, ns *namespace, paths []string) {
// 	t.Helper()

// 	n := len(paths)
// 	errs := make([]error, n)
// 	nodes := make([]*node, n)

// 	var acquired sync.WaitGroup
// 	acquired.Add(n)

// 	ntTimeout(t, 10*time.Second, func() {
// 		var wg sync.WaitGroup
// 		for i, p := range paths {
// 			wg.Add(1)
// 			go func(i int, p string) {
// 				defer wg.Done()
// 				tx := newTransaction()
// 				nodes[i], errs[i] = ns.lookup(tx, p)
// 				// every lookup holds its locks at the same time
// 				acquired.Done()
// 				acquired.Wait()
// 				ntExec(tx)
// 			}(i, p)
// 		}
// 		wg.Wait()
// 	})

// 	for i, p := range paths {
// 		if errs[i] != nil {
// 			t.Fatalf("lookup %s: %v", p, errs[i])
// 		}
// 		if nodes[i] != ns.entries[p] {
// 			t.Fatalf("lookup %s returned the wrong node", p)
// 		}
// 	}
// 	ntNoLocks(t, ns)
// }

// func TestNamespace_Lookup(t *testing.T) {

// 	t.Run("non existent node", func(t *testing.T) {
// 		ns := ntNew()
// 		tx := newTransaction()
// 		n, err := ns.lookup(tx, "/nope")
// 		if got := ntKind(err); got != "notfound" || n != nil {
// 			t.Fatalf("want notfound and nil node, got %s and %v", got, n)
// 		}
// 		tx.rollback()
// 		ntNoLocks(t, ns)
// 	})

// 	t.Run("dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f")
// 		n := ntLookupOK(t, ns, "/d")
// 		if n != ns.entries["/d"] || !n.dir {
// 			t.Fatalf("expected the /d dir node, got %+v", n)
// 		}
// 	})

// 	t.Run("file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f")
// 		n := ntLookupOK(t, ns, "/d/f")
// 		if n != ns.entries["/d/f"] || n.dir {
// 			t.Fatalf("expected the /d/f file node, got %+v", n)
// 		}
// 	})

// 	t.Run("intermediate node is a file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		tx := newTransaction()
// 		n, err := ns.lookup(tx, "/a/b")
// 		if got := ntKind(err); got != "notfound" || n != nil {
// 			t.Fatalf("want notfound and nil node, got %s and %v", got, n)
// 		}
// 		tx.rollback()
// 		ntNoLocks(t, ns)
// 	})

// 	t.Run("concurrent lookups on the same node", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f")
// 		ntConcurrentLookups(t, ns, []string{"/d/f", "/d/f", "/d/f", "/d/f", "/d/f", "/d/f"})
// 	})

// 	t.Run("concurrent lookups on nodes with same ancestors", func(t *testing.T) {
// 		ns := ntNew()
// 		paths := []string{}
// 		for i := 0; i < 6; i++ {
// 			p := fmt.Sprintf("/d/sub/f%d", i)
// 			ntCreate(t, ns, p)
// 			paths = append(paths, p)
// 		}
// 		ntConcurrentLookups(t, ns, paths)
// 	})

// 	t.Run("locks released after exec and after a failed lookup", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")

// 		tx := newTransaction()
// 		if _, err := ns.lookup(tx, "/f"); err != nil {
// 			t.Fatal(err)
// 		}
// 		ntExec(tx)
// 		ntNoLocks(t, ns)

// 		// the write lock on the node must be free
// 		ntTimeout(t, 10*time.Second, func() {
// 			release, err := ns.acquireLocks(ns.expand([]*lockReq{{path: "/f", read: false}}))
// 			if err != nil {
// 				t.Errorf("acquire: %v", err)
// 				return
// 			}
// 			release()
// 		})

// 		tx = newTransaction()
// 		if _, err := ns.lookup(tx, "/missing"); ntKind(err) != "notfound" {
// 			t.Fatalf("want notfound, got %v", err)
// 		}
// 		tx.rollback()
// 		ntNoLocks(t, ns)
// 	})
// }

// // ---------------------------------------------------------------------------
// // move
// // ---------------------------------------------------------------------------

// func ntMoveErr(t *testing.T, ns *namespace, src, dst, want string) {
// 	t.Helper()
// 	before := ntKinds(ns)
// 	tx := newTransaction()
// 	err := ns.move(tx, src, dst)
// 	if got := ntKind(err); got != want {
// 		t.Fatalf("move %s -> %s: want %s, got %s (%v)", src, dst, want, got, err)
// 	}
// 	tx.rollback()
// 	ntNoLocks(t, ns)
// 	ntAssertKinds(t, "entries after failed move", before, ntKinds(ns))
// }

// func ntMoveOK(t *testing.T, ns *namespace, src, dst string) {
// 	t.Helper()
// 	tx := newTransaction()
// 	if err := ns.move(tx, src, dst); err != nil {
// 		t.Fatalf("move %s -> %s: %v", src, dst, err)
// 	}
// 	ntExec(tx)
// 	ntNoLocks(t, ns)
// }

// func TestNamespace_Move(t *testing.T) {

// 	t.Run("same path", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		ntMoveErr(t, ns, "/a", "/a", "invalid")
// 	})

// 	t.Run("dir into its descendant", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x")
// 		ntMoveErr(t, ns, "/a", "/a/b", "invalid")
// 	})

// 	t.Run("move root", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		ntMoveErr(t, ns, "/", "/x", "invalid")
// 	})

// 	t.Run("move to root", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		ntMoveErr(t, ns, "/a", "/", "invalid")
// 	})

// 	t.Run("/a to /ab is not a descendant move", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/f", "/ac/z")
// 		ntMoveOK(t, ns, "/a", "/ab")
// 		ntAssertKinds(t, "entries", ntK(
// 			"/", "dir",
// 			"/ab", "dir",
// 			"/ab/f", "file",
// 			"/ac", "dir",
// 			"/ac/z", "file",
// 		), ntKinds(ns))
// 	})

// 	t.Run("non existent src", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/other")
// 		ntMoveErr(t, ns, "/nope", "/x", "notfound")
// 	})

// 	t.Run("existing dst, file to file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f1", "/f2")
// 		ntMoveErr(t, ns, "/f1", "/f2", "invalid")
// 	})

// 	t.Run("existing dst, dir into dir that has a same named child", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/b/a/y")
// 		ntMoveErr(t, ns, "/a", "/b", "invalid")
// 	})

// 	t.Run("dir to file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/f")
// 		ntMoveErr(t, ns, "/a", "/f", "invalid")
// 	})

// 	t.Run("dir to existing dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/a/sub/y", "/b/z")
// 		ntMoveOK(t, ns, "/a", "/b")
// 		ntAssertKinds(t, "entries", ntK(
// 			"/", "dir",
// 			"/b", "dir",
// 			"/b/z", "file",
// 			"/b/a", "dir",
// 			"/b/a/x", "file",
// 			"/b/a/sub", "dir",
// 			"/b/a/sub/y", "file",
// 		), ntKinds(ns))
// 	})

// 	t.Run("dir to new path", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/a/sub/y", "/b/z")
// 		ntMoveOK(t, ns, "/a", "/c")
// 		ntAssertKinds(t, "entries", ntK(
// 			"/", "dir",
// 			"/b", "dir",
// 			"/b/z", "file",
// 			"/c", "dir",
// 			"/c/x", "file",
// 			"/c/sub", "dir",
// 			"/c/sub/y", "file",
// 		), ntKinds(ns))
// 	})

// 	t.Run("file to dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f", "/d/x")
// 		ntMoveOK(t, ns, "/f", "/d")
// 		ntAssertKinds(t, "entries", ntK(
// 			"/", "dir",
// 			"/d", "dir",
// 			"/d/x", "file",
// 			"/d/f", "file",
// 		), ntKinds(ns))
// 	})

// 	t.Run("file to new path", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntMoveOK(t, ns, "/f", "/g")
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/g", "file"), ntKinds(ns))
// 	})

// 	t.Run("concurrent move /a /b and move /b /a", func(t *testing.T) {
// 		for iter := 0; iter < 50; iter++ {

// 			ns := ntNew()
// 			ntCreate(t, ns, "/a")

// 			var errA, errB error
// 			ntTimeout(t, 10*time.Second, func() {
// 				var wg sync.WaitGroup
// 				wg.Add(2)
// 				run := func(src, dst string, out *error) {
// 					defer wg.Done()
// 					tx := newTransaction()
// 					if err := ns.move(tx, src, dst); err != nil {
// 						*out = err
// 						tx.rollback()
// 						return
// 					}
// 					ntExec(tx)
// 				}
// 				go run("/a", "/b", &errA)
// 				go run("/b", "/a", &errB)
// 				wg.Wait()
// 			})

// 			if errA != nil {
// 				t.Fatalf("iteration %d: move /a /b: %v", iter, errA)
// 			}
// 			switch ntKind(errB) {
// 			case "nil":
// 				// /a -> /b ran first, then /b -> /a
// 				ntAssertKinds(t, "entries", ntK("/", "dir", "/a", "file"), ntKinds(ns))
// 			case "invalid":
// 				// /b -> /a ran first and found /a occupied
// 				ntAssertKinds(t, "entries", ntK("/", "dir", "/b", "file"), ntKinds(ns))
// 			default:
// 				t.Fatalf("iteration %d: move /b /a: unexpected %v", iter, errB)
// 			}
// 			ntNoLocks(t, ns)
// 		}
// 	})

// 	t.Run("rollback of a staged move releases every lock", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		before := ntKinds(ns)

// 		tx := newTransaction()
// 		if err := ns.move(tx, "/f", "/g"); err != nil {
// 			t.Fatal(err)
// 		}
// 		tx.rollback()

// 		ntNoLocks(t, ns)
// 		ntAssertKinds(t, "entries after rollback", before, ntKinds(ns))
// 	})
// }

// // ---------------------------------------------------------------------------
// // snapshot
// // ---------------------------------------------------------------------------

// func ntSnapErr(t *testing.T, ns *namespace, src, dst, want string) {
// 	t.Helper()
// 	before := ntKinds(ns)
// 	tx := newTransaction()
// 	nodes, err := ns.snapshot(tx, src, dst)
// 	if got := ntKind(err); got != want {
// 		t.Fatalf("snapshot %s -> %s: want %s, got %s (%v)", src, dst, want, got, err)
// 	}
// 	if nodes != nil {
// 		t.Fatalf("snapshot %s -> %s: expected no nodes on error, got %d", src, dst, len(nodes))
// 	}
// 	tx.rollback()
// 	ntNoLocks(t, ns)
// 	ntAssertKinds(t, "entries after failed snapshot", before, ntKinds(ns))
// }

// func ntSnapOK(t *testing.T, ns *namespace, src, dst string) []*node {
// 	t.Helper()
// 	tx := newTransaction()
// 	nodes, err := ns.snapshot(tx, src, dst)
// 	if err != nil {
// 		t.Fatalf("snapshot %s -> %s: %v", src, dst, err)
// 	}
// 	ntExec(tx)
// 	ntNoLocks(t, ns)
// 	return nodes
// }

// func TestNamespace_Snapshot(t *testing.T) {

// 	t.Run("same path", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntSnapErr(t, ns, "/f", "/f", "invalid")
// 	})

// 	t.Run("root to /rootSnap", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntSnapErr(t, ns, "/", "/rootSnap", "invalid")
// 	})

// 	t.Run("src does not exist", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/other")
// 		ntSnapErr(t, ns, "/nope", "/x", "notfound")
// 	})

// 	t.Run("file to non existent file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ns.entries["/f"].chunkHandles = []uint64{1, 2, 3}

// 		nodes := ntSnapOK(t, ns, "/f", "/g")

// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/f", "file", "/g", "file"), ntKinds(ns))
// 		if !reflect.DeepEqual(ns.entries["/g"].chunkHandles, []uint64{1, 2, 3}) {
// 			t.Fatalf("dst handles: got %v", ns.entries["/g"].chunkHandles)
// 		}
// 		if ns.entries["/g"] == ns.entries["/f"] {
// 			t.Fatal("dst must be a distinct node")
// 		}
// 		if len(nodes) != 1 || nodes[0] != ns.entries["/f"] {
// 			t.Fatalf("expected the src node to be returned, got %v", nodes)
// 		}
// 	})

// 	t.Run("file to existing file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f", "/g")
// 		ntSnapErr(t, ns, "/f", "/g", "exists")
// 	})

// 	t.Run("file to non existent dir name", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntSnapOK(t, ns, "/f", "/newdir")
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/f", "file", "/newdir", "file"), ntKinds(ns))
// 	})

// 	t.Run("dst parent does not exist", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntSnapErr(t, ns, "/f", "/nodir/g", "notfound")
// 	})

// 	t.Run("file to existing dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f", "/d/x")
// 		ntSnapErr(t, ns, "/f", "/d", "exists")
// 	})

// 	t.Run("dir to file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/f")
// 		ntSnapErr(t, ns, "/a", "/f", "exists")
// 	})

// 	t.Run("dir to non existent dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/a/sub/y")
// 		nodes := ntSnapOK(t, ns, "/a", "/c")
// 		ntAssertKinds(t, "entries", ntK(
// 			"/", "dir",
// 			"/a", "dir",
// 			"/a/x", "file",
// 			"/a/sub", "dir",
// 			"/a/sub/y", "file",
// 			"/c", "dir",
// 			"/c/x", "file",
// 			"/c/sub", "dir",
// 			"/c/sub/y", "file",
// 		), ntKinds(ns))
// 		if len(nodes) != 4 {
// 			t.Fatalf("expected 4 src nodes, got %d", len(nodes))
// 		}
// 	})

// 	t.Run("dir to existing dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x", "/b/z")
// 		ntSnapErr(t, ns, "/a", "/b", "exists")
// 	})

// 	t.Run("/a to /ab is not a descendant snapshot", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/f", "/ac/z")
// 		nodes := ntSnapOK(t, ns, "/a", "/ab")
// 		ntAssertKinds(t, "entries", ntK(
// 			"/", "dir",
// 			"/a", "dir",
// 			"/a/f", "file",
// 			"/ab", "dir",
// 			"/ab/f", "file",
// 			"/ac", "dir",
// 			"/ac/z", "file",
// 		), ntKinds(ns))
// 		if len(nodes) != 2 {
// 			t.Fatalf("expected 2 src nodes, got %d", len(nodes))
// 		}
// 	})

// 	t.Run("empty dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntDir(ns, "/e")
// 		ntSnapOK(t, ns, "/e", "/e2")
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/e", "dir", "/e2", "dir"), ntKinds(ns))
// 	})

// 	t.Run("empty file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		ntSnapOK(t, ns, "/f", "/g")
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/f", "file", "/g", "file"), ntKinds(ns))
// 		if len(ns.entries["/g"].chunkHandles) != 0 {
// 			t.Fatalf("expected no handles, got %v", ns.entries["/g"].chunkHandles)
// 		}
// 	})

// 	t.Run("intermediate node of src is not a dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		ntSnapErr(t, ns, "/a/b", "/x", "invalid")
// 	})

// 	t.Run("intermediate node of dst is not a dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f", "/a")
// 		ntSnapErr(t, ns, "/f", "/a/x", "invalid")
// 	})

// 	t.Run("concurrent snapshot /a/b /c/d and /c/d /a/b", func(t *testing.T) {
// 		for iter := 0; iter < 50; iter++ {

// 			ns := ntNew()
// 			ntCreate(t, ns, "/a/b", "/c/keep")

// 			var err1, err2 error
// 			ntTimeout(t, 10*time.Second, func() {
// 				var wg sync.WaitGroup
// 				wg.Add(2)
// 				run := func(src, dst string, out *error) {
// 					defer wg.Done()
// 					tx := newTransaction()
// 					if _, err := ns.snapshot(tx, src, dst); err != nil {
// 						*out = err
// 						tx.rollback()
// 						return
// 					}
// 					ntExec(tx)
// 				}
// 				go run("/a/b", "/c/d", &err1)
// 				go run("/c/d", "/a/b", &err2)
// 				wg.Wait()
// 			})

// 			if err1 != nil {
// 				t.Fatalf("iteration %d: snapshot /a/b /c/d: %v", iter, err1)
// 			}
// 			// /c/d did not exist yet (notfound) or the first snapshot already ran (exists)
// 			if k := ntKind(err2); k != "notfound" && k != "exists" {
// 				t.Fatalf("iteration %d: snapshot /c/d /a/b: unexpected %v", iter, err2)
// 			}
// 			ntAssertKinds(t, "entries", ntK(
// 				"/", "dir",
// 				"/a", "dir",
// 				"/a/b", "file",
// 				"/c", "dir",
// 				"/c/keep", "file",
// 				"/c/d", "file",
// 			), ntKinds(ns))
// 			ntNoLocks(t, ns)
// 		}
// 	})

// 	t.Run("rollback of a staged snapshot releases every lock", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a/x")
// 		before := ntKinds(ns)

// 		tx := newTransaction()
// 		if _, err := ns.snapshot(tx, "/a", "/b"); err != nil {
// 			t.Fatal(err)
// 		}
// 		tx.rollback()

// 		ntNoLocks(t, ns)
// 		ntAssertKinds(t, "entries after rollback", before, ntKinds(ns))
// 	})
// }

// // ---------------------------------------------------------------------------
// // list
// // ---------------------------------------------------------------------------

// func ntList(t *testing.T, ns *namespace, path string) ([]string, error) {
// 	t.Helper()
// 	got, err := ns.list(path)
// 	// list must release its locks on every path
// 	ntNoLocks(t, ns)
// 	sort.Strings(got)
// 	return got, err
// }

// func TestNamespace_List(t *testing.T) {

// 	t.Run("non existent path", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/other")
// 		got, err := ntList(t, ns, "/nope")
// 		if err != nil || len(got) != 0 {
// 			t.Fatalf("want empty and nil, got %v and %v", got, err)
// 		}
// 	})

// 	t.Run("intermediate node is a file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/a")
// 		_, err := ntList(t, ns, "/a/b")
// 		if got := ntKind(err); got != "invalid" {
// 			t.Fatalf("want invalid, got %s (%v)", got, err)
// 		}
// 	})

// 	t.Run("success case", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f1", "/d/f2", "/d/sub/f3", "/d2/x", "/other")
// 		got, err := ntList(t, ns, "/d")
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		want := []string{"/d/f1", "/d/f2", "/d/sub/f3"}
// 		if !reflect.DeepEqual(got, want) {
// 			t.Fatalf("want %v, got %v", want, got)
// 		}
// 	})

// 	t.Run("concurrent list requests", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f1", "/d/f2", "/d/sub/f3")
// 		want := []string{"/d/f1", "/d/f2", "/d/sub/f3"}

// 		const n = 8
// 		results := make([][]string, n)
// 		errs := make([]error, n)

// 		ntTimeout(t, 10*time.Second, func() {
// 			var wg sync.WaitGroup
// 			for i := 0; i < n; i++ {
// 				wg.Add(1)
// 				go func(i int) {
// 					defer wg.Done()
// 					results[i], errs[i] = ns.list("/d")
// 				}(i)
// 			}
// 			wg.Wait()
// 		})

// 		for i := 0; i < n; i++ {
// 			if errs[i] != nil {
// 				t.Fatalf("list %d: %v", i, errs[i])
// 			}
// 			sort.Strings(results[i])
// 			if !reflect.DeepEqual(results[i], want) {
// 				t.Fatalf("list %d: want %v, got %v", i, want, results[i])
// 			}
// 		}
// 		ntNoLocks(t, ns)
// 	})

// 	t.Run("list a file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		got, err := ntList(t, ns, "/f")
// 		if err != nil || !reflect.DeepEqual(got, []string{"/f"}) {
// 			t.Fatalf("want [/f] and nil, got %v and %v", got, err)
// 		}
// 	})

// 	t.Run("list an empty dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntDir(ns, "/e")
// 		got, err := ntList(t, ns, "/e")
// 		if err != nil || len(got) != 0 {
// 			t.Fatalf("want empty and nil, got %v and %v", got, err)
// 		}
// 	})
// }

// // ---------------------------------------------------------------------------
// // delete
// // ---------------------------------------------------------------------------

// // every staged REMOVE must have landed in deleted under path/time with the original node
// func ntCheckDeleted(t *testing.T, ns *namespace, tx *transaction, orig map[string]*node) {
// 	t.Helper()

// 	ns.deletedMu.Lock()
// 	defer ns.deletedMu.Unlock()

// 	if len(ns.deleted) != len(tx.operations) {
// 		t.Fatalf("want %d deleted entries, got %d", len(tx.operations), len(ns.deleted))
// 	}
// 	for _, o := range tx.operations {
// 		if o.t != REMOVE {
// 			t.Fatalf("expected only REMOVE ops, got %d", o.t)
// 		}
// 		key := o.args[0] + "/" + o.args[1]
// 		d, ok := ns.deleted[key]
// 		if !ok {
// 			t.Fatalf("missing deleted entry %s", key)
// 		}
// 		ts, err := strconv.ParseInt(o.args[1], 10, 64)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if d.deletedAt != ts {
// 			t.Fatalf("%s: deletedAt want %d, got %d", key, ts, d.deletedAt)
// 		}
// 		if d.node != orig[o.args[0]] {
// 			t.Fatalf("%s: deleted node is not the original node", key)
// 		}
// 	}
// }

// func ntDelete(t *testing.T, ns *namespace, path string) *transaction {
// 	t.Helper()
// 	tx := newTransaction()
// 	if err := ns.delete(tx, path); err != nil {
// 		t.Fatalf("delete %s: %v", path, err)
// 	}
// 	ntExec(tx)
// 	ntNoLocks(t, ns)
// 	return tx
// }

// func TestNamespace_Delete(t *testing.T) {

// 	t.Run("invalid path", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		for _, p := range []string{"/a/b.c/d", "f", ""} {
// 			tx := newTransaction()
// 			if got := ntKind(ns.delete(tx, p)); got != "invalid" {
// 				t.Fatalf("delete %q: want invalid, got %s", p, got)
// 			}
// 			tx.rollback()
// 			ntNoLocks(t, ns)
// 		}
// 	})

// 	t.Run("normal dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f1", "/d/sub/f2", "/keep")
// 		orig := ntNodes(ns)

// 		tx := ntDelete(t, ns, "/d")

// 		wantOps := []string{
// 			ntOp(REMOVE, "/d"), ntOp(REMOVE, "/d/f1"), ntOp(REMOVE, "/d/sub"), ntOp(REMOVE, "/d/sub/f2"),
// 		}
// 		gotOps := []string{}
// 		for _, o := range tx.operations {
// 			gotOps = append(gotOps, ntOp(o.t, o.args[0]))
// 		}
// 		sort.Strings(gotOps)
// 		sort.Strings(wantOps)
// 		if !reflect.DeepEqual(gotOps, wantOps) {
// 			t.Fatalf("ops: want %v, got %v", wantOps, gotOps)
// 		}

// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/keep", "file"), ntKinds(ns))
// 		ntCheckDeleted(t, ns, tx, orig)
// 	})

// 	t.Run("empty dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntDir(ns, "/e")
// 		ntCreate(t, ns, "/keep")
// 		orig := ntNodes(ns)

// 		tx := ntDelete(t, ns, "/e")

// 		if len(tx.operations) != 1 || tx.operations[0].args[0] != "/e" {
// 			t.Fatalf("expected one REMOVE of /e, got %v", ntOps(tx))
// 		}
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/keep", "file"), ntKinds(ns))
// 		ntCheckDeleted(t, ns, tx, orig)
// 	})

// 	t.Run("normal file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f", "/keep")
// 		orig := ntNodes(ns)

// 		tx := ntDelete(t, ns, "/d/f")

// 		if len(tx.operations) != 1 || tx.operations[0].args[0] != "/d/f" {
// 			t.Fatalf("expected one REMOVE of /d/f, got %v", ntOps(tx))
// 		}
// 		ntAssertKinds(t, "entries", ntK("/", "dir", "/d", "dir", "/keep", "file"), ntKinds(ns))
// 		ntCheckDeleted(t, ns, tx, orig)
// 	})

// 	t.Run("delete, create, delete the same node", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		first := ns.entries["/f"]

// 		ntDelete(t, ns, "/f")
// 		time.Sleep(time.Millisecond) // keys carry the delete time, keep the two apart

// 		ntCreate(t, ns, "/f")
// 		second := ns.entries["/f"]
// 		if second == first {
// 			t.Fatal("create after delete must make a new node")
// 		}
// 		time.Sleep(time.Millisecond)

// 		ntDelete(t, ns, "/f")

// 		ntAssertKinds(t, "entries", ntK("/", "dir"), ntKinds(ns))

// 		ns.deletedMu.Lock()
// 		defer ns.deletedMu.Unlock()
// 		if len(ns.deleted) != 2 {
// 			t.Fatalf("want 2 deleted entries, got %d", len(ns.deleted))
// 		}
// 		seen := map[*node]bool{}
// 		for _, d := range ns.deleted {
// 			seen[d.node] = true
// 		}
// 		if !seen[first] || !seen[second] {
// 			t.Fatal("both the first and the second node must be in deleted")
// 		}
// 	})

// 	concurrent := func(t *testing.T, ns *namespace, path string, wantNodes int) {
// 		t.Helper()
// 		orig := ntNodes(ns)

// 		const n = 4
// 		errs := make([]error, n)
// 		txs := make([]*transaction, n)

// 		ntTimeout(t, 10*time.Second, func() {
// 			var wg sync.WaitGroup
// 			for i := 0; i < n; i++ {
// 				wg.Add(1)
// 				go func(i int) {
// 					defer wg.Done()
// 					tx := newTransaction()
// 					txs[i] = tx
// 					if err := ns.delete(tx, path); err != nil {
// 						errs[i] = err
// 						tx.rollback()
// 						return
// 					}
// 					ntExec(tx)
// 				}(i)
// 			}
// 			wg.Wait()
// 		})

// 		total := &transaction{}
// 		for i := 0; i < n; i++ {
// 			if errs[i] != nil {
// 				t.Fatalf("delete %d: %v", i, errs[i])
// 			}
// 			total.operations = append(total.operations, txs[i].operations...)
// 		}
// 		// the nodes are removed exactly once across all the deletes
// 		if len(total.operations) != wantNodes {
// 			t.Fatalf("want %d REMOVE ops in total, got %d", wantNodes, len(total.operations))
// 		}
// 		ntCheckDeleted(t, ns, total, orig)
// 		ntAssertKinds(t, "entries", ntK("/", "dir"), ntKinds(ns))
// 		ntNoLocks(t, ns)
// 	}

// 	t.Run("concurrent deletes on the same dir", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/d/f1", "/d/sub/f2")
// 		concurrent(t, ns, "/d", 4)
// 	})

// 	t.Run("concurrent deletes on the same file", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		concurrent(t, ns, "/f", 1)
// 	})

// 	t.Run("delete root", func(t *testing.T) {
// 		ns := ntNew()
// 		ntCreate(t, ns, "/f")
// 		before := ntKinds(ns)

// 		tx := newTransaction()
// 		if got := ntKind(ns.delete(tx, "/")); got != "invalid" {
// 			t.Fatalf("want invalid, got %s", got)
// 		}
// 		tx.rollback()
// 		ntNoLocks(t, ns)
// 		ntAssertKinds(t, "entries", before, ntKinds(ns))
// 	})
// }