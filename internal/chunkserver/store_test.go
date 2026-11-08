package chunkserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/slothhcommits/Hive-FS/internal"
)

// ---------------------------------------------------------------- helpers

func newTestStore(t *testing.T) *store {
	t.Helper()
	return newStore(t.TempDir())
}

func pseudoData(seed, n int) []byte {
	b := make([]byte, n)
	x := uint32(seed)*2654435761 + 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

func tableFor(data []byte) []uint32 {
	var tbl []uint32
	for off := 0; off < len(data); off += blockSize {
		end := min(off+blockSize, len(data))
		tbl = append(tbl, crc32.Checksum(data[off:end], castagnoli))
	}
	return tbl
}

func encodeTable(tbl []uint32) []byte {
	buf := make([]byte, len(tbl)*4)
	for i, v := range tbl {
		binary.BigEndian.PutUint32(buf[i*4:], v)
	}
	return buf
}

func verBytes(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func rawVersion(t *testing.T, st *store, handle uint64) uint32 {
	t.Helper()
	b := mustRead(t, st.makeVersionPath(handle))
	if len(b) != 4 {
		t.Fatalf("version file must be 4 bytes, got %d", len(b))
	}
	return binary.BigEndian.Uint32(b)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func openRW(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func chunkFileWith(t *testing.T, data []byte) *os.File {
	t.Helper()
	f := openRW(t, filepath.Join(t.TempDir(), "c.chunk"))
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	return f
}

func tablesEqual(t *testing.T, got, want []uint32) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("checksum table mismatch\n got: %v\nwant: %v", got, want)
	}
}

// ------------------------------------------------------------------ store

func TestBuildState(t *testing.T) {
	st := newTestStore(t)
	dir := st.path
	put := func(name string, data []byte) { mustWrite(t, filepath.Join(dir, name), data) }

	// garbage: directory, unknown extensions, non numeric handles
	if err := os.Mkdir(filepath.Join(dir, "junkdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	put("readme.txt", []byte("x"))
	put("notes", []byte("x"))
	put("7.chunk.bak", []byte("x"))
	put("abc.chunk", []byte("x"))
	put("abc.sum", []byte("x"))
	put("abc.ver", []byte("x"))

	// valid triplets
	type valid struct {
		handle  uint64
		data    []byte
		version uint32
	}
	valids := []valid{
		{1, pseudoData(1, 100), 3},
		{2, pseudoData(2, blockSize+10), 7},
		{3, nil, 0},
	}
	for _, v := range valids {
		put(fmt.Sprintf("%d.chunk", v.handle), v.data)
		put(fmt.Sprintf("%d.sum", v.handle), encodeTable(tableFor(v.data)))
		put(fmt.Sprintf("%d.ver", v.handle), verBytes(v.version))
	}

	// incomplete sets
	pair := pseudoData(9, 10)
	put("10.ver", verBytes(1))                                                // ver only
	put("11.chunk", pair)                                                     // chunk only
	put("12.sum", nil)                                                        // sum only
	put("13.chunk", pair)                                                     // chunk + sum, no ver
	put("13.sum", encodeTable(tableFor(pair)))                                //
	put("14.ver", verBytes(1))                                                // ver + sum, no chunk
	put("14.sum", encodeTable(tableFor(pair)))                                //
	put("15.ver", verBytes(1))                                                // ver + chunk, no sum
	put("15.chunk", pair)                                                     //

	state := st.buildState()

	wantFiles := []string{}
	for _, v := range valids {
		for _, ext := range []string{"chunk", "sum", "ver"} {
			wantFiles = append(wantFiles, fmt.Sprintf("%d.%s", v.handle, ext))
		}
	}
	slices.Sort(wantFiles)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	gotFiles := []string{}
	for _, e := range entries {
		gotFiles = append(gotFiles, e.Name())
	}
	if !slices.Equal(gotFiles, wantFiles) {
		t.Fatalf("store contents after buildState\n got: %v\nwant: %v", gotFiles, wantFiles)
	}

	vm := state.versionMap.entries
	cs := state.chunkSet.entries
	if len(vm) != len(valids) || len(cs) != len(valids) {
		t.Fatalf("expected %d entries, got versions=%d chunks=%d", len(valids), len(vm), len(cs))
	}
	for _, v := range valids {
		if got, ok := vm[v.handle]; !ok || got != v.version {
			t.Errorf("handle %d: version = %d (present=%v), want %d", v.handle, got, ok, v.version)
		}
		c, ok := cs[v.handle]
		if !ok {
			t.Errorf("handle %d missing from chunk set", v.handle)
			continue
		}
		if c.handle != v.handle || c.checksums.handle != v.handle {
			t.Errorf("handle %d: chunk carries wrong handle", v.handle)
		}
		tablesEqual(t, c.checksums.table, tableFor(v.data))
	}
}

func TestMakeNewChunkFiles(t *testing.T) {

	t.Run("non existent handle", func(t *testing.T) {
		st := newTestStore(t)
		h := uint64(42)
		if err := st.makeNewChunkFiles(h); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{st.makeChunkPath(h), st.makeChecksumPath(h), st.makeVersionPath(h)} {
			if !exists(p) {
				t.Fatalf("%s was not created", p)
			}
		}
		if len(mustRead(t, st.makeChunkPath(h))) != 0 || len(mustRead(t, st.makeChecksumPath(h))) != 0 {
			t.Fatal("chunk and checksum files must start empty")
		}
		if v := rawVersion(t, st, h); v != 0 {
			t.Fatalf("initial version = %d, want 0", v)
		}
	})

	t.Run("existing handle", func(t *testing.T) {
		st := newTestStore(t)
		h := uint64(42)
		if err := st.makeNewChunkFiles(h); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, st.makeChunkPath(h), []byte("chunk data"))
		mustWrite(t, st.makeChecksumPath(h), []byte("sum"))
		if err := st.logVersion(h, 9); err != nil {
			t.Fatal(err)
		}

		err := st.makeNewChunkFiles(h)
		if !errors.Is(err, os.ErrExist) {
			t.Fatalf("expected os.ErrExist, got %v", err)
		}
		// nothing existing may be touched or removed
		if string(mustRead(t, st.makeChunkPath(h))) != "chunk data" ||
			string(mustRead(t, st.makeChecksumPath(h))) != "sum" ||
			rawVersion(t, st, h) != 9 {
			t.Fatal("existing chunk files were modified by a failed makeNewChunkFiles")
		}
	})
}

func TestDeleteChunkFiles(t *testing.T) {
	t.Run("existing files", func(t *testing.T) {
		st := newTestStore(t)
		h := uint64(5)
		if err := st.makeNewChunkFiles(h); err != nil {
			t.Fatal(err)
		}
		if err := st.deleteChunkFiles(h); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{st.makeChunkPath(h), st.makeChecksumPath(h), st.makeVersionPath(h)} {
			if exists(p) {
				t.Fatalf("%s still exists", p)
			}
		}
	})

	t.Run("missing files", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.deleteChunkFiles(404); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("partially missing files", func(t *testing.T) {
		st := newTestStore(t)
		h := uint64(6)
		if err := st.makeNewChunkFiles(h); err != nil {
			t.Fatal(err)
		}
		os.Remove(st.makeVersionPath(h))
		if err := st.deleteChunkFiles(h); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected os.ErrNotExist, got %v", err)
		}
		if exists(st.makeChunkPath(h)) || exists(st.makeChecksumPath(h)) {
			t.Fatal("remaining files must still be deleted")
		}
	})
}

func TestLogVersion(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.makeNewChunkFiles(1); err != nil {
			t.Fatal(err)
		}
		if err := st.logVersion(1, 5); err != nil {
			t.Fatal(err)
		}
		if v := rawVersion(t, st, 1); v != 5 {
			t.Fatalf("version = %d, want 5", v)
		}
	})

	t.Run("multiple", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.makeNewChunkFiles(1); err != nil {
			t.Fatal(err)
		}
		for _, want := range []uint32{1, 2, 3, 100, 4294967295} {
			if err := st.logVersion(1, want); err != nil {
				t.Fatal(err)
			}
			if v := rawVersion(t, st, 1); v != want {
				t.Fatalf("version = %d, want %d", v, want)
			}
		}
	})
}

func TestReadVersion(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.makeNewChunkFiles(1); err != nil {
			t.Fatal(err)
		}
		if err := st.logVersion(1, 5); err != nil {
			t.Fatal(err)
		}
		v, err := st.readVersion(1)
		if err != nil || v != 5 {
			t.Fatalf("readVersion = %d, %v; want 5, nil", v, err)
		}
	})

	t.Run("multiple", func(t *testing.T) {
		st := newTestStore(t)
		if err := st.makeNewChunkFiles(1); err != nil {
			t.Fatal(err)
		}
		for _, want := range []uint32{1, 2, 3, 100, 4294967295} {
			if err := st.logVersion(1, want); err != nil {
				t.Fatal(err)
			}
			v, err := st.readVersion(1)
			if err != nil || v != want {
				t.Fatalf("readVersion = %d, %v; want %d, nil", v, err, want)
			}
		}
	})
}

func TestWriteChunk(t *testing.T) {
	st := newTestStore(t)
	f := openRW(t, filepath.Join(st.path, "w.chunk"))
	path := f.Name()

	if err := st.writeChunk(f, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := st.writeChunk(f, 10, []byte("world")); err != nil {
		t.Fatal(err)
	}
	if got, want := string(mustRead(t, path)), "hello\x00\x00\x00\x00\x00world"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// overwrite in the middle
	if err := st.writeChunk(f, 2, []byte("XY")); err != nil {
		t.Fatal(err)
	}
	if got, want := string(mustRead(t, path)), "heXYo\x00\x00\x00\x00\x00world"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// empty buffer changes nothing
	if err := st.writeChunk(f, 0, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := string(mustRead(t, path)), "heXYo\x00\x00\x00\x00\x00world"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// write across a block boundary at an unaligned offset
	big := pseudoData(3, 1000)
	if err := st.writeChunk(f, blockSize-500, big); err != nil {
		t.Fatal(err)
	}
	got := mustRead(t, path)
	if len(got) != blockSize+500 {
		t.Fatalf("file size = %d, want %d", len(got), blockSize+500)
	}
	if string(got[blockSize-500:]) != string(big) {
		t.Fatal("data written across the block boundary does not match")
	}
}

func TestLoadChecksums(t *testing.T) {
	t.Run("non existent handle", func(t *testing.T) {
		st := newTestStore(t)
		if _, err := st.loadChecksums(77); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("corrupt", func(t *testing.T) {
		cases := []struct {
			name  string
			chunk []byte
			sum   []byte
		}{
			{"size not multiple of 4", pseudoData(1, 10), make([]byte, 5)},
			{"size multiple of 4 but above max table", pseudoData(1, 10), make([]byte, blocksPerChunk*4+4)},
			{"fewer checksums than blocks", pseudoData(1, blockSize+1), make([]byte, 4)},
			{"more checksums than blocks", pseudoData(1, 10), make([]byte, 8)},
			{"checksums for an empty chunk", nil, make([]byte, 4)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				st := newTestStore(t)
				mustWrite(t, st.makeChunkPath(1), tc.chunk)
				mustWrite(t, st.makeChecksumPath(1), tc.sum)
				if _, err := st.loadChecksums(1); !errors.Is(err, internal.ErrFileCorrupt) {
					t.Fatalf("expected ErrFileCorrupt, got %v", err)
				}
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		for _, size := range []int{0, 100, blockSize, 2*blockSize + 5000} {
			t.Run(fmt.Sprintf("chunk size %d", size), func(t *testing.T) {
				st := newTestStore(t)
				data := pseudoData(4, size)
				mustWrite(t, st.makeChunkPath(8), data)
				mustWrite(t, st.makeChecksumPath(8), encodeTable(tableFor(data)))

				cs, err := st.loadChecksums(8)
				if err != nil {
					t.Fatal(err)
				}
				if cs.handle != 8 {
					t.Fatalf("handle = %d, want 8", cs.handle)
				}
				tablesEqual(t, cs.table, tableFor(data))
			})
		}
	})
}

// -------------------------------------------------------------- checksums

func TestChecksumsVerify(t *testing.T) {
	data := pseudoData(1, 2*blockSize+5000) // 3 blocks, last one partial
	setup := func(t *testing.T) (*checksums, *os.File) {
		t.Helper()
		return &checksums{handle: 1, table: tableFor(data)}, chunkFileWith(t, data)
	}
	corrupt := func(t *testing.T, f *os.File, off int64) {
		t.Helper()
		if _, err := f.WriteAt([]byte{data[off] ^ 0xff}, off); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("negative offset", func(t *testing.T) {
		cs, f := setup(t)
		if err := cs.verify(f, -1, 10); !errors.Is(err, internal.ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
	})

	t.Run("zero length", func(t *testing.T) {
		cs, f := setup(t)
		if err := cs.verify(f, 0, 0); err != nil {
			t.Fatal(err)
		}
		if err := cs.verify(f, 10*blockSize, 0); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("window beyond last checksum block", func(t *testing.T) {
		cs, f := setup(t)
		if err := cs.verify(f, 2*blockSize+100, blockSize); !errors.Is(err, internal.ErrBadRequest) {
			t.Fatalf("window spilling past last block: expected ErrBadRequest, got %v", err)
		}
		if err := cs.verify(f, 5*blockSize, 1); !errors.Is(err, internal.ErrBadRequest) {
			t.Fatalf("window entirely past table: expected ErrBadRequest, got %v", err)
		}
	})

	t.Run("file shorter than checksum table", func(t *testing.T) {
		cs, f := setup(t)
		if err := f.Truncate(2 * blockSize); err != nil {
			t.Fatal(err)
		}
		if err := cs.verify(f, 2*blockSize, 10); !errors.Is(err, internal.ErrFileCorrupt) {
			t.Fatalf("expected ErrFileCorrupt, got %v", err)
		}

		cs, f = setup(t)
		if err := f.Truncate(0); err != nil {
			t.Fatal(err)
		}
		if err := cs.verify(f, 0, 1); !errors.Is(err, internal.ErrFileCorrupt) {
			t.Fatalf("empty file: expected ErrFileCorrupt, got %v", err)
		}
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		cs, f := setup(t)
		corrupt(t, f, blockSize+100)
		if err := cs.verify(f, blockSize, 200); !errors.Is(err, internal.ErrFileCorrupt) {
			t.Fatalf("window inside corrupt block: expected ErrFileCorrupt, got %v", err)
		}
		if err := cs.verify(f, 0, 2*blockSize); !errors.Is(err, internal.ErrFileCorrupt) {
			t.Fatalf("window spanning corrupt block: expected ErrFileCorrupt, got %v", err)
		}
	})

	t.Run("valid chunk valid request", func(t *testing.T) {
		cs, f := setup(t)
		windows := []struct {
			off int64
			n   uint32
		}{
			{0, uint32(len(data))},
			{1, 1},
			{100, 70000},
			{blockSize - 1, 2},
			{2*blockSize + 10, 4000},
		}
		for _, w := range windows {
			if err := cs.verify(f, w.off, w.n); err != nil {
				t.Fatalf("verify(%d, %d): %v", w.off, w.n, err)
			}
		}
	})

	t.Run("scoped verification ignores corruption outside window", func(t *testing.T) {
		cs, f := setup(t)
		corrupt(t, f, 2*blockSize+10)
		if err := cs.verify(f, 0, 2*blockSize); err != nil {
			t.Fatalf("window over blocks 0-1: %v", err)
		}
		if err := cs.verify(f, blockSize+5, 100); err != nil {
			t.Fatalf("window inside block 1: %v", err)
		}
		// sanity: the corruption is real
		if err := cs.verify(f, 2*blockSize, 100); !errors.Is(err, internal.ErrFileCorrupt) {
			t.Fatalf("window over corrupt block: expected ErrFileCorrupt, got %v", err)
		}
	})
}

func TestChecksumsUpdate(t *testing.T) {
	data := pseudoData(2, 2*blockSize+5000) // 3 blocks, last one partial

	t.Run("negative offset", func(t *testing.T) {
		cs := newChecksums(1)
		f := chunkFileWith(t, data)
		if err := cs.update(f, -1, 10); !errors.Is(err, internal.ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if len(cs.table) != 0 {
			t.Fatal("table modified on error")
		}
	})

	t.Run("zero length", func(t *testing.T) {
		cs := newChecksums(1)
		f := chunkFileWith(t, data)
		if err := cs.update(f, 100, 0); err != nil {
			t.Fatal(err)
		}
		if len(cs.table) != 0 {
			t.Fatal("table modified by zero length update")
		}
	})

	t.Run("beyond max chunk size", func(t *testing.T) {
		cs := newChecksums(1)
		f := chunkFileWith(t, data)
		if err := cs.update(f, maxChunkSize-blockSize, blockSize+1); !errors.Is(err, internal.ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if err := cs.update(f, maxChunkSize, 1); !errors.Is(err, internal.ErrBadRequest) {
			t.Fatalf("offset at max: expected ErrBadRequest, got %v", err)
		}
		if len(cs.table) != 0 {
			t.Fatal("table modified on error")
		}
	})

	t.Run("exceeding table length", func(t *testing.T) {
		// empty table, window covers the whole file
		cs := newChecksums(1)
		f := chunkFileWith(t, data)
		if err := cs.update(f, 100, uint32(len(data)-100)); err != nil {
			t.Fatal(err)
		}
		tablesEqual(t, cs.table, tableFor(data))

		// table of 1, window starts at the next block: existing entry untouched
		const sentinel = 0xdeadbeef
		cs = &checksums{handle: 1, table: []uint32{sentinel}}
		f = chunkFileWith(t, data)
		if err := cs.update(f, blockSize+100, 100); err != nil {
			t.Fatal(err)
		}
		tablesEqual(t, cs.table, []uint32{sentinel, tableFor(data)[1]})

		// window starts past the end of the table: gap entries are zero
		cs = newChecksums(1)
		f = chunkFileWith(t, data)
		if err := cs.update(f, 2*blockSize+100, 50); err != nil {
			t.Fatal(err)
		}
		tablesEqual(t, cs.table, []uint32{0, 0, tableFor(data)[2]})
	})

	t.Run("within table length", func(t *testing.T) {
		cur := slices.Clone(data)
		f := chunkFileWith(t, cur)
		cs := &checksums{handle: 1, table: tableFor(cur)}

		mutate := func(off int64, n, seed int) {
			t.Helper()
			buf := pseudoData(seed, n)
			if _, err := f.WriteAt(buf, off); err != nil {
				t.Fatal(err)
			}
			copy(cur[off:], buf)
			if err := cs.update(f, off, uint32(n)); err != nil {
				t.Fatal(err)
			}
			tablesEqual(t, cs.table, tableFor(cur))
		}

		mutate(blockSize+100, 500, 11)   // inside one block, unaligned
		mutate(blockSize-50, 100, 12)    // spans blocks 0 and 1
		mutate(2*blockSize+100, 50, 13)  // last, partial block
		mutate(1, 2*blockSize+1000, 14)  // spans all three blocks
	})
}

func TestChecksumsFlush(t *testing.T) {
	t.Run("zero length table", func(t *testing.T) {
		f := openRW(t, filepath.Join(t.TempDir(), "z.sum"))
		cs := newChecksums(1)
		if err := cs.flush(f); err != nil {
			t.Fatal(err)
		}
		if got := mustRead(t, f.Name()); len(got) != 0 {
			t.Fatalf("expected empty file, got %d bytes", len(got))
		}
	})

	t.Run("completely filled table", func(t *testing.T) {
		f := openRW(t, filepath.Join(t.TempDir(), "full.sum"))
		table := make([]uint32, blocksPerChunk)
		for i := range table {
			table[i] = uint32(i+1) * 2654435761
		}
		cs := &checksums{handle: 1, table: table}
		if err := cs.flush(f); err != nil {
			t.Fatal(err)
		}
		got := mustRead(t, f.Name())
		if len(got) != blocksPerChunk*4 {
			t.Fatalf("file size = %d, want %d", len(got), blocksPerChunk*4)
		}
		if string(got) != string(encodeTable(table)) {
			t.Fatal("flushed bytes do not match the table")
		}
		for i := range table {
			if binary.BigEndian.Uint32(got[i*4:]) != table[i] {
				t.Fatalf("entry %d mismatch", i)
			}
		}
	})
}