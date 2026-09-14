// The demo lives in its own file so it sorts last: go test runs a package's
// files in filename order, and this transfer is meant to be the final thing on
// screen.
package hive

import (
	"fmt"
	"hash/crc32"
	"io"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// demo
// ---------------------------------------------------------------------------

// TestDemoBulkTransfer moves a full gigabyte between two transports over a
// single stream and verifies it byte for byte with a rolling checksum. The
// sender is paced so the run takes about fifteen seconds and the progress is
// actually watchable; loopback would otherwise finish it in a fraction of a
// second.
//
//	go test ./internal/hive/ -v -run TestDemoBulkTransfer
func TestDemoBulkTransfer(t *testing.T) {

	if testing.Short() {
		t.Skip("demo")
	}

	const (
		totalSize   = 1 << 30  // 1 GiB
		blockSize   = 1 << 20  // 1 MiB on the wire at a time
		reportEvery = 64 << 20 // progress line every 64 MiB
		targetTime  = 15 * time.Second
	)

	say := func(format string, args ...any) {
		fmt.Printf("  "+format+"\n", args...)
	}

	table := crc32.MakeTable(crc32.Castagnoli)

	// receiver side: drain the stream, checksum it, report as it goes
	type receipt struct {
		n   int64
		sum uint32
		err error
	}
	done := make(chan receipt, 1)

	srv := newTestServer(t, echo, func(s *Stream) {
		defer s.Close()

		h := crc32.New(table)
		buf := make([]byte, 256<<10)

		var (
			read     int64
			nextMark int64 = reportEvery
			start          = time.Now()
		)

		for read < int64(s.Size) {

			n, err := s.Pipe.Read(buf)
			if n > 0 {
				h.Write(buf[:n])
				read += int64(n)
			}

			if read >= nextMark {
				say("receiver  %4d MiB in  |  %5.1f MiB/s  |  running crc %08x",
					read>>20,
					float64(read>>20)/time.Since(start).Seconds(),
					h.Sum32())
				nextMark += reportEvery
			}

			if err != nil {
				if err == io.EOF {
					break
				}
				done <- receipt{n: read, err: err}
				return
			}
		}

		done <- receipt{n: read, sum: h.Sum32()}
	})

	client := NewTCPTransport()
	defer client.Close()

	// one random block, sent over and over: the payload is 512 MiB but nothing
	// ever holds more than a megabyte of it
	block := randomBytes(blockSize)

	want := crc32.New(table)
	for i := 0; i < totalSize/blockSize; i++ {
		want.Write(block)
	}

	say("")
	say("hive stream demo")
	say("  payload   : %d MiB", totalSize>>20)
	say("  block     : %d KiB per write", blockSize>>10)
	say("  expected crc32c : %08x", want.Sum32())
	say("")

	w, err := client.OpenStream(srv.addr, dialTimeout, Metadata{
		Type: uint8(21),
		Size: uint32(totalSize),
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// pace the sender so the transfer lasts roughly targetTime
	perBlock := targetTime / (totalSize / blockSize)

	start := time.Now()
	var (
		sent     int64
		nextMark int64 = reportEvery
	)

	for sent < totalSize {

		deadline := start.Add(time.Duration(sent/blockSize+1) * perBlock)

		if _, err := w.Write(block); err != nil {
			t.Fatalf("write at %d MiB: %v", sent>>20, err)
		}
		sent += blockSize

		if sent >= nextMark {
			say("sender    %4d MiB out |  %5.1f MiB/s  |  %4.1f%% done",
				sent>>20,
				float64(sent>>20)/time.Since(start).Seconds(),
				100*float64(sent)/float64(totalSize))
			nextMark += reportEvery
		}

		if wait := time.Until(deadline); wait > 0 {
			time.Sleep(wait)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	say("")
	say("sender finished, waiting on the receiver...")

	select {
	case got := <-done:

		elapsed := time.Since(start)

		if got.err != nil {
			t.Fatalf("receiver: %v", got.err)
		}
		if got.n != totalSize {
			t.Fatalf("receiver got %d bytes, want %d", got.n, totalSize)
		}
		if got.sum != want.Sum32() {
			t.Fatalf("checksum %08x, want %08x — the stream was corrupted",
				got.sum, want.Sum32())
		}

		say("")
		say("  delivered : %d MiB in %s", got.n>>20, elapsed.Round(10*time.Millisecond))
		say("  throughput: %.1f MiB/s", float64(got.n>>20)/elapsed.Seconds())
		say("  crc32c    : %08x  == expected, every byte intact", got.sum)
		say("")

	case <-time.After(2 * time.Minute):
		t.Fatal("receiver never finished")
	}
}
