# Hive-FS

**A distributed file system in Go, built from the Google File System paper.**

## What is Hive-FS?

Hive-FS stores large files across many ordinary machines and keeps working when some of them fail.

A file is cut into 64 MB pieces called **chunks**, and each chunk is copied onto several different machines (three by default, configurable per chunk). Three kinds of programs work together:

- **The master** keeps the folder tree, the list of chunks that make up each file, and where every copy lives. It never handles file data.
- **Chunkservers** store the chunk copies on disk and serve the actual reads and writes.
- **The client** asks the master where a file's chunks are, then talks directly to the chunkservers.

Because file data never passes through the master, it doesn't become a bottleneck.

When a chunkserver dies, the master notices, finds the chunks that lost a copy, and has other servers re-copy them. Nothing is lost and nobody has to step in.

![Architecture diagram](./assets/Hive-FS-Architecture.png)

## Features

- **POSIX-like client interface** with a hierarchical namespace
- **Directories work like in a normal file system.** Move, delete, snapshot or list an entire directory in a single metadata operation.
- **64 MB chunks:** files are split into chunks and replicated across chunkservers.
- **Checksummed storage:** every chunk carries 64 KB block checksums, verified on each chunk operation and scanned in the background by a scrubber.
- **Snapshots** of files and directories using copy-on-write
- **Atomic record append**
- **Automatic re-replication** of chunks that lose a copy
- **Lazy garbage collection** of deleted files
- **Crash-tolerant master** with a replayable WAL and checkpoints
- **Stale replica detection** through chunk version numbers

## Highlights

### Hive protocol over TCP

Hive-FS talks over its own protocol, built on TCP.

- **Compact framing.** Every message starts with a fixed 16-byte header (magic, version, flags, request ID, type and payload length), followed by the payload.
- **Request multiplexing.** Each request carries an ID, so many requests can be in flight on one connection and replies can arrive in any order. A slow reply never blocks the ones behind it.

![Hive protocol frame and request multiplexing](./assets/Highlight-FrameLayout-and-RequestMultiplexing.png)

- **Separate control and data connections.** Small control messages use framed, multiplexed connections. Bulk chunk data goes over its own connection as a raw stream with no framing, and a single CRC32C checksum at the end. Corruption is caught once per transfer, with no per-frame overhead, and a heartbeat never waits behind a 64 MB chunk.

![Control and data connections](IMAGE_PATH_HERE)

