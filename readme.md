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