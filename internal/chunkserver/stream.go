package chunkserver

import (
	"io"

	"github.com/slothhcommits/Hive-FS/internal/messages"
)

type forwardStreamPeer struct {
	addr     string
	metadata *messages.Client_WriteStreamMetadata
	src      io.Reader
	size     int64
}
