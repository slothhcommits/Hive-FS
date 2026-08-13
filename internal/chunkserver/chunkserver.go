package internal

import "io"

type ChunkReader struct {
	src io.ReadSeekCloser
	io.Reader
	start int64
	size  uint32
}

func NewChunkReader(size uint32, r io.ReadSeekCloser) (*ChunkReader, error) {

	start, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	return &ChunkReader{
		src:    r,
		Reader: io.LimitReader(r, int64(size)),
		start:  start,
		size:   size,
	}, nil
}

func (cr *ChunkReader) Read(p []byte) (int, error) {
	return cr.Read(p)
}

func (cr *ChunkReader) Close() error {
	return cr.src.Close()
}

func (cr *ChunkReader) Reset() error {

	if _, err := cr.src.Seek(cr.start, io.SeekStart); err != nil {
		return err
	}

	cr.Reader = io.LimitReader(cr.src, int64(cr.size))
	return nil
}
