package chunkreaderat

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/bluele/gcache"
)

// ChunkReaderAt implement io.ReaderAt interface
type ChunkReaderAt struct {
	cache     gcache.Cache
	chunkSize int64
	size      int64
}

var (
	ErrAssertion      = errors.New("assertion error")
	ErrNegativeOffset = errors.New("bytes.Reader.ReadAt: negative offset")
	ErrBufferSize     = errors.New("bufferSize can't be <= 0")
	ErrChunkSize      = errors.New("chunkSize can't be <= 0")
)

// NewChunkReaderAt caches reads from rd, whose size is supplied separately.
// chunkSize is the maximum number of bytes in each cached chunk.
// bufferSize is the number of chunks stored using ARC eviction.
// Both chunkSize and bufferSize must be positive. The source must remain
// unchanged and support concurrent ReadAt calls if the wrapper is shared.
func NewChunkReaderAt(rd io.ReaderAt, size, chunkSize int64, bufferSize int) (*ChunkReaderAt, error) {
	if bufferSize <= 0 {
		return nil, ErrBufferSize
	}
	if chunkSize <= 0 {
		return nil, ErrChunkSize
	}

	loadFunction := func(key interface{}) (interface{}, error) {
		numChunk, ok := key.(int64)
		if !ok {
			return nil, ErrAssertion
		}

		offset := numChunk * chunkSize
		buflen := chunkSize

		var buf []byte
		if numChunk == size/chunkSize {
			buf = make([]byte, size%chunkSize)
		} else {
			buf = make([]byte, buflen)
		}

		n, err := rd.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("can't read at: %w", err)
		}

		return buf[:n], nil
	}

	gc := gcache.New(bufferSize).
		LoaderFunc(loadFunction).
		ARC().
		Build()

	return &ChunkReaderAt{
		chunkSize: chunkSize,
		cache:     gc,
		size:      size,
	}, nil
}

func (r *ChunkReaderAt) ReadAt(b []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, ErrNegativeOffset
	}

	if offset >= r.size {
		return 0, io.EOF
	}

	currentChunk := offset / r.chunkSize
	currentOffset := offset % r.chunkSize

	readData := 0

	for currentChunk <= r.size/r.chunkSize {
		bufI, err := r.cache.Get(currentChunk)
		if err != nil {
			return readData, fmt.Errorf("can't get chunk %d: %w", currentChunk, err)
		}

		buf, ok := bufI.([]byte)
		if !ok {
			return readData, ErrAssertion
		}

		n, err := bytes.NewReader(buf).ReadAt(b[readData:], currentOffset)
		readData += n

		if err != nil && !errors.Is(err, io.EOF) {
			return readData, fmt.Errorf("can't read at: %w", err)
		}

		if n == 0 {
			break
		}

		if readData == len(b) {
			break
		}

		currentChunk++

		currentOffset = 0
	}

	if readData < len(b) {
		return readData, io.EOF
	}

	return readData, nil
}

// Size return size of source
func (r *ChunkReaderAt) Size() int64 {
	return r.size
}
