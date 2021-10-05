package chunkreaderat

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/bluele/gcache"
)

type ReaderAtSizer interface {
	io.ReaderAt
	Size() int64
}

type ChunkReaderAt struct {
	cache     gcache.Cache
	chunkSize int64
	size      int64
}

var (
	ErrAssertion      = errors.New("assertion error")
	ErrNegativeOffset = errors.New("bytes.Reader.ReadAt: negative offset")
)

func NewChunkReaderAt(rd ReaderAtSizer, chunkSize int64) (io.ReaderAt, error) {
	size := rd.Size()

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

	gc := gcache.New(1).
		LoaderFunc(loadFunction).
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

	ret := make([]byte, 0, len(b))

	for currentChunk <= r.size/r.chunkSize {
		loopb := make([]byte, len(b)-readData)

		bufI, err := r.cache.Get(currentChunk)
		if err != nil {
			return readData, fmt.Errorf("can't get chunk %d: %w", currentChunk, err)
		}

		buf, ok := bufI.([]byte)
		if !ok {
			return readData, ErrAssertion
		}

		n, err := bytes.NewReader(buf).ReadAt(loopb, currentOffset)
		readData += n

		if err != nil && !errors.Is(err, io.EOF) {
			return readData, fmt.Errorf("can't read at: %w", err)
		}

		if n == 0 {
			break
		}

		ret = append(ret, loopb[:n]...)

		if readData == len(b) {
			break
		}

		currentChunk++

		currentOffset = 0
	}

	n := copy(b, ret[:readData])
	if n < len(b) {
		return n, io.EOF
	}

	return n, nil
}
