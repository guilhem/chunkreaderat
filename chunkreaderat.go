package chunkreaderat

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/allegro/bigcache/v2"
	"github.com/eko/gocache/cache"
	"github.com/eko/gocache/store"
)

type ReaderAtSizer interface {
	io.ReaderAt
	Size() int64
}

type ChunkReaderAt struct {
	cache     cache.CacheInterface
	chunkSize int64
	size      int64
}

const evictionTime = 5 * time.Minute

var (
	errAssertion      = errors.New("assertion error")
	errNegativeOffset = errors.New("bytes.Reader.ReadAt: negative offset")
)

func NewChunkReaderAt(rd ReaderAtSizer, chunkSize int64, maxMemoryMB int) (io.ReaderAt, error) {
	size := rd.Size()

	config := bigcache.DefaultConfig(evictionTime)
	config.HardMaxCacheSize = maxMemoryMB
	config.MaxEntrySize = int(chunkSize)

	// find closest power of 2 for big chunk
	if size/chunkSize <= 512 {
		config.Shards = int(math.Pow(2, math.Ceil(math.Log2(float64(size/chunkSize)))))
	}

	bigcacheClient, err := bigcache.NewBigCache(config)
	if err != nil {
		return nil, fmt.Errorf("can't create BigCache client: %w", err)
	}

	bigcacheStore := store.NewBigcache(bigcacheClient, nil)

	loadFunction := func(key interface{}) (interface{}, error) {
		numChunk, ok := key.(int64)
		offset := numChunk * chunkSize
		buflen := chunkSize

		var buf []byte
		if numChunk == size/chunkSize {
			buf = make([]byte, size%chunkSize)
		} else {
			buf = make([]byte, buflen)
		}

		if !ok {
			return nil, errAssertion
		}

		n, err := rd.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("can't read at: %w", err)
		}

		return buf[:n], nil
	}

	// Initialize loadable cache
	cacheManager := cache.NewLoadable(
		loadFunction,
		cache.New(bigcacheStore),
	)

	return &ChunkReaderAt{
		chunkSize: chunkSize,
		cache:     cacheManager,
		size:      size,
	}, nil
}

func (r *ChunkReaderAt) ReadAt(b []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errNegativeOffset
	}

	if offset >= r.size {
		return 0, io.EOF
	}

	currentChunk := offset / r.chunkSize
	currentOffset := offset % r.chunkSize

	readedData := 0

	ret := make([]byte, 0, len(b))

	for currentChunk <= r.size/r.chunkSize {
		loopb := make([]byte, len(b)-readedData)

		bufI, err := r.cache.Get(currentChunk)
		if err != nil {
			return readedData, fmt.Errorf("can't get chunk %d: %w", currentChunk, err)
		}

		buf, ok := bufI.([]byte)
		if !ok {
			return readedData, errAssertion
		}

		n, err := bytes.NewReader(buf).ReadAt(loopb, currentOffset)
		readedData += n

		if err != nil && !errors.Is(err, io.EOF) {
			return readedData, fmt.Errorf("can't read at: %w", err)
		}

		if n == 0 {
			break
		}

		ret = append(ret, loopb[:n]...)

		if readedData == len(b) {
			break
		}

		currentChunk++

		currentOffset = 0
	}

	n := copy(b, ret[:readedData])
	if n < len(b) {
		return n, io.EOF
	}

	return n, nil
}
