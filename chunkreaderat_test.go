package chunkreaderat_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/guilhem/chunkreaderat"
)

func TestChunkReaderAt_ReadAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		off     int64
		n       int
		chunk   int64
		memory  int
		want    string
		wanterr interface{}
	}{
		{0, 10, 1, 10, "0123456789", nil},
		{1, 10, 1, 10, "123456789", io.EOF},
		{1, 9, 1, 10, "123456789", nil},
		{11, 10, 1, 10, "", io.EOF},
		{0, 0, 1, 10, "", nil},
		{-1, 0, 1, 10, "", "bytes.Reader.ReadAt: negative offset"},
	}

	for i, tt := range tests {
		buf := bytes.NewReader([]byte("0123456789"))
		r, _ := chunkreaderat.NewChunkReaderAt(buf, 1)
		b := make([]byte, tt.n)
		rn, err := r.ReadAt(b, tt.off)
		got := string(b[:rn])

		if got != tt.want {
			t.Errorf("%d. got %q; want %q", i, got, tt.want)
		}

		if fmt.Sprintf("%v", err) != fmt.Sprintf("%v", tt.wanterr) {
			t.Errorf("%d. got error = %v; want %v", i, err, tt.wanterr)
		}
	}
}

func TestChunkReaderAt_ReadAtBig(t *testing.T) {
	t.Parallel()

	mem100M := int64(100 * 1024 * 1024)
	mem1M := int64(1024 * 1024)

	tests := []struct {
		size    int64
		off     int64
		n       int
		chunk   int64
		wanterr interface{}
	}{
		{mem100M, 0, 10, 1024, nil},
		{mem100M, 0, 10, 1024, nil},
		{mem100M, (mem100M) - 9, 10, 1024, io.EOF},
		{mem100M, 1, 9, 10, nil},
		{mem100M, (mem100M) + 1, 10, 1024, io.EOF},
		{mem100M, 0, 0, 1, nil},
		{mem100M, -1, 0, 1024, "bytes.Reader.ReadAt: negative offset"},
		/* #nosec */
		{mem100M, rand.Int63n(mem100M - 100), 100, 1024, nil},
		/* #nosec */
		{mem100M, rand.Int63n(mem100M - mem1M), int(mem1M), mem1M, nil},
	}

	for i, tt := range tests {
		d := make([]byte, tt.size)
		/* #nosec */
		rand.Read(d)

		buf := bytes.NewReader(d)
		r, _ := chunkreaderat.NewChunkReaderAt(buf, tt.chunk)
		b := make([]byte, tt.n)
		_, err := r.ReadAt(b, tt.off)

		if fmt.Sprintf("%v", err) != fmt.Sprintf("%v", tt.wanterr) {
			t.Errorf("%d. got error = %v; want %v", i, err, tt.wanterr)
		}
	}
}
