package chunkreaderat_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/guilhem/chunkreaderat"
)

func TestChunkReaderAt_ReadAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		off        int64
		n          int
		chunk      int64
		memory     int
		bufferSize int
		want       string
		wanterr    error
	}{
		{0, 10, 1, 10, 1, "0123456789", nil},
		{1, 10, 1, 10, 1, "123456789", io.EOF},
		{1, 9, 1, 10, 1, "123456789", nil},
		{11, 10, 1, 10, 1, "", io.EOF},
		{0, 0, 1, 10, 1, "", nil},
		{-1, 0, 1, 10, 1, "", chunkreaderat.ErrNegativeOffset},
	}

	for i, tt := range tests {
		buf := bytes.NewReader([]byte("0123456789"))
		r, err := chunkreaderat.NewChunkReaderAt(buf, buf.Size(), tt.chunk, tt.bufferSize)
		if err != nil {
			if !errors.Is(err, tt.wanterr) {
				t.Errorf("%d. got error = %v; want %v", i, err, tt.wanterr)
			}
			return
		}
		b := make([]byte, tt.n)
		rn, err := r.ReadAt(b, tt.off)
		got := string(b[:rn])

		if got != tt.want {
			t.Errorf("%d. got %q; want %q", i, got, tt.want)
		}

		if !errors.Is(err, tt.wanterr) {
			t.Errorf("%d. got error = %v; want %v", i, err, tt.wanterr)
		}
	}
}

func TestChunkReaderAt_ReadAtBig(t *testing.T) {
	t.Parallel()

	mem100M := int64(100 * 1024 * 1024)
	mem1M := int64(1024 * 1024)

	tests := []struct {
		size       int64
		off        int64
		n          int
		chunk      int64
		bufferSize int
		wanterr    error
	}{
		{mem100M, 0, 10, 1024, 1, nil},
		{mem100M, 0, 10, 1024, 0, chunkreaderat.ErrBufferSize},
		{mem100M, (mem100M) - 9, 10, 1024, 1, io.EOF},
		{mem100M, 1, 9, 10, 1, nil},
		{mem100M, (mem100M) + 1, 10, 1024, 1, io.EOF},
		{mem100M, 0, 0, 1, 20, nil},
		{mem100M, -1, 0, 1024, 1, chunkreaderat.ErrNegativeOffset},
		/* #nosec */
		{mem100M, rand.Int63n(mem100M - 100), 100, 1024, 1, nil},
		/* #nosec */
		{mem100M, rand.Int63n(mem100M - mem1M), int(mem1M), mem1M, 1, nil},
	}

	for i, tt := range tests {
		d := make([]byte, tt.size)
		/* #nosec */
		rand.Read(d)

		buf := bytes.NewReader(d)
		r, err := chunkreaderat.NewChunkReaderAt(buf, buf.Size(), tt.chunk, tt.bufferSize)
		if err != nil {
			if !errors.Is(err, tt.wanterr) {
				t.Errorf("%d. got error = %v; want %v", i, err, tt.wanterr)
			}
			return
		}
		b := make([]byte, tt.n)
		_, err = r.ReadAt(b, tt.off)

		if fmt.Sprintf("%v", err) != fmt.Sprintf("%v", tt.wanterr) {
			t.Errorf("%d. got error = %v; want %v", i, err, tt.wanterr)
		}
	}
}

func TestChunkReaderAt_Size(t *testing.T) {
	type fields struct {
		chunkSize int64
		size      int64
	}
	random := rand.Int63n(9999)
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{
			name: "1",
			fields: fields{
				chunkSize: 1,
				size:      1,
			},
			want: 1,
		},
		{
			name: "1000",
			fields: fields{
				chunkSize: 1,
				size:      1000,
			},
			want: 1000,
		},
		{
			name: "random",
			fields: fields{
				chunkSize: 1,
				size:      random,
			},
			want: random,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := make([]byte, tt.fields.size)
			/* #nosec */
			rand.Read(d)
			buf := bytes.NewReader(d)
			r, _ := chunkreaderat.NewChunkReaderAt(buf, buf.Size(), tt.fields.chunkSize, int(tt.fields.size))
			if got := r.Size(); got != tt.want {
				t.Errorf("ChunkReaderAt.Size() = %v, want %v", got, tt.want)
			}
		})
	}
}
