package chunkreaderat_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/guilhem/chunkreaderat"
)

func TestChunkReaderAt_ReadAt(t *testing.T) {
	t.Parallel()

	buf := bytes.NewReader([]byte("0123456789"))
	r, _ := chunkreaderat.NewChunkReaderAt(buf, 1, 10)
	tests := []struct {
		off     int64
		n       int
		want    string
		wanterr interface{}
	}{
		{0, 10, "0123456789", nil},
		{1, 10, "123456789", io.EOF},
		{1, 9, "123456789", nil},
		{11, 10, "", io.EOF},
		{0, 0, "", nil},
		{-1, 0, "", "bytes.Reader.ReadAt: negative offset"},
	}

	for i, tt := range tests {
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
