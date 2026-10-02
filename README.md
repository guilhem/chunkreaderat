# chunkreaderat

[![Go Reference](https://pkg.go.dev/badge/github.com/guilhem/chunkreaderat.svg)](https://pkg.go.dev/github.com/guilhem/chunkreaderat)
[![Go](https://github.com/guilhem/chunkreaderat/actions/workflows/go.yml/badge.svg)](https://github.com/guilhem/chunkreaderat/actions/workflows/go.yml)

Cache reads from any Go `io.ReaderAt` in fixed-size chunks. Useful when the
underlying reader is expensive, such as an HTTP range reader, and consumers
like `archive/zip` make repeated small reads.

## Install

Requires Go 1.26 or newer.

```sh
go get github.com/guilhem/chunkreaderat
```

## Read through a chunk cache

```go
package main

import (
    "bytes"
    "fmt"
    "log"

    "github.com/guilhem/chunkreaderat"
)

func main() {
    source := bytes.NewReader([]byte("hello world"))
    reader, err := chunkreaderat.NewChunkReaderAt(source, source.Size(), 4, 2)
    if err != nil {
        log.Fatal(err)
    }
    buf := make([]byte, 5)
    n, err := reader.ReadAt(buf, 6)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(string(buf[:n])) // world
}
```

`NewChunkReaderAt(source, size, chunkSize, bufferSize)` takes the source size
separately; the source only needs to implement `io.ReaderAt`.

| Argument | Meaning |
| --- | --- |
| `size` | Actual source length in bytes; must be known and nonnegative |
| `chunkSize` | Maximum bytes per cached chunk; must be positive |
| `bufferSize` | Maximum number of cached chunks; must be positive |

Chunks load on demand and use ARC eviction through
[bluele/gcache](https://github.com/bluele/gcache). Each miss reads a complete
chunk, except the final chunk, which is limited to the source size. Cached
payload is at most `chunkSize * bufferSize` bytes, plus cache bookkeeping and
in-flight loads. Larger chunks reduce source reads but fetch more unused data.

## Use with HTTP and ZIP

Create an [httpreaderat](https://github.com/guilhem/httpreaderat) reader, then
wrap it before passing it to `archive/zip`:

```go
// remote is an initialized *httpreaderat.HTTPReaderAt with a known Size().
reader, err := chunkreaderat.NewChunkReaderAt(remote, remote.Size(), 1<<20, 8)
if err != nil {
    return err
}
archive, err := zip.NewReader(reader, reader.Size())
```

This configuration retains up to eight 1 MiB chunks. The wrapper does not own
or close the source; close any underlying file or HTTP fallback store yourself.

## Behavior and limits

- The source must remain unchanged. There is no expiration or invalidation API;
  create a new wrapper when the source changes. Cached HTTP chunks do not
  revalidate their source until a cache miss occurs.
- Concurrent reads are supported when the underlying source supports them.
- Reads spanning chunks return the bytes already copied if a later chunk fails.
  The source error remains available through `errors.Is`.
- Reads past the supplied size return `io.EOF`; negative offsets return
  `ErrNegativeOffset`. Invalid chunk and cache capacities return `ErrChunkSize`
  and `ErrBufferSize` respectively.
- Passing an accurate size is the caller's responsibility; the constructor
  does not probe the source or validate the size.

## Development

```sh
go test -race ./...
go vet ./...
```

CI builds, runs the race detector and vet, and scans the library with CodeQL.

## License

[Apache-2.0](LICENSE).
