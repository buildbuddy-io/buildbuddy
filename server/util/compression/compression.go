package compression

import (
	"bytes"
	"compress/flate"
	"io"
	"io/ioutil"
	"runtime"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/klauspost/compress/zstd"
)

var (
	// zstdEncoder can be shared across goroutines to compress chunks of data
	// using EncodeAll. Streaming functions should not be used. The encoder should
	// not be closed.
	zstdEncoder = mustGetZstdEncoder()

	// ZstdDecoderPool can be used across goroutines to retrieve ZSTD decoders,
	// either for streaming decompression using ReadFrom or batch decompression
	// using DecodeAll. The returned decoders *must not* be closed.
	ZstdDecoderPool = newZstdDecoderPool()
)

func mustGetZstdEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	return enc
}

// CompressZstd compresses a chunk of data into dst using zstd compression.
// If dst is not big enough then a new buffer will be allocated.
func CompressZstd(dst []byte, src []byte) []byte {
	return zstdEncoder.EncodeAll(src, dst[:0])
}

// DecompressZstd decompresses a full chunk of zstd data into dst. If dst is
// not big enough then a new buffer will be allocated.
func DecompressZstd(dst []byte, src []byte) ([]byte, error) {
	dec, err := ZstdDecoderPool.Get(nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := ZstdDecoderPool.Put(dec); err != nil {
			log.Errorf("Failed to return zstd decoder to pool: %s", err)
		}
	}()
	return dec.DecodeAll(src, dst[:0])
}

// ZstdDecoderPool allows reusing zstd decoders to avoid excessive allocations.
type zstdDecoderPool struct {
	pool sync.Pool
}

func newZstdDecoderPool() *zstdDecoderPool {
	return &zstdDecoderPool{
		pool: sync.Pool{
			New: func() interface{} {
				dc, err := zstd.NewReader(nil)
				if err != nil {
					return err
				}
				runtime.SetFinalizer(dc, (*zstd.Decoder).Close)
				return dc
			},
		},
	}
}

// Get returns a decoder from the pool. The returned decoder must be returned
// back to the pool with Put. The returned decoders *must not* be closed.
//
// If the returned decoder will only be used to decode chunks via DecodeAll, a
// nil reader can be passed.
func (p *zstdDecoderPool) Get(reader io.Reader) (*zstd.Decoder, error) {
	val := p.pool.Get()
	if err, ok := val.(error); ok {
		return nil, err
	}
	// No need to check this type assertion since Put can only accept decoders.
	decoder := val.(*zstd.Decoder)
	if err := decoder.Reset(reader); err != nil {
		return nil, err
	}
	return decoder, nil
}

func (p *zstdDecoderPool) Put(decoder *zstd.Decoder) error {
	// Release reference to enclosed reader before adding back to the pool.
	if err := decoder.Reset(nil); err != nil {
		return err
	}
	p.pool.Put(decoder)
	return nil
}

func CompressFlate(data []byte, level int) ([]byte, error) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func DecompressFlate(data []byte) ([]byte, error) {
	dataReader := bytes.NewReader(data)
	rc := flate.NewReader(dataReader)

	buf, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
