package registry

import (
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// Error provides information about the failed hash verification.
type Error struct {
	got     string
	want    v1.Hash
	gotSize int64
}

func (v Error) Error() string {
	return fmt.Sprintf("error verifying %s checksum after reading %d bytes; got %q, want %q",
		v.want.Algorithm, v.gotSize, v.got, v.want)
}

// ReadCloser wraps the given io.ReadCloser to verify that its contents match
// the provided v1.Hash before io.EOF is returned.
//
// The reader will only be read up to size bytes, to prevent resource
// exhaustion. If EOF is returned before size bytes are read, an error is
// returned.
//
// A size of SizeUnknown (-1) indicates disables size verification when the size
// is unknown ahead of time.
func ReadCloser(r io.ReadCloser, size int64, h v1.Hash) (io.ReadCloser, error) {
	w, err := v1.Hasher(h.Algorithm)
	if err != nil {
		return nil, err
	}
	r2 := io.TeeReader(r, w) // pass all writes to the hasher.
	if size != -1 {
		r2 = io.LimitReader(r2, size) // if we know the size, limit to that size.
	}
	return &AndReadCloser{
		Reader: &verifyReader{
			inner:    r2,
			hasher:   w,
			expected: h,
			wantSize: size,
		},
		CloseFunc: r.Close,
	}, nil
}

type verifyReader struct {
	inner             io.Reader
	hasher            hash.Hash
	expected          v1.Hash
	gotSize, wantSize int64
}

// Read implements io.Reader
func (vc *verifyReader) Read(b []byte) (int, error) {
	n, err := vc.inner.Read(b)
	vc.gotSize += int64(n)
	if err == io.EOF {
		if vc.wantSize != -1 && vc.gotSize != vc.wantSize {
			return n, fmt.Errorf("error verifying size; got %d, want %d", vc.gotSize, vc.wantSize)
		}
		got := hex.EncodeToString(vc.hasher.Sum(nil))
		if want := vc.expected.Hex; got != want {
			return n, Error{
				got:     vc.expected.Algorithm + ":" + got,
				want:    vc.expected,
				gotSize: vc.gotSize,
			}
		}
	}
	return n, err
}
