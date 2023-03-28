package util

import (
	"bytes"
	"compress/gzip"
	"flag"
	"io"
	"path/filepath"
)

var (
	pathPrefix = flag.String("storage.path_prefix", "", "The prefix directory to store all blobs in")
)

func Decompress(in []byte, err error) ([]byte, error) {
	if err != nil {
		return in, err
	}

	var buf bytes.Buffer
	// Write instead of using NewBuffer because if this is not a gzip file
	// we want to return "in" directly later, and NewBuffer would take
	// ownership of it.
	if _, err := buf.Write(in); err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(&buf)
	if err == gzip.ErrHeader {
		// Compatibility hack: if we got a header error it means this
		// is probably an uncompressed record written before we were
		// compressing. Just read it as-is.
		return in, nil
	}
	if err != nil {
		return nil, err
	}
	rsp, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	if err := zr.Close(); err != nil {
		return nil, err
	}
	return rsp, nil
}

func Compress(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	zr := gzip.NewWriter(&buf)
	if _, err := zr.Write(in); err != nil {
		return nil, err
	}
	if err := zr.Close(); err != nil {
		return nil, err
	}
	return io.ReadAll(&buf)
}

var BlobPath = func(blobName string) string {
	return filepath.Join(*pathPrefix, blobName)
}
