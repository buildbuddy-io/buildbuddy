package testcompression

import (
	"bytes"
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// CompressionCache simulates a cache that supports compression for unit tests
type CompressionCache struct {
	interfaces.Cache
}

func (c *CompressionCache) Get(ctx context.Context, r *resource.ResourceName) ([]byte, error) {
	data, err := c.Cache.Get(ctx, r)
	if err != nil {
		return nil, err
	}

	cachedDataWithCompression, err := dataWithCompression(r, data)
	if err != nil {
		return nil, status.InternalErrorf("Could not get data for compression %v for %s: %s", r.GetCompressor(), r.GetDigest(), err)
	}

	return cachedDataWithCompression, nil
}

func (c *CompressionCache) Reader(ctx context.Context, rn *resource.ResourceName, offset, limit int64) (io.ReadCloser, error) {
	buf, err := c.Get(ctx, rn)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(buf)
	r.Seek(offset, 0)
	length := int64(len(buf))
	if limit != 0 && limit < length {
		length = limit
	}
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	return io.NopCloser(r), nil
}

func (c *CompressionCache) GetMulti(ctx context.Context, resources []*resource.ResourceName) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(resources))
	for _, r := range resources {
		data, err := c.Get(ctx, r)
		if status.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		foundMap[r.GetDigest()] = data
	}
	return foundMap, nil
}

func dataWithCompression(requestedResource *resource.ResourceName, cachedData []byte) ([]byte, error) {
	isCompressed := int64(len(cachedData)) != requestedResource.GetDigest().GetSizeBytes()
	if isCompressed {
		if requestedResource.GetCompressor() == repb.Compressor_ZSTD {
			return cachedData, nil
		} else if requestedResource.GetCompressor() == repb.Compressor_IDENTITY {
			return compression.DecompressZstd(nil, cachedData)
		}
	}

	if requestedResource.GetCompressor() == repb.Compressor_ZSTD {
		compressedData := compression.CompressZstd(nil, cachedData)
		return compressedData, nil
	} else if requestedResource.GetCompressor() == repb.Compressor_IDENTITY {
		return cachedData, nil
	}

	return nil, status.InternalErrorf("Cache does not support fetching data with compression %v", requestedResource.GetCompressor())
}

func (c *CompressionCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	switch compressor {
	case repb.Compressor_IDENTITY, repb.Compressor_ZSTD:
		return true
	default:
		return false
	}
}
