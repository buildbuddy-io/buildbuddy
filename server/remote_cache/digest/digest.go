package digest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/golang/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	hashKeyLength = 64
	EmptySha256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	EmptyHash     = ""
)

var (
	// Cache keys must be:
	//  - lower case
	//  - ascii
	//  - a sha256 sum
	hashKeyRegex = regexp.MustCompile("^[a-f0-9]{64}$")
)

func Validate(d *repb.Digest) (string, error) {
	if d == nil {
		return "", status.InvalidArgumentError("Invalid (nil) D")
	}
	if d.SizeBytes == int64(0) {
		if d.Hash == EmptySha256 {
			return "", status.OK()
		}
		return "", status.InvalidArgumentError("Invalid (zero-length) SHA256 hash")
	}

	if len(d.Hash) != hashKeyLength {
		return "", status.InvalidArgumentError(fmt.Sprintf("Hash length was %d, expected %d", len(d.Hash), hashKeyLength))
	}

	if !hashKeyRegex.MatchString(d.Hash) {
		return "", status.InvalidArgumentError("Malformed hash")
	}
	return d.Hash, nil
}

func ComputeForMessage(in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	return Compute(bytes.NewReader(data))
}

func Compute(in io.Reader) (*repb.Digest, error) {
	h := sha256.New()
	// Read file in 32KB chunks (default)
	n, err := io.Copy(h, in)
	if err != nil {
		return nil, err
	}
	return &repb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: n,
	}, nil
}

func GetResourceName(d *repb.Digest) string {
	return fmt.Sprintf("/blobs/%s/%d", d.GetHash(), d.GetSizeBytes())
}

// DigestCache is a cache that uses digests as keys instead of strings.
// Adding a WithPrefix method allows us to separate AC content from CAS
// content.
type DigestCache struct {
	cache  interfaces.Cache
	prefix string
}

func NewDigestCache(cache interfaces.Cache) *DigestCache {
	return &DigestCache{
		cache:  cache,
		prefix: "",
	}
}

func (c *DigestCache) key(ctx context.Context, d *repb.Digest) (string, error) {
	hash, err := Validate(d)
	if err != nil {
		return "", err
	}
	return perms.UserPrefixFromContext(ctx) + c.prefix + hash, nil
}

func (c *DigestCache) WithPrefix(prefix string) interfaces.DigestCache {
	return &DigestCache{
		cache:  c.cache,
		prefix: prefix,
	}
}

func (c *DigestCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	k, err := c.key(ctx, d)
	if err != nil {
		return false, err
	}
	return c.cache.Contains(ctx, k)
}

func (c *DigestCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	keys := make([]string, 0, len(digests))
	digestsByKey := make(map[string]*repb.Digest, len(digests))
	for _, d := range digests {
		k, err := c.key(ctx, d)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
		digestsByKey[k] = d
	}
	foundMap, err := c.cache.ContainsMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	response := make(map[*repb.Digest]bool, len(keys))
	for _, k := range keys {
		d := digestsByKey[k]
		found, ok := foundMap[k]
		if !ok {
			return nil, status.InternalError("ContainsMulti inconsistency")
		}
		response[d] = found
	}
	return response, nil
}

func (c *DigestCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	return c.cache.Get(ctx, k)
}

func (c *DigestCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	return c.cache.Set(ctx, k, data)
}

func (c *DigestCache) Delete(ctx context.Context, d *repb.Digest) error {
	k, err := c.key(ctx, d)
	if err != nil {
		return err
	}
	return c.cache.Delete(ctx, k)
}

// Low level interface used for seeking and stream-writing.
func (c *DigestCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.Reader, error) {
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	length := d.GetSizeBytes()
	return c.cache.Reader(ctx, k, offset, length)
}

func (c *DigestCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	k, err := c.key(ctx, d)
	if err != nil {
		return nil, err
	}
	return c.cache.Writer(ctx, k)
}

func (c *DigestCache) Start() error {
	return nil
}

func (c *DigestCache) Stop() error {
	return nil
}
