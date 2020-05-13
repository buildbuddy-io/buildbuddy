package digest

import (
	"crypto/sha256"
	"fmt"
	"io"
	"regexp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
