package digest

import (
	"fmt"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	repb "proto/remote_execution"
)

const (
	hashKeyLength = 64
	EmptySha256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	EmptyHash = ""
)

var (
	// Cache keys must be:
	//  - lower case
	//  - ascii
	//  - a sha256 sum
	hashKeyRegex = regexp.MustCompile("^[a-f0-9]{64}$")
)

func Validate(digest *repb.Digest) (string, error) {
	if digest.SizeBytes == int64(0) {
		if digest.Hash == EmptySha256 {
			return "", status.OK()
		}
		return "", status.InvalidArgumentError("Invalid (zero-length) SHA256 hash")
	}

	if len(digest.Hash) != hashKeyLength {
		return "", status.InvalidArgumentError(fmt.Sprintf("Hash length was %d, expected %d", len(digest.Hash), hashKeyLength))
	}

	if !hashKeyRegex.MatchString(digest.Hash) {
		return "", status.InvalidArgumentError("Malformed hash")
	}
	return digest.Hash, nil
}
