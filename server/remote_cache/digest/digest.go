package digest

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	guuid "github.com/google/uuid"
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

type InstanceNameDigest struct {
	*repb.Digest
	instanceName string
}

func NewInstanceNameDigest(d *repb.Digest, instanceName string) *InstanceNameDigest {
	return &InstanceNameDigest{
		Digest:       d,
		instanceName: instanceName,
	}
}

func (i *InstanceNameDigest) GetInstanceName() string {
	return i.instanceName
}

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

func DownloadResourceName(d *repb.Digest, instanceName string) string {
	// Haven't found docs on what a valid instance name looks like. But generally
	// seems like a string, possibly separated by "/".

	// Ensure there is no trailing slash and path has components.
	instanceName = filepath.Join(filepath.SplitList(instanceName)...)
	return fmt.Sprintf("%s/blobs/%s/%d", instanceName, d.GetHash(), d.GetSizeBytes())
}

func UploadResourceName(d *repb.Digest, instanceName string) (string, error) {
	// Haven't found docs on what a valid instance name looks like. But generally
	// seems like a string, possibly separated by "/".

	// Ensure there is no trailing slash and path has components.
	instanceName = filepath.Join(filepath.SplitList(instanceName)...)

	u, err := guuid.NewRandom()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", instanceName, u.String(), d.GetHash(), d.GetSizeBytes()), nil
}
