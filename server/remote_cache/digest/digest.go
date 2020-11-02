package digest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	guuid "github.com/google/uuid"
	gcodes "google.golang.org/grpc/codes"
	gmetadata "google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"
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

	// Matches:
	// - "blobs/469db13020c60f8bdf9c89aa4e9a449914db23139b53a24d064f967a51057868/39120"
	// - "uploads/2042a8f9-eade-4271-ae58-f5f6f5a32555/blobs/8afb02ca7aace3ae5cd8748ac589e2e33022b1a4bfd22d5d234c5887e270fe9c/17997850"
	uploadRegex   = regexp.MustCompile("^(?:(?:(?P<instance_name>.*)/)?uploads/(?P<uuid>[a-f0-9-]{36})/)?blobs/(?P<hash>[a-f0-9]{64})/(?P<size>\\d+)")
	downloadRegex = regexp.MustCompile("^(?:(?P<instance_name>.*)/)?blobs/(?P<hash>[a-f0-9]{64})/(?P<size>\\d+)")
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
		return "", status.InvalidArgumentError("Invalid (nil) Digest")
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

func extractDigest(resourceName string, matcher *regexp.Regexp) (string, *repb.Digest, error) {
	match := matcher.FindStringSubmatch(resourceName)
	result := make(map[string]string, len(match))
	for i, name := range matcher.SubexpNames() {
		if i != 0 && name != "" && i < len(match) {
			result[name] = match[i]
		}
	}
	hash, hashOK := result["hash"]
	sizeStr, sizeOK := result["size"]
	if !hashOK || !sizeOK {
		return "", nil, status.InvalidArgumentErrorf("Unparsable resource name: %s", resourceName)
	}
	if hash == "" {
		return "", nil, status.InvalidArgumentErrorf("Unparsable resource name (empty hash?): %s", resourceName)
	}
	sizeBytes, err := strconv.ParseInt(sizeStr, 10, 0)
	if err != nil {
		return "", nil, err
	}

	// Set the instance name, if one was present.
	instanceName := ""
	if in, ok := result["instance_name"]; ok {
		instanceName = in
	}

	return instanceName, &repb.Digest{
		Hash:      hash,
		SizeBytes: sizeBytes,
	}, nil
}

func ExtractDigestFromUploadResourceName(resourceName string) (string, *repb.Digest, error) {
	return extractDigest(resourceName, uploadRegex)
}

func ExtractDigestFromDownloadResourceName(resourceName string) (string, *repb.Digest, error) {
	return extractDigest(resourceName, downloadRegex)
}

// This is probably the wrong place for this, but works for now.
func GetRequestMetadata(ctx context.Context) *repb.RequestMetadata {
	if grpcMD, ok := gmetadata.FromIncomingContext(ctx); ok {
		rmdVals := grpcMD["build.bazel.remote.execution.v2.requestmetadata-bin"]
		for _, rmdVal := range rmdVals {
			rmd := &repb.RequestMetadata{}
			if err := proto.Unmarshal([]byte(rmdVal), rmd); err == nil {
				return rmd
			}
		}
	}
	return nil
}

func GetInvocationIDFromMD(ctx context.Context) string {
	iid := ""
	if rmd := GetRequestMetadata(ctx); rmd != nil {
		iid = rmd.GetToolInvocationId()
	}
	return iid
}

func IsCacheDebuggingEnabled(ctx context.Context) bool {
	if grpcMD, ok := gmetadata.FromIncomingContext(ctx); ok {
		debugCacheHitsValue := grpcMD["debug-cache-hits"]
		if len(debugCacheHitsValue) == 1 {
			if strings.ToLower(strings.TrimSpace(debugCacheHitsValue[0])) == "true" {
				return true
			}
		}
	}
	return false
}

func MissingDigestError(d *repb.Digest) error {
	pf := &errdetails.PreconditionFailure{}
	pf.Violations = append(pf.Violations, &errdetails.PreconditionFailure_Violation{
		Type:    "MISSING",
		Subject: fmt.Sprintf("blobs/%s/%d", d.GetHash(), d.GetSizeBytes()),
	})
	st := gstatus.Newf(gcodes.FailedPrecondition, "Digest %v not found", d)
	if st, err := st.WithDetails(pf); err != nil {
		return status.InternalErrorf("Digest %v not found.", d)
	} else {
		return st.Err()
	}
}
