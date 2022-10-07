package digest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
	// - "blobs/ac/469db13020c60f8bdf9c89aa4e9a449914db23139b53a24d064f967a51057868/39120"
	// - "uploads/2042a8f9-eade-4271-ae58-f5f6f5a32555/blobs/8afb02ca7aace3ae5cd8748ac589e2e33022b1a4bfd22d5d234c5887e270fe9c/17997850"
	uploadRegex      = regexp.MustCompile(`^(?:(?:(?P<instance_name>.*)/)?uploads/(?P<uuid>[a-f0-9-]{36})/)?(?P<blob_type>blobs|compressed-blobs/zstd)/(?P<hash>[a-f0-9]{64})/(?P<size>\d+)`)
	downloadRegex    = regexp.MustCompile(`^(?:(?P<instance_name>.*)/)?(?P<blob_type>blobs|compressed-blobs/zstd)/(?P<hash>[a-f0-9]{64})/(?P<size>\d+)`)
	actionCacheRegex = regexp.MustCompile(`^(?:(?P<instance_name>.*)/)?(?P<blob_type>blobs|compressed-blobs/zstd)/ac/(?P<hash>[a-f0-9]{64})/(?P<size>\d+)`)
)

type ResourceName struct {
	rn *rspb.ResourceName
}

func NewResourceName(d *repb.Digest, instanceName string) *ResourceName {
	return &ResourceName{
		rn: &rspb.ResourceName{
			Digest:       d,
			InstanceName: instanceName,
			Compressor:   repb.Compressor_IDENTITY,
		},
	}
}

func (r *ResourceName) GetDigest() *repb.Digest {
	return r.rn.GetDigest()
}

func (r *ResourceName) GetInstanceName() string {
	return r.rn.GetInstanceName()
}

func (r *ResourceName) GetCompressor() repb.Compressor_Value {
	return r.rn.GetCompressor()
}

func (r *ResourceName) SetCompressor(compressor repb.Compressor_Value) {
	r.rn.Compressor = compressor
}

// DownloadString returns a string representing the resource name for download
// purposes.
func (r *ResourceName) DownloadString() string {
	// Normalize slashes, e.g. "//foo/bar//"" becomes "/foo/bar".
	instanceName := filepath.Join(filepath.SplitList(r.GetInstanceName())...)
	return fmt.Sprintf(
		"%s/%s/%s/%d",
		instanceName, blobTypeSegment(r.GetCompressor()),
		r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
}

// UploadString returns a string representing the resource name for upload
// purposes.
func (r *ResourceName) UploadString() (string, error) {
	// Normalize slashes, e.g. "//foo/bar//"" becomes "/foo/bar".
	instanceName := filepath.Join(filepath.SplitList(r.GetInstanceName())...)
	u, err := guuid.NewRandom()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s/uploads/%s/%s/%s/%d",
		instanceName, u.String(), blobTypeSegment(r.GetCompressor()),
		r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes(),
	), nil
}

func CacheTypeToPrefix(cacheType rspb.CacheType) string {
	switch cacheType {
	case rspb.CacheType_CAS:
		return ""
	case rspb.CacheType_AC:
		return "ac"
	default:
		alert.UnexpectedEvent("unknown_cache_type", "type: %v", cacheType)
		return "unknown"
	}
}

// Key is a representation of a digest that can be used as a map key.
type Key struct {
	Hash      string
	SizeBytes int64
}

func NewKey(digest *repb.Digest) Key {
	return Key{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()}
}
func (dk Key) ToDigest() *repb.Digest {
	return &repb.Digest{Hash: dk.Hash, SizeBytes: dk.SizeBytes}
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

// AddInvocationIDToDigest combines the hash of the input digest and input invocationID and re-hash.
// This is only to be used for failed action results.
func AddInvocationIDToDigest(digest *repb.Digest, invocationID string) (*repb.Digest, error) {
	if digest == nil {
		return nil, status.FailedPreconditionError("nil digest")
	}
	h := sha256.New()
	h.Write([]byte(digest.Hash))
	h.Write([]byte(invocationID))
	return &repb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: digest.SizeBytes,
	}, nil
}

func isResourceName(url string, matcher *regexp.Regexp) bool {
	return matcher.MatchString(url)
}

func parseResourceName(resourceName string, matcher *regexp.Regexp) (*ResourceName, error) {
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
		return nil, status.InvalidArgumentErrorf("Unparsable resource name: %s", resourceName)
	}
	if hash == "" {
		return nil, status.InvalidArgumentErrorf("Unparsable resource name (empty hash?): %s", resourceName)
	}
	sizeBytes, err := strconv.ParseInt(sizeStr, 10, 0)
	if err != nil {
		return nil, err
	}

	// Set the instance name, if one was present.
	instanceName := ""
	if in, ok := result["instance_name"]; ok {
		instanceName = in
	}

	// Determine compression level from blob type segment
	blobTypeStr, sizeOK := result["blob_type"]
	if !sizeOK {
		// Should never happen since the regex would not match otherwise.
		return nil, status.InvalidArgumentError(`Unparsable resource name: "/blobs" or "/compressed-blobs/zstd" missing or out of place`)
	}
	compressor := repb.Compressor_IDENTITY
	if blobTypeStr == "compressed-blobs/zstd" {
		compressor = repb.Compressor_ZSTD
	}
	d := &repb.Digest{Hash: hash, SizeBytes: sizeBytes}
	r := NewResourceName(d, instanceName)
	r.SetCompressor(compressor)
	return r, nil
}

func ParseUploadResourceName(resourceName string) (*ResourceName, error) {
	return parseResourceName(resourceName, uploadRegex)
}

func ParseDownloadResourceName(resourceName string) (*ResourceName, error) {
	return parseResourceName(resourceName, downloadRegex)
}

func ParseActionCacheResourceName(resourceName string) (*ResourceName, error) {
	return parseResourceName(resourceName, actionCacheRegex)
}

func IsDownloadResourceName(url string) bool {
	return isResourceName(url, downloadRegex)
}

func IsActionCacheResourceName(url string) bool {
	return isResourceName(url, actionCacheRegex)
}

func blobTypeSegment(compressor repb.Compressor_Value) string {
	if compressor == repb.Compressor_ZSTD {
		return "compressed-blobs/zstd"
	}
	return "blobs"
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

func Parse(str string) (*repb.Digest, error) {
	dParts := strings.SplitN(str, "/", 2)
	if len(dParts) != 2 || len(dParts[0]) != 64 {
		return nil, status.FailedPreconditionErrorf("Error parsing digest %q: should be of form 'f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142'", str)
	}
	i, err := strconv.ParseInt(dParts[1], 10, 64)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Error parsing digest %q: %s", str, err)
	}
	return &repb.Digest{
		Hash:      dParts[0],
		SizeBytes: i,
	}, nil
}

// ElementsMatch returns whether two slices contain the same digests, ignoring the order of the elements.
// If there are duplicate elements, the number of appearances of each of them in both lists should match.
func ElementsMatch(s1 []*repb.Digest, s2 []*repb.Digest) bool {
	if len(s1) != len(s2) {
		return false
	}

	foundS1 := make(map[*repb.Digest]int, len(s1))
	for _, d := range s1 {
		foundS1[d]++
	}

	for _, d := range s2 {
		var numOccur int
		var inS1 bool
		if numOccur, inS1 = foundS1[d]; !inS1 {
			return false
		}
		if numOccur == 1 {
			delete(foundS1, d)
		} else {
			foundS1[d]--
		}
	}

	return true
}

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val & 0xff)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}
}

type Generator struct {
	randMaker *randomDataMaker
	mu        sync.Mutex
}

// RandomGenerator returns a digest sample generator for use in testing tools.
func RandomGenerator(seed int64) *Generator {
	return &Generator{
		randMaker: &randomDataMaker{rand.NewSource(seed)},
		mu:        sync.Mutex{},
	}
}

func (g *Generator) RandomDigestReader(sizeBytes int64) (*repb.Digest, io.ReadSeeker, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Read some random bytes.
	buf := new(bytes.Buffer)
	if _, err := io.CopyN(buf, g.randMaker, sizeBytes); err != nil {
		return nil, nil, err
	}
	readSeeker := bytes.NewReader(buf.Bytes())

	// Compute a digest for the random bytes.
	d, err := Compute(readSeeker)
	if err != nil {
		return nil, nil, err
	}
	if _, err := readSeeker.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	return d, readSeeker, nil
}

func (g *Generator) RandomDigestBuf(sizeBytes int64) (*repb.Digest, []byte, error) {
	d, rs, err := g.RandomDigestReader(sizeBytes)
	if err != nil {
		return nil, nil, err
	}
	buf, err := io.ReadAll(rs)
	if err != nil {
		return nil, nil, err
	}
	return d, buf, nil
}
