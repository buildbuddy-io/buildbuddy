package digest

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/zeebo/blake3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	guuid "github.com/google/uuid"
	gcodes "google.golang.org/grpc/codes"
	gmetadata "google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"
)

type resourceNameType int

const (
	EmptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	EmptyHash   = ""

	uploadResourceName resourceNameType = iota
	downloadResourceName
)

var (
	knownDigestFunctions = []repb.DigestFunction_Value{
		repb.DigestFunction_SHA256,
		repb.DigestFunction_SHA384,
		repb.DigestFunction_SHA512,
		repb.DigestFunction_SHA1,
		repb.DigestFunction_BLAKE3,
	}
)

func SupportedDigestFunctions() []repb.DigestFunction_Value {
	return knownDigestFunctions
}

type ResourceName struct {
	rn *rspb.ResourceName
}

// Prefer either CASResourceNameFromProto or ACResourceNameFromProto.
func ResourceNameFromProto(in *rspb.ResourceName) *ResourceName {
	rn := in.CloneVT()
	// TODO(tylerw): remove once digest function is explicit everywhere.
	if rn.GetDigestFunction() == repb.DigestFunction_UNKNOWN {
		rn.DigestFunction = repb.DigestFunction_SHA256
	}
	return &ResourceName{
		rn: rn,
	}
}

func CASResourceNameFromProto(in *rspb.ResourceName) (*CASResourceName, error) {
	if in.GetCacheType() != rspb.CacheType_CAS {
		return nil, status.FailedPreconditionErrorf("ResourceName is not a CAS resource name: %s", in)
	}
	return &CASResourceName{*ResourceNameFromProto(in)}, nil
}

func ACResourceNameFromProto(in *rspb.ResourceName) (*ACResourceName, error) {
	if in.GetCacheType() != rspb.CacheType_AC {
		return nil, status.FailedPreconditionErrorf("ResourceName is not an AC resource name: %s", in)
	}
	return &ACResourceName{*ResourceNameFromProto(in)}, nil
}

// Prefer either NewCASResourceName or NewACResourceName.
func NewResourceName(d *repb.Digest, instanceName string, cacheType rspb.CacheType, digestFunction repb.DigestFunction_Value) *ResourceName {
	if digestFunction == repb.DigestFunction_UNKNOWN {
		digestFunction = InferOldStyleDigestFunctionInDesperation(d)
	}
	return &ResourceName{
		rn: &rspb.ResourceName{
			Digest:         d,
			InstanceName:   instanceName,
			Compressor:     repb.Compressor_IDENTITY,
			CacheType:      cacheType,
			DigestFunction: digestFunction,
		},
	}
}

func NewCASResourceName(d *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) *CASResourceName {
	return &CASResourceName{*NewResourceName(d, instanceName, rspb.CacheType_CAS, digestFunction)}
}

func NewACResourceName(d *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) *ACResourceName {
	return &ACResourceName{*NewResourceName(d, instanceName, rspb.CacheType_AC, digestFunction)}
}

func (r *ResourceName) CheckCAS() (*CASResourceName, error) {
	if r.rn.GetCacheType() != rspb.CacheType_CAS {
		return nil, status.FailedPreconditionErrorf("ResourceName is not a CAS resource name: %s", r.rn)
	}
	return &CASResourceName{*r}, nil
}

func (r *ResourceName) CheckAC() (*ACResourceName, error) {
	if r.rn.GetCacheType() != rspb.CacheType_AC {
		return nil, status.FailedPreconditionErrorf("ResourceName is not an AC resource name: %s", r.rn)
	}
	return &ACResourceName{*r}, nil
}

func (r *ResourceName) ToProto() *rspb.ResourceName {
	return r.rn
}

func (r *ResourceName) GetDigest() *repb.Digest {
	return r.rn.GetDigest()
}

func (r *ResourceName) GetDigestFunction() repb.DigestFunction_Value {
	return r.rn.GetDigestFunction()
}

func (r *ResourceName) GetInstanceName() string {
	return r.rn.GetInstanceName()
}

func (r *ResourceName) GetCacheType() rspb.CacheType {
	return r.rn.GetCacheType()
}

func (r *ResourceName) GetCompressor() repb.Compressor_Value {
	return r.rn.GetCompressor()
}

func (r *ResourceName) SetCompressor(compressor repb.Compressor_Value) {
	r.rn.Compressor = compressor
}

func (r *ResourceName) IsEmpty() bool {
	return IsEmptyHash(r.rn.GetDigest(), r.rn.GetDigestFunction())
}

func isLowerHex(s string) bool {
	for _, ch := range s {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func (r *ResourceName) Validate() error {
	d := r.rn.GetDigest()
	if d == nil {
		return status.InvalidArgumentError("Invalid (nil) Digest")
	}
	if d.GetSizeBytes() < 0 {
		return status.InvalidArgumentErrorf("Invalid (negative) digest size")
	}
	if d.GetSizeBytes() == int64(0) {
		if r.IsEmpty() {
			return nil
		}
		return status.InvalidArgumentError("Invalid (zero-length) SHA256 hash")
	}
	hash := d.GetHash()
	if expected := hashLength(r.GetDigestFunction()); len(hash) != expected {
		return status.InvalidArgumentErrorf("Invalid length hash. Expected len %v for %s function. Got %v",
			expected, r.GetDigestFunction().String(), len(hash))
	}
	if !isLowerHex(hash) {
		return status.InvalidArgumentError("Hash isn't all lower case hex characters.")
	}
	return nil
}

type CASResourceName struct {
	ResourceName
}

// DownloadString returns a string representing the resource name for download
// purposes.
func (r *CASResourceName) DownloadString() string {
	// Normalize slashes, e.g. "//foo/bar//"" becomes "/foo/bar".
	instanceName := filepath.Join(filepath.SplitList(r.GetInstanceName())...)
	if isOldStyleDigestFunction(r.rn.DigestFunction) {
		return instanceName + "/" + blobTypeSegment(r.GetCompressor()) + "/" + r.GetDigest().GetHash() + "/" + strconv.FormatInt(r.GetDigest().GetSizeBytes(), 10)
	}
	return instanceName + "/" + blobTypeSegment(r.GetCompressor()) + "/" + strings.ToLower(r.rn.DigestFunction.String()) + "/" + r.GetDigest().GetHash() + "/" + strconv.FormatInt(r.GetDigest().GetSizeBytes(), 10)
}

// NewUploadString returns a new string representing the resource name for
// upload purposes each time it is called.
func (r *CASResourceName) NewUploadString() string {
	// Normalize slashes, e.g. "//foo/bar//"" becomes "/foo/bar".
	instanceName := filepath.Join(filepath.SplitList(r.GetInstanceName())...)
	u := guuid.New().String()
	if isOldStyleDigestFunction(r.rn.DigestFunction) {
		return instanceName + "/uploads/" + u + "/" + blobTypeSegment(r.GetCompressor()) + "/" + r.GetDigest().GetHash() + "/" + strconv.FormatInt(r.GetDigest().GetSizeBytes(), 10)
	}
	return instanceName + "/uploads/" + u + "/" + blobTypeSegment(r.GetCompressor()) + "/" + strings.ToLower(r.rn.DigestFunction.String()) + "/" + r.GetDigest().GetHash() + "/" + strconv.FormatInt(r.GetDigest().GetSizeBytes(), 10)
}

type ACResourceName struct {
	ResourceName
}

// ActionCacheString returns a string representing the resource name for in
// the action cache. This is BuildBuddy specific.
func (r *ACResourceName) ActionCacheString() string {
	// Normalize slashes, e.g. "//foo/bar//"" becomes "/foo/bar".
	instanceName := filepath.Join(filepath.SplitList(r.GetInstanceName())...)
	if isOldStyleDigestFunction(r.rn.DigestFunction) {
		return instanceName + "/" + blobTypeSegment(r.GetCompressor()) + "/ac/" + r.GetDigest().GetHash() + "/" + strconv.FormatInt(r.GetDigest().GetSizeBytes(), 10)
	}
	return instanceName + "/" + blobTypeSegment(r.GetCompressor()) + "/ac/" + strings.ToLower(r.rn.DigestFunction.String()) + "/" + r.GetDigest().GetHash() + "/" + strconv.FormatInt(r.GetDigest().GetSizeBytes(), 10)
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

func ResourceNames(cacheType rspb.CacheType, remoteInstanceName string, digests []*repb.Digest) []*rspb.ResourceName {
	rns := make([]*rspb.ResourceName, 0, len(digests))
	for _, d := range digests {
		rns = append(rns, &rspb.ResourceName{
			Digest:       d,
			InstanceName: remoteInstanceName,
			Compressor:   repb.Compressor_IDENTITY,
			CacheType:    cacheType,
		})
	}
	return rns
}

func ResourceNameMap(cacheType rspb.CacheType, remoteInstanceName string, digestMap map[*repb.Digest][]byte) map[*rspb.ResourceName][]byte {
	rnMap := make(map[*rspb.ResourceName][]byte, len(digestMap))
	for d, data := range digestMap {
		rn := &rspb.ResourceName{
			Digest:       d,
			InstanceName: remoteInstanceName,
			Compressor:   repb.Compressor_IDENTITY,
			CacheType:    cacheType,
		}
		rnMap[rn] = data
	}
	return rnMap
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

func ComputeForMessage(in proto.Message, digestType repb.DigestFunction_Value) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	return Compute(bytes.NewReader(data), digestType)
}

func HashForDigestType(digestType repb.DigestFunction_Value) (hash.Hash, error) {
	switch digestType {
	case repb.DigestFunction_SHA1:
		return sha1.New(), nil
	case repb.DigestFunction_SHA256:
		return sha256.New(), nil
	case repb.DigestFunction_SHA384:
		return sha512.New384(), nil
	case repb.DigestFunction_SHA512:
		return sha512.New(), nil
	case repb.DigestFunction_BLAKE3:
		return blake3.New(), nil
	case repb.DigestFunction_UNKNOWN:
		// TODO(tylerw): make this a warning when clients support this.
		// log.Warningf("Digest function was unset: defaulting to SHA256")
		return sha256.New(), nil
	default:
		return nil, status.UnimplementedErrorf("No support for digest type: %s", digestType)
	}
}

func InferOldStyleDigestFunctionInDesperation(d *repb.Digest) repb.DigestFunction_Value {
	switch len(d.GetHash()) {
	case sha1.Size * 2:
		return repb.DigestFunction_SHA1
	case md5.Size * 2:
		return repb.DigestFunction_MD5
	case sha256.Size * 2:
		return repb.DigestFunction_SHA256
	case sha512.Size384 * 2:
		return repb.DigestFunction_SHA384
	case sha512.Size * 2:
		return repb.DigestFunction_SHA512
	default:
		return repb.DigestFunction_UNKNOWN
	}
}

func IsEmptyHash(d *repb.Digest, digestFunction repb.DigestFunction_Value) bool {
	switch digestFunction {
	case repb.DigestFunction_SHA1:
		return d.GetHash() == "da39a3ee5e6b4b0d3255bfef95601890afd80709"
	case repb.DigestFunction_MD5:
		return d.GetHash() == "d41d8cd98f00b204e9800998ecf8427e"
	case repb.DigestFunction_SHA256:
		return d.GetHash() == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	case repb.DigestFunction_SHA384:
		return d.GetHash() == "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"
	case repb.DigestFunction_SHA512:
		return d.GetHash() == "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
	case repb.DigestFunction_BLAKE3:
		return d.GetHash() == "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	default:
		return false
	}
}

func hashLength(digestFunction repb.DigestFunction_Value) int {
	switch digestFunction {
	case repb.DigestFunction_BLAKE3:
		return 32 * 2
	case repb.DigestFunction_SHA256:
		return sha256.Size * 2
	case repb.DigestFunction_SHA384:
		return sha512.Size384 * 2
	case repb.DigestFunction_SHA512:
		return sha512.Size * 2
	case repb.DigestFunction_SHA1:
		return sha1.Size * 2
	case repb.DigestFunction_MD5:
		return md5.Size * 2
	default:
		return -1
	}
}

func isOldStyleDigestFunction(digestFunction repb.DigestFunction_Value) bool {
	switch digestFunction {
	case repb.DigestFunction_SHA1:
		return true
	case repb.DigestFunction_MD5:
		return true
	case repb.DigestFunction_SHA256:
		return true
	case repb.DigestFunction_SHA384:
		return true
	case repb.DigestFunction_SHA512:
		return true
	default:
		return false
	}
}

var blake3Hashes = sync.Pool{New: func() any { return blake3.New() }}

func Compute(in io.Reader, digestType repb.DigestFunction_Value) (*repb.Digest, error) {
	var h hash.Hash
	if digestType == repb.DigestFunction_BLAKE3 {
		// blake3.New() allocates over 10KiB. Use a pool to avoid this.
		// sha256.New() allocates under 360 bytes, so we don't pool it.
		// The other hash functions are not used frequently enough to pool.
		h = blake3Hashes.Get().(hash.Hash)
		defer func() {
			h.Reset()
			blake3Hashes.Put(h)
		}()
	} else {
		var err error
		h, err = HashForDigestType(digestType)
		if err != nil {
			return nil, err
		}
	}

	// Read file in 32KB chunks (default)
	n, err := io.Copy(h, in)
	if err != nil {
		return nil, err
	}
	return &repb.Digest{
		Hash:      hex.EncodeToString(h.Sum(nil)),
		SizeBytes: n,
	}, nil
}

func ComputeForFile(path string, digestType repb.DigestFunction_Value) (*repb.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Compute(f, digestType)
}

func parseResourceName(resourceName string, cacheType rspb.CacheType, resourceType resourceNameType) (*ResourceName, error) {
	pieces := strings.Split(resourceName, "/")

	// Need at least 2 slashes for CAS: blobs/hash/size
	// 3 for AC: blobs/ac/hash/size
	// 4 for upload: uploads/uuid/blobs/hash/size
	if len(pieces) < 3 || (cacheType == rspb.CacheType_AC && len(pieces) < 4) || (resourceType == uploadResourceName && len(pieces) < 5) {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, not enough pieces: %s", resourceName)
	}

	// Parse back-to-front to avoid having to find the end of the instance name
	// The last piece must be the size (in bytes)
	pieceIdx := len(pieces) - 1
	sizeBytes, err := strconv.ParseInt(pieces[pieceIdx], 10, 64)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, invalid size: %s", resourceName)
	}
	if sizeBytes < 0 {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, negative size: %s", resourceName)
	}

	// The second-to-last piece is the hash
	pieceIdx--
	hash := pieces[pieceIdx]
	d := &repb.Digest{Hash: hash, SizeBytes: sizeBytes}
	inferredDigestFunction := InferOldStyleDigestFunctionInDesperation(d)
	digestFunction := inferredDigestFunction
	if inferredDigestFunction == repb.DigestFunction_UNKNOWN {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, invalid hash (wrong length): %s", resourceName)
	}
	if !isLowerHex(hash) {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, invalid hash (bad character(s)): %s", resourceName)
	}

	// The next piece may be the digest function
	pieceIdx--
	piece := pieces[pieceIdx]
	switch piece {
	case "blake3":
		digestFunction = repb.DigestFunction_BLAKE3
	case "sha1":
		digestFunction = repb.DigestFunction_SHA1
	case "sha512":
		digestFunction = repb.DigestFunction_SHA512
	case "sha384":
		digestFunction = repb.DigestFunction_SHA384
	case "sha256":
		digestFunction = repb.DigestFunction_SHA256
	default:
		pieceIdx++
	}
	if len(hash) != hashLength(digestFunction) {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, invalid hash (wrong length): %s", resourceName)
	}
	pieceIdx--
	if pieceIdx >= 0 {
		piece = pieces[pieceIdx]
	}

	// The next piece must be "ac" for AC entries
	if cacheType == rspb.CacheType_AC {
		if piece != "ac" {
			return nil, status.InvalidArgumentErrorf("Unparseable Action Cache resource name, missing 'ac' blob type: %s", resourceName)
		}
		pieceIdx--
		if pieceIdx >= 0 {
			piece = pieces[pieceIdx]
		}
	}

	// The next piece must be "blobs" or "zstd"
	compressor := repb.Compressor_IDENTITY
	if piece == "zstd" {
		compressor = repb.Compressor_ZSTD
	} else if piece != "blobs" {
		return nil, status.InvalidArgumentErrorf("Unparseable resource name, invalid compressed blob type: %s", resourceName)
	}

	// If this is a compressed blob, the next piece must be "compressed-blobs"
	// We have proceeded far enough that we need to check the index now
	pieceIdx--
	piece = ""
	if pieceIdx >= 0 {
		piece = pieces[pieceIdx]
	}
	if compressor != repb.Compressor_IDENTITY {
		if piece != "compressed-blobs" {
			return nil, status.InvalidArgumentErrorf("Unparseable resource name, invalid compressed blob type: %s", resourceName)
		}
		pieceIdx--
		piece = ""
		if pieceIdx >= 0 {
			piece = pieces[pieceIdx]
		}
	}

	// If this is an upload, the next two pieces must be "uploads" and a UUID
	if resourceType == uploadResourceName {
		if guuid.Validate(piece) != nil {
			return nil, status.InvalidArgumentErrorf("Unparseable upload resource name, invalid UUID: %s", resourceName)
		}
		pieceIdx--
		if pieceIdx < 0 {
			return nil, status.InvalidArgumentErrorf("Unparseable upload resource name name, not enough pieces: %s", resourceName)
		}
		piece = pieces[pieceIdx]
		if piece != "uploads" {
			return nil, status.InvalidArgumentErrorf("Unparseable upload resource name, missing 'uploads': %s", resourceName)
		}
		pieceIdx--
		// No need to advance "piece", remainder is instance name
	}

	// Everything remaining is the instance name
	instanceName := ""
	if pieceIdx >= 0 {
		instanceName = strings.Join(pieces[0:pieceIdx+1], "/")
	}

	r := NewResourceName(d, instanceName, cacheType, digestFunction)
	r.SetCompressor(compressor)
	return r, nil
}

func ParseUploadResourceName(resourceName string) (*CASResourceName, error) {
	rn, err := parseResourceName(resourceName, rspb.CacheType_CAS, uploadResourceName)
	if err != nil {
		return nil, err
	}
	return rn.CheckCAS()
}

func ParseDownloadResourceName(resourceName string) (*CASResourceName, error) {
	rn, err := parseResourceName(resourceName, rspb.CacheType_CAS, downloadResourceName)
	if err != nil {
		return nil, err
	}
	return rn.CheckCAS()
}

func ParseActionCacheResourceName(resourceName string) (*ACResourceName, error) {
	rn, err := parseResourceName(resourceName, rspb.CacheType_AC, downloadResourceName)
	if err != nil {
		return nil, err
	}
	return rn.CheckAC()
}

func blobTypeSegment(compressor repb.Compressor_Value) string {
	if compressor == repb.Compressor_ZSTD {
		return "compressed-blobs/zstd"
	}
	return "blobs"
}

func IsCacheDebuggingEnabled(ctx context.Context) bool {
	if hdrs := gmetadata.ValueFromIncomingContext(ctx, "debug-cache-hits"); len(hdrs) > 0 {
		if strings.ToLower(strings.TrimSpace(hdrs[0])) == "true" {
			return true
		}
	}
	return false
}

func MissingDigestError(d *repb.Digest) error {
	if d == nil {
		log.Infof("MissingDigestError called with nil digest. Stack trace:\n%s", string(debug.Stack()))
	}

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

// String returns the digest formatted as "HASH/SIZE" or the string "<nil>"
// if the digest is nil.
//
// Note: this is intended mainly for logging - to get a representation of a
// digest suitable for use as a map key, use NewKey instead.
func String(d *repb.Digest) string {
	if d == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s/%d", d.Hash, d.SizeBytes)
}

// Equal returns whether two digests are exactly equal, including the size.
func Equal(d1 *repb.Digest, d2 *repb.Digest) bool {
	return d1.GetHash() == d2.GetHash() && d1.GetSizeBytes() == d2.GetSizeBytes()
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

// Diff returns the differences between two slices of digests. If the slices differ in the count of a non-unique element,
// that does not count as a difference
//
// missingFromS1 contains the digests that are in S2 but not S1
// missingFromS2 contains the digests that are in S1 but not S2
func Diff(s1 []*repb.Digest, s2 []*repb.Digest) (missingFromS1 []*repb.Digest, missingFromS2 []*repb.Digest) {
	missingFromS1 = make([]*repb.Digest, 0)
	missingFromS2 = make([]*repb.Digest, 0)

	s1Set := make(map[Key]*repb.Digest, len(s1))
	for _, d := range s1 {
		s1Set[NewKey(d)] = d
	}

	s2Set := make(map[Key]struct{}, len(s2))
	for _, d := range s2 {
		k := NewKey(d)
		s2Set[k] = struct{}{}

		if _, inS1 := s1Set[k]; !inS1 {
			missingFromS1 = append(missingFromS1, d)
		}
	}

	for k, d := range s1Set {
		if _, inS2 := s2Set[k]; !inS2 {
			missingFromS2 = append(missingFromS2, d)
		}
	}

	return missingFromS1, missingFromS2
}

func (g *Generator) fill(p []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	todo := len(p)
	offset := 0
	compressionPercent := int64(g.compressionRatio * 100)
	for {
		// Generate a new random int64 (8 bytes) if we haven't generated one
		// yet, or with a percent chance given by the compression ratio. This is
		// a *very* rough way to generate blobs with the average compression
		// ratios that we see in practice.
		var val int64
		if g.src.Int63()%100 >= compressionPercent {
			val = g.src.Int63()
		}
		for range 8 {
			if todo == 0 {
				return
			}
			p[offset] = byte(val)
			todo--
			offset++
			val >>= 8
		}
	}
}

type Generator struct {
	src              rand.Source
	compressionRatio float64
	mu               sync.Mutex
}

// RandomGenerator returns a digest sample generator for use in testing tools.
// It generates digests with compression ratios similar to what we see in
// practice.
func RandomGenerator(seed int64) *Generator {
	return &Generator{
		src:              rand.NewSource(seed),
		compressionRatio: 0.7,
	}
}

// UniformRandomGenerator generates uniformly random, incompressible digests,
// for use in testing. The data generated does not look realistic, but it is
// useful in cases where unique digests are needed.
func UniformRandomGenerator(seed int64) *Generator {
	return &Generator{
		src: rand.NewSource(seed),
	}
}

func (g *Generator) RandomDigestReader(sizeBytes int64) (*repb.Digest, io.ReadSeeker, error) {
	d, buf, err := g.RandomDigestBuf(sizeBytes)
	if err != nil {
		return nil, nil, err
	}
	return d, bytes.NewReader(buf), nil
}

func (g *Generator) RandomDigestBuf(sizeBytes int64) (*repb.Digest, []byte, error) {
	// Read some random bytes.
	buf := make([]byte, sizeBytes)
	g.fill(buf)

	// Compute a digest for the random bytes.
	d, err := Compute(bytes.NewReader(buf), repb.DigestFunction_SHA256)
	if err != nil {
		return nil, nil, err
	}
	return d, buf, nil
}

// ParseFunction parses a digest function name to a proto.
func ParseFunction(s string) (repb.DigestFunction_Value, error) {
	if df, ok := repb.DigestFunction_Value_value[strings.ToUpper(s)]; ok {
		return repb.DigestFunction_Value(df), nil
	}
	return 0, status.InvalidArgumentErrorf("unknown digest function %q", s)
}
