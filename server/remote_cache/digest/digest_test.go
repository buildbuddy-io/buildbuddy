package digest_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

func TestParseDownloadResourceName(t *testing.T) {
	cases := []struct {
		resourceName string
		wantDRN      *digest.CASResourceName
		wantError    error
	}{
		{ // Empty string
			resourceName: "",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Bad hash
			resourceName: "/blobs/invalid_hash/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Hash too long
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982dd/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Hash too short
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Missing size
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Invalid size
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234b",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Trailing slashes
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234////////",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Bad compression type
			resourceName: "/compressed-blobs/unknownCompressionType/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Missing blob-type
			resourceName: "blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Valid, but incorrect hash length
			resourceName: "/blobs/sha256/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // SHA1 hardcoded
			resourceName: "/blobs/sha1/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac", SizeBytes: 1234}, "", repb.DigestFunction_SHA1),
		},
		{ // SHA1
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac", SizeBytes: 1234}, "", repb.DigestFunction_SHA1),
		},
		{ // MD5 -- shortest possible resource name
			resourceName: "blobs/072d9dd55aacaa829d7d1cc9ec8c4b51/1",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b51", SizeBytes: 1}, "", repb.DigestFunction_MD5),
		},
		{ // SHA256
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_SHA256),
		},
		{ // SHA384
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d8d5a2729eb7c46628cf8f765ad7626bc/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d8d5a2729eb7c46628cf8f765ad7626bc", SizeBytes: 1234}, "", repb.DigestFunction_SHA384),
		},
		{ // SHA512
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d8d5a2729eb7c46628cf8f765ad7626bc072d9dd55aacaa82ad7626bccc9ec8c4/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d8d5a2729eb7c46628cf8f765ad7626bc072d9dd55aacaa82ad7626bccc9ec8c4", SizeBytes: 1234}, "", repb.DigestFunction_SHA512),
		},
		{ // BLAKE3
			resourceName: "/blobs/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_BLAKE3),
		},
		{ // ZSTD compression
			resourceName: "/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_SHA256),
		},
		{ // ZSTD compression with instance name
			resourceName: "my_instance_name/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "my_instance_name", repb.DigestFunction_SHA256),
		},
		{ // BLAKE3 and compressed with tricky instance name
			resourceName: "//8/compressed-blobs//lol/deadbeaf/zstd/compressed-blobs/zstd/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "//8/compressed-blobs//lol/deadbeaf/zstd", repb.DigestFunction_BLAKE3),
		},
		{ // SHA256 with no leading '/'
			resourceName: "blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_SHA256),
		},
		{ // ZSTD compression with no leading '/'
			resourceName: "compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_SHA256),
		},
		{ // BLAKE3 & ZSTD compression with no leading '/'
			resourceName: "compressed-blobs/zstd/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantDRN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_BLAKE3),
		},
	}
	for _, tc := range cases {
		drn, err := digest.ParseDownloadResourceName(tc.resourceName)
		if gstatus.Code(err) != gstatus.Code(tc.wantError) {
			t.Errorf("parseResourceName(%q) got err %v; want %v", tc.resourceName, err, tc.wantError)
		}
		if !casrnsEqual(drn, tc.wantDRN) {
			t.Errorf("parseResourceName(%q): got %v+; want %v+", tc.resourceName, drn.DownloadString(), tc.wantDRN.DownloadString())
		}
	}
}

func FuzzParseDownloadResourceName(f *testing.F) {
	f.Add("blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac/1234")
	f.Add("/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Add("//8/compressed-blobs//lol/deadbeaf/zstd/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Fuzz(func(t *testing.T, resourceName string) {
		// Log the problematic input instead of making the user try to find it
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("digest.ParseDownloadResourceName(\"%s\") panicked", resourceName)
			}
		}()

		digest.ParseDownloadResourceName(resourceName)
	})
}

func TestParseUploadResourceName(t *testing.T) {
	cases := []struct {
		resourceName string
		wantURN      *digest.CASResourceName
		wantError    error
	}{
		{ // Invalid UUID (wrong length)
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac12/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Invalid UUID (bad character)
			resourceName: "instance_name/uploads/2148ezf1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Misspelled "uploads"
			resourceName: "instance_name/uplaods/2148ezf1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Missing "uploads"
			resourceName: "instance_name/2148ezf1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Bad blob type
			resourceName: "instance_name/uploads/2148ezf1-aacc-41eb-a31c-22b6da7c7ac1/bloobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Bad hash (too long)
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3ddb134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Bad hash (invalid character)
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4bz180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Bad size
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4bz180ef49acac4a3c2f3ca16a3db134982d/12d34",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Trailing '/'
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4bz180ef49acac4a3c2f3ca16a3db134982d/1234/",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Empty string
			resourceName: "",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Invalid string
			resourceName: strings.Repeat("/", 256),
			wantError:    status.InvalidArgumentError(""),
		},
		{ // SHA1 upload
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/da39a3ee5e6b4b0d3255bfef95601890afd80709/1234",
			wantURN:      digest.NewCASResourceName(&repb.Digest{Hash: "da39a3ee5e6b4b0d3255bfef95601890afd80709", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA1),
		},
		// TODO(iain): this is broken!
		// { // MD5 upload
		// 	resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/d41d8cd98f00b204e9800998ecf8427e/1234",
		// 	wantURN:      digest.NewCASResourceName(&repb.Digest{Hash: "d41d8cd98f00b204e9800998ecf8427e", SizeBytes: 1234}, "instance_name", repb.DigestFunction_MD5),
		// },
		{ // SHA256 upload
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantURN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA256),
		},
		{ // SHA256 upload with compression
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantURN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA256),
		},
		{ // Blake3 upload
			resourceName: "/blake3/zstd/blake3/zstd/this-is-the-instance-name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantURN:      digest.NewCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "/blake3/zstd/blake3/zstd/this-is-the-instance-name", repb.DigestFunction_BLAKE3),
		},
		{ // Blake3 upload with compression
			resourceName: "/blake3/zstd/blake3/zstd/this-is-the-instance-name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/compressed-blobs/zstd/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantURN:      newCompressedCASRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "/blake3/zstd/blake3/zstd/this-is-the-instance-name", repb.DigestFunction_BLAKE3),
		},
	}
	for _, tc := range cases {
		urn, err := digest.ParseUploadResourceName(tc.resourceName)
		if gstatus.Code(err) != gstatus.Code(tc.wantError) {
			t.Errorf("parseResourceName(%q) got err %v; want %v", tc.resourceName, err, tc.wantError)
		}
		if !casrnsEqual(urn, tc.wantURN) {
			t.Errorf("parseResourceName(%q): got %s; want %s", tc.resourceName, urn.DownloadString(), tc.wantURN.DownloadString())
		}
	}
}

func TestActionCacheString(t *testing.T) {
	for _, tc := range []struct {
		name string
		have string
		want string
	}{
		{
			name: "implicit sha256 hash",
			have: "/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			want: "/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		},
		{
			name: "explicit sha256 hash",
			have: "/blobs/ac/sha256/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			want: "/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		},
		{
			name: "implicit sha256 hash with instance name",
			have: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			want: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		},
		{
			name: "blake3 hash",
			have: "/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			want: "/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		},
		{
			name: "blake3 hash with instance name",
			have: "instance_name/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			want: "instance_name/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		},
		{
			name: "normalized instance name slashes",
			have: "//foo//bar//blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			want: "/foo/bar/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			have, err := digest.ParseActionCacheResourceName(tc.have)
			if err != nil {
				t.Fatalf("ParseActionCacheResourceName(%q) got err %v", tc.have, err)
			}
			if have.ActionCacheString() != tc.want {
				t.Errorf("ActionCacheString(%q) got %q; want %q", tc.have, have.ActionCacheString(), tc.want)
			}
		})
	}
}

func FuzzParseUploadResourceName(f *testing.F) {
	f.Add("instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/da39a3ee5e6b4b0d3255bfef95601890afd80709/1234")
	f.Add("instance/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Add("/blake3/zstd/blake3/zstd/this-is-the-instance-name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/compressed-blobs/zstd/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Fuzz(func(t *testing.T, resourceName string) {
		// Log the problematic input instead of making the user try to find it
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("digest.ParseUploadResourceName(\"%s\") panicked", resourceName)
			}
		}()

		digest.ParseUploadResourceName(resourceName)
	})
}

func casrnsEqual(first *digest.CASResourceName, second *digest.CASResourceName) bool {
	if first == nil && second == nil {
		return true
	}
	if first == nil || second == nil {
		return false
	}
	return first.GetDigest().GetHash() == second.GetDigest().GetHash() &&
		first.GetDigest().GetSizeBytes() == second.GetDigest().GetSizeBytes() &&
		first.GetInstanceName() == second.GetInstanceName() &&
		first.GetCompressor() == second.GetCompressor()
}

func newCompressedCASRN(d *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) *digest.CASResourceName {
	rn := digest.NewCASResourceName(d, instanceName, digestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return rn
}

func TestParseActionCacheResourceName(t *testing.T) {
	cases := []struct {
		resourceName string
		wantACRN     *digest.ACResourceName
		wantError    error
	}{
		{ // Hash too short
			resourceName: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Hash too long
			resourceName: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db13498282/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Invalid hash character
			resourceName: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9eg8c4b5180ef49acac4a3c2f3ca16a3db134982/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Invalid cache type
			resourceName: "instance_name/blobs/notac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Invalid size
			resourceName: "instance_name/blobs/notac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/123d4",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // No size
			resourceName: "instance_name/blobs/notac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Empty string
			resourceName: "",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Not enough pieces
			resourceName: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // Missing blob-type
			resourceName: "ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantError:    status.InvalidArgumentError(""),
		},
		{ // SHA256 AC resource name with instance name
			resourceName: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     digest.NewACResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA256),
		},
		{ // SHA512 AC resource name with instance name
			resourceName: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     digest.NewACResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA512),
		},
		{ // Compressed SHA256 AC resource name with instance name
			resourceName: "instance_name/compressed-blobs/zstd/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     newCompressedACRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA256),
		},
		{ // Blake3 AC resource name with instance name
			resourceName: "instance_name/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     digest.NewACResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_BLAKE3),
		},
		{ // SHA256 AC resource name with no instance name
			resourceName: "/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     digest.NewACResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_SHA256),
		},
		{ // SHA256 AC resource name with tricky instance name
			resourceName: "/sha256/deadbeef////blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     digest.NewACResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "/sha256/deadbeef///", repb.DigestFunction_SHA256),
		},
		{ // Compressed Blake3 AC resource name with tricky instance name
			resourceName: "/a/b/c/d/e/f/g/h/i/j/k/l/m/compressed-blobs/zstd/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     newCompressedACRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "/a/b/c/d/e/f/g/h/i/j/k/l/m", repb.DigestFunction_BLAKE3),
		},
		{ // Compressed Blake3 AC resource name with no leading '/'
			resourceName: "compressed-blobs/zstd/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			wantACRN:     newCompressedACRN(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_BLAKE3),
		},
	}
	for _, tc := range cases {
		arn, err := digest.ParseActionCacheResourceName(tc.resourceName)
		if gstatus.Code(err) != gstatus.Code(tc.wantError) {
			t.Errorf("ParseActionCacheResourceName(%q) got err %v; want %v", tc.resourceName, err, tc.wantError)
		}
		if !acrnsEqual(arn, tc.wantACRN) {
			t.Errorf("ParseActionCacheResourceName(%q): got %s; want %s", tc.resourceName, arn.ActionCacheString(), tc.wantACRN.ActionCacheString())
		}
	}
}

func FuzzParseActionCacheResourceName(f *testing.F) {
	f.Add("compressed-blobs/zstd/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Add("instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Add("/a/b/c/d/e/f/g/h/i/j/k/l/m/compressed-blobs/zstd/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234")
	f.Fuzz(func(t *testing.T, resourceName string) {
		// Log the problematic input instead of making the user try to find it
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("digest.ParseActionCacheResourceName(\"%s\") panicked", resourceName)
			}
		}()

		digest.ParseActionCacheResourceName(resourceName)
	})
}

func acrnsEqual(first *digest.ACResourceName, second *digest.ACResourceName) bool {
	if first == nil && second == nil {
		return true
	}
	if first == nil || second == nil {
		return false
	}
	return first.GetDigest().GetHash() == second.GetDigest().GetHash() &&
		first.GetDigest().GetSizeBytes() == second.GetDigest().GetSizeBytes() &&
		first.GetInstanceName() == second.GetInstanceName() &&
		first.GetCompressor() == second.GetCompressor()
}

func newCompressedACRN(d *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) *digest.ACResourceName {
	rn := digest.NewACResourceName(d, instanceName, digestFunction)
	rn.SetCompressor(repb.Compressor_ZSTD)
	return rn
}

func TestElementsMatch(t *testing.T) {
	d1 := &repb.Digest{Hash: "1234", SizeBytes: 100}
	d2 := &repb.Digest{Hash: "1111", SizeBytes: 10}
	d3 := &repb.Digest{Hash: "1234", SizeBytes: 1}

	cases := []struct {
		s1   []*repb.Digest
		s2   []*repb.Digest
		want bool
	}{
		{
			s1:   []*repb.Digest{},
			s2:   []*repb.Digest{},
			want: true,
		},
		{
			s1:   []*repb.Digest{d1, d2},
			s2:   []*repb.Digest{d1, d2},
			want: true,
		},
		{
			s1:   []*repb.Digest{d1, d2},
			s2:   []*repb.Digest{d2, d1},
			want: true,
		},
		{
			s1:   []*repb.Digest{d1, d2, d3},
			s2:   []*repb.Digest{d2, d1},
			want: false,
		},
		{
			s1:   []*repb.Digest{d1, d2},
			s2:   []*repb.Digest{d2, d1, d3},
			want: false,
		},
		{
			s1:   []*repb.Digest{d1, d2, d1},
			s2:   []*repb.Digest{d2, d1, d1},
			want: true,
		},
	}
	for _, tc := range cases {
		got := digest.ElementsMatch(tc.s1, tc.s2)
		require.Equal(t, tc.want, got)
	}
}

func TestDiff(t *testing.T) {
	d1 := &repb.Digest{Hash: "1234", SizeBytes: 100}
	d2 := &repb.Digest{Hash: "1111", SizeBytes: 10}
	d3_1 := &repb.Digest{Hash: "1234", SizeBytes: 1}
	d3_2 := &repb.Digest{Hash: "1234", SizeBytes: 1}

	cases := []struct {
		s1 []*repb.Digest
		s2 []*repb.Digest

		expectedMissingFromS1 []*repb.Digest
		expectedMissingFromS2 []*repb.Digest
	}{
		{
			s1:                    []*repb.Digest{},
			s2:                    []*repb.Digest{},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{},
		},
		{
			s1:                    []*repb.Digest{d1, d2},
			s2:                    []*repb.Digest{d1, d2},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{},
		},
		{
			s1:                    []*repb.Digest{d1, d2},
			s2:                    []*repb.Digest{d2, d1},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{},
		},
		{
			s1:                    []*repb.Digest{d1, d2, d3_1},
			s2:                    []*repb.Digest{d2, d1},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{d3_1},
		},
		{
			s1:                    []*repb.Digest{d1, d2},
			s2:                    []*repb.Digest{d2, d1, d3_1},
			expectedMissingFromS1: []*repb.Digest{d3_1},
			expectedMissingFromS2: []*repb.Digest{},
		},
		{
			s1:                    []*repb.Digest{d1, d2, d1},
			s2:                    []*repb.Digest{d2, d2, d1},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{},
		},
		{
			s1:                    []*repb.Digest{d1, d2, d1},
			s2:                    []*repb.Digest{d2, d1, d3_1},
			expectedMissingFromS1: []*repb.Digest{d3_1},
			expectedMissingFromS2: []*repb.Digest{},
		},
		{
			s1:                    []*repb.Digest{d3_1},
			s2:                    []*repb.Digest{d3_2},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{},
		},
	}
	for _, tc := range cases {
		got1, got2 := digest.Diff(tc.s1, tc.s2)
		require.ElementsMatch(t, tc.expectedMissingFromS1, got1)
		require.ElementsMatch(t, tc.expectedMissingFromS2, got2)
	}
}

func TestRandomGenerator(t *testing.T) {
	const expectedCompression = 0.3
	gen := digest.RandomGenerator(0)

	for _, size := range []int64{0, 1_000, 10_000, 100_000} {
		d, b, err := gen.RandomDigestBuf(size)
		cb := compression.CompressZstd(nil, b)

		require.NoError(t, err)
		require.Equal(t, size, d.SizeBytes)
		assert.InDelta(t, int(float64(size)*expectedCompression), len(cb), float64(size)*0.20, "should get approximately 0.3 compression ratio")
	}
}

func BenchmarkParseDownloadString(b *testing.B) {
	rns := []string{
		"/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49ac/1234",
		"/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d8d5a2729eb7c46628cf8f765ad7626bc/1234",
		"/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d8d5a2729eb7c46628cf8f765ad7626bc072d9dd55aacaa82ad7626bccc9ec8c4/1234",
		"/blobs/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"my_instance_name/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"//8/compressed-blobs//lol/deadbeaf/zstd/compressed-blobs/zstd/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
		"compressed-blobs/zstd/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, rn := range rns {
			digest.ParseDownloadResourceName(rn)
		}
	}
}

func BenchmarkDigestCompute(b *testing.B) {
	for _, size := range []int64{1, 10, 100, 1000, 10_000, 100_000} {
		for _, df := range []repb.DigestFunction_Value{repb.DigestFunction_SHA256, repb.DigestFunction_BLAKE3} {
			b.Run(fmt.Sprintf("%s/%d", repb.DigestFunction_Value_name[int32(df)], size), func(b *testing.B) {
				buf := make([]byte, size)
				_, err := rand.Read(buf)
				require.NoError(b, err)
				for b.Loop() {
					digest.Compute(bytes.NewReader(buf), df)
				}
			})
		}
	}
}
