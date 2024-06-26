package digest

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"regexp"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

// This is an example of a pretty simple go unit test: Tests are typically
// written in a "table-driven" way -- where you enumerate a list of expected
// inputs and outputs, often in an anonymous struct, and then exercise a method
// under test across those cases.
func TestParseResourceName(t *testing.T) {
	cases := []struct {
		resourceName string
		matcher      *regexp.Regexp
		wantParsed   *ResourceName
		wantError    error
	}{
		{ // download, bad hash
			resourceName: "my_instance_name/blobs/invalid_hash/1234",
			matcher:      downloadRegex,
			wantError:    status.InvalidArgumentError(""),
		},
		{ // download, missing size
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/",
			matcher:      downloadRegex,
			wantError:    status.InvalidArgumentError(""),
		},
		{ // download, bad compression type
			resourceName: "/compressed-blobs/unknownCompressionType/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantError:    status.InvalidArgumentError(""),
		},
		{ // download, resource with instance name
			resourceName: "my_instance_name/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "my_instance_name", repb.DigestFunction_SHA256),
		},
		{ // download, resource with zstd compression
			resourceName: "/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   newZstdResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, ""),
		},
		{ // download, resource with zstd compression and instance name
			resourceName: "my_instance_name/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   newZstdResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "my_instance_name"),
		},
		{ // download, resource with digest only
			resourceName: "/blobs/040f06fd774092478d450774f5ba30c5da78acc8/1234",
			matcher:      downloadRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "040f06fd774092478d450774f5ba30c5da78acc8", SizeBytes: 1234}, "", repb.DigestFunction_SHA1),
		},
		{ // download, resource with digest only
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "", repb.DigestFunction_SHA256),
		},
		{ // download, resource with digest only
			resourceName: "/blobs/5406ebea1618e9b73a7290c5d716f0b47b4f1fbc5d8c5e78c9010a3e01c18d8594aa942e3536f7e01574245d34647523/1234",
			matcher:      downloadRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "5406ebea1618e9b73a7290c5d716f0b47b4f1fbc5d8c5e78c9010a3e01c18d8594aa942e3536f7e01574245d34647523", SizeBytes: 1234}, "", repb.DigestFunction_SHA384),
		},
		{ // download, resource with digest only
			resourceName: "/blobs/b2d1d285b5199c85f988d03649c37e44fd3dde01e5d69c50fef90651962f48110e9340b60d49a479c4c0b53f5f07d690686dd87d2481937a512e8b85ee7c617f/1234",
			matcher:      downloadRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "b2d1d285b5199c85f988d03649c37e44fd3dde01e5d69c50fef90651962f48110e9340b60d49a479c4c0b53f5f07d690686dd87d2481937a512e8b85ee7c617f", SizeBytes: 1234}, "", repb.DigestFunction_SHA512),
		},
		{ // upload, UUID and instance name
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      uploadRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA256),
		},
		{ // upload, UUID, instance name, and compression
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      uploadRegex,
			wantParsed:   newZstdResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name"),
		},
		{ // action
			resourceName: "instance_name/blobs/ac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      actionCacheRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_SHA256),
		},
		{ // action
			resourceName: "instance_name/blobs/ac/blake3/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      actionCacheRegex,
			wantParsed:   newCASResourceName(&repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234}, "instance_name", repb.DigestFunction_BLAKE3),
		},
		{ // invalid action
			resourceName: "instance_name/blobs/notac/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      actionCacheRegex,
			wantError:    status.InvalidArgumentError(""),
		},
	}
	for _, tc := range cases {
		gotParsed, gotErr := parseResourceName(tc.resourceName, tc.matcher, rspb.CacheType_CAS)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("parseResourceName(%q) got err %v; want %v", tc.resourceName, gotErr, tc.wantError)
			continue
		}
		if tc.wantParsed != nil && (gotParsed == nil ||
			gotParsed.GetDigest().GetHash() != tc.wantParsed.GetDigest().GetHash() ||
			gotParsed.GetDigest().GetSizeBytes() != tc.wantParsed.GetDigest().GetSizeBytes() ||
			gotParsed.GetInstanceName() != tc.wantParsed.GetInstanceName() ||
			gotParsed.GetCompressor() != tc.wantParsed.GetCompressor()) {
			t.Errorf("parseResourceName(%q): got %+v; want %+v", tc.resourceName, gotParsed, tc.wantParsed)
		}
	}
}

func newCASResourceName(d *repb.Digest, instanceName string, digestFunction repb.DigestFunction_Value) *ResourceName {
	r := NewResourceName(d, instanceName, rspb.CacheType_CAS, digestFunction)
	return r
}

func newZstdResourceName(d *repb.Digest, instanceName string) *ResourceName {
	r := NewResourceName(d, instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	r.SetCompressor(repb.Compressor_ZSTD)
	return r
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
		got := ElementsMatch(tc.s1, tc.s2)
		require.Equal(t, tc.want, got)
	}
}

func TestDiff(t *testing.T) {
	d1 := &repb.Digest{Hash: "1234", SizeBytes: 100}
	d2 := &repb.Digest{Hash: "1111", SizeBytes: 10}
	d3 := &repb.Digest{Hash: "1234", SizeBytes: 1}

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
			s1:                    []*repb.Digest{d1, d2, d3},
			s2:                    []*repb.Digest{d2, d1},
			expectedMissingFromS1: []*repb.Digest{},
			expectedMissingFromS2: []*repb.Digest{d3},
		},
		{
			s1:                    []*repb.Digest{d1, d2},
			s2:                    []*repb.Digest{d2, d1, d3},
			expectedMissingFromS1: []*repb.Digest{d3},
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
			s2:                    []*repb.Digest{d2, d1, d3},
			expectedMissingFromS1: []*repb.Digest{d3},
			expectedMissingFromS2: []*repb.Digest{},
		},
	}
	for _, tc := range cases {
		got1, got2 := Diff(tc.s1, tc.s2)
		require.ElementsMatch(t, tc.expectedMissingFromS1, got1)
		require.ElementsMatch(t, tc.expectedMissingFromS2, got2)
	}
}

func TestRandomGenerator(t *testing.T) {
	const expectedCompression = 0.3
	gen := RandomGenerator(0)

	for _, size := range []int64{1_000, 10_000, 100_000} {
		d, b, err := gen.RandomDigestBuf(size)
		cb := compression.CompressZstd(nil, b)

		require.NoError(t, err)
		require.Equal(t, size, d.SizeBytes)
		assert.InDelta(t, int(float64(size)*expectedCompression), len(cb), float64(size)*0.20, "should get approximately 0.3 compression ratio")
	}
}

func BenchmarkDigestCompute(b *testing.B) {
	for _, size := range []int64{1, 10, 100, 1000, 10_000, 100_000} {
		for _, df := range []repb.DigestFunction_Value{repb.DigestFunction_SHA256, repb.DigestFunction_BLAKE3} {
			b.Run(fmt.Sprintf("%s/%d", repb.DigestFunction_Value_name[int32(df)], size), func(b *testing.B) {
				buf := make([]byte, size)
				_, err := rand.Read(buf)
				require.NoError(b, err)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					Compute(bytes.NewReader(buf), df)
				}
			})
		}
	}
}
