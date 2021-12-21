package digest

import (
	"regexp"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

// This is an example of a pretty simple go unit test: Tests are typically
// written in a "table-driven" way -- where you enumerate a list of expected
// inputs and outputs, often in an anonymous struct, and then exercise a method
// under test across those cases.
func TestParseResourceName(t *testing.T) {
	d := &repb.Digest{
		Hash:      "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d",
		SizeBytes: 1234,
	}

	cases := []struct {
		matcher      *regexp.Regexp
		resourceName string
		wantParsed   *ResourceName
		wantError    error
	}{
		{ // download, bad hash
			resourceName: "my_instance_name/blobs/invalid_hash/1234",
			matcher:      downloadRegex,
			wantParsed:   nil,
			wantError:    status.InvalidArgumentError(""),
		},
		{ // download, missing size
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/",
			matcher:      downloadRegex,
			wantParsed:   nil,
			wantError:    status.InvalidArgumentError(""),
		},
		{ // download, resource with instance name
			resourceName: "my_instance_name/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   &ResourceName{NewInstanceNameDigest(d, "my_instance_name"), repb.Compressor_IDENTITY},
			wantError:    nil,
		},
		{ // download, resource without instance name
			resourceName: "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   &ResourceName{NewInstanceNameDigest(d, ""), repb.Compressor_IDENTITY},
			wantError:    nil,
		},
		{ // download, resource without instance name, zstd-compressed
			resourceName: "/compressed-blobs/zstd/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      downloadRegex,
			wantParsed:   &ResourceName{NewInstanceNameDigest(d, ""), repb.Compressor_ZSTD},
			wantError:    nil,
		},
		{ // upload, UUID and instance name
			resourceName: "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:      uploadRegex,
			wantParsed:   &ResourceName{NewInstanceNameDigest(d, "instance_name"), repb.Compressor_IDENTITY},
			wantError:    nil,
		},
	}
	for _, tc := range cases {
		gotParsed, gotErr := parseResourceName(tc.resourceName, tc.matcher)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("parseResourceName(%q) returned error %v; want %v", tc.resourceName, gotErr, tc.wantError)
			continue
		}
		if tc.wantParsed == nil {
			if gotParsed != nil {
				t.Errorf("parseResourceName(%q) returned %v; want %v", tc.resourceName, gotParsed, tc.wantParsed)
			}
			continue
		}

		assert.Equal(t, tc.wantParsed.GetHash(), gotParsed.GetHash(), "unexpected hash parsed from %q", tc.resourceName)
		assert.Equal(t, tc.wantParsed.GetSizeBytes(), gotParsed.GetSizeBytes(), "unexpected size_bytes parsed from %q", tc.resourceName)
		assert.Equal(t, tc.wantParsed.GetInstanceName(), gotParsed.GetInstanceName(), "unexpected instance name parsed from %q", tc.resourceName)
		assert.Equal(t, tc.wantParsed.Compression, gotParsed.Compression, "unexpected compression parsed from %q", tc.resourceName)
	}
}
