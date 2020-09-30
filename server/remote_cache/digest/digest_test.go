package digest

import (
	"regexp"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

// This is an example of a pretty simple go unit test: Tests are typically
// written in a "table-driven" way -- where you enumerate a list of expected
// inputs and outputs, often in an anonymous struct, and then exercise a method
// under test across those cases.
func TestExtractDigest(t *testing.T) {
	cases := []struct {
		resourceName     string
		matcher          *regexp.Regexp
		wantInstanceName string
		wantDigest       *repb.Digest
		wantError        error
	}{
		{ // download, bad hash
			resourceName:     "my_instance_name/blobs/invalid_hash/1234",
			matcher:          downloadRegex,
			wantInstanceName: "",
			wantDigest:       nil,
			wantError:        status.InvalidArgumentError(""),
		},
		{ // download, missing size
			resourceName:     "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/",
			matcher:          downloadRegex,
			wantInstanceName: "",
			wantDigest:       nil,
			wantError:        status.InvalidArgumentError(""),
		},
		{ // download, resource with instance name
			resourceName:     "my_instance_name/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          downloadRegex,
			wantInstanceName: "my_instance_name",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
		{ // download, resource without instance name
			resourceName:     "/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          downloadRegex,
			wantInstanceName: "",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
		{ // upload, UUID and instance name
			resourceName:     "instance_name/uploads/2148e1f1-aacc-41eb-a31c-22b6da7c7ac1/blobs/072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d/1234",
			matcher:          uploadRegex,
			wantInstanceName: "instance_name",
			wantDigest:       &repb.Digest{Hash: "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d", SizeBytes: 1234},
			wantError:        nil,
		},
	}
	for _, tc := range cases {
		gotInstanceName, gotDigest, gotErr := extractDigest(tc.resourceName, tc.matcher)
		if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
			t.Errorf("extractDigest(%q) returned %v; want %v", tc.resourceName, gotErr, tc.wantError)
			continue
		}
		if gotInstanceName != tc.wantInstanceName {
			t.Errorf("extractDigest(%q): got instance_name: %v; want %v", tc.resourceName, gotInstanceName, tc.wantInstanceName)
		}
		if gotDigest.GetHash() != tc.wantDigest.GetHash() || gotDigest.GetSizeBytes() != tc.wantDigest.GetSizeBytes() {
			t.Errorf("extractDigest(%q) got digest: %v; want %v", tc.resourceName, gotDigest, tc.wantDigest)
		}
	}
}
