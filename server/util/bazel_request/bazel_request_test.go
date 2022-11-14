package bazel_request_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestParseBazelVersion(t *testing.T) {
	for _, testCase := range []struct {
		Tool, Version   string
		ExpectedVersion *bazel_request.Version
	}{
		{"unknown-tool", "5.0.0", nil},
		{"bazel", "INVALID", nil},
		{"bazel", "", nil},
		{"bazel", "0.23", v(0, 23, 0, "")},
		{"bazel", "5.3.1", v(5, 3, 1, "")},
		{"bazel", "7.0.0-pre.20221102.3", v(7, 0, 0, "-pre.20221102.3")},
	} {
		ctx := context.Background()
		rmd := &repb.RequestMetadata{
			ToolDetails: &repb.ToolDetails{
				ToolName:    testCase.Tool,
				ToolVersion: testCase.Version,
			},
		}
		ctx = withIncomingMetadata(t, ctx, rmd)

		actualVersion := bazel_request.GetVersion(ctx)

		assert.Equal(t, testCase.ExpectedVersion, actualVersion)
	}
}

func TestVersionIsAtLeast(t *testing.T) {
	for _, testCase := range []struct {
		V1, V2   *bazel_request.Version
		Expected bool
	}{
		// Equal
		{V1: v(1, 2, 3, ""), V2: v(1, 2, 3, ""), Expected: true},
		// Pre-release vs. release
		{V1: v(7, 0, 0, ""), V2: v(7, 0, 0, "-pre.20221026.2"), Expected: true},
		{V1: v(7, 0, 0, "-pre.20221026.2"), V2: v(7, 0, 0, ""), Expected: false},
		// Pre-release
		{V1: v(7, 0, 0, "-pre.20221102.3"), V2: v(7, 0, 0, "-pre.20221026.2"), Expected: true},
		{V1: v(7, 0, 0, "-pre.20221026.2"), V2: v(7, 0, 0, "-pre.20221102.3"), Expected: false},
		// Patch
		{V1: v(5, 3, 2, ""), V2: v(5, 3, 1, ""), Expected: true},
		{V1: v(5, 3, 1, ""), V2: v(5, 3, 2, ""), Expected: false},
		// Minor
		{V1: v(5, 2, 0, ""), V2: v(5, 1, 0, ""), Expected: true},
		{V1: v(5, 1, 0, ""), V2: v(5, 2, 0, ""), Expected: false},
		// Major
		{V1: v(6, 0, 0, ""), V2: v(5, 0, 0, ""), Expected: true},
		{V1: v(5, 0, 0, ""), V2: v(6, 0, 0, ""), Expected: false},
	} {
		actual := testCase.V1.IsAtLeast(testCase.V2)

		assert.Equal(
			t, testCase.Expected, actual,
			"expected %+v >= %+v", testCase.V1, testCase.V2)
	}
}

func v(major, minor, patch int, suffix string) *bazel_request.Version {
	return &bazel_request.Version{
		Major:  major,
		Minor:  minor,
		Patch:  patch,
		Suffix: suffix,
	}
}

// Note: Can't use bazel_request.WithRequestMetadata here since it sets the
// metadata on the outgoing context, not the incoming context.
func withIncomingMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	md := metadata.Pairs(bazel_request.RequestMetadataKey, string(b))
	return metadata.NewIncomingContext(ctx, md)
}
