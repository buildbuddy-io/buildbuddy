package usageutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestLabels(t *testing.T) {
	for _, test := range []struct {
		Name     string
		MD       *repb.RequestMetadata
		Origin   string
		Expected *tables.UsageLabels
	}{
		{
			Name:     "NoLabels",
			Expected: &tables.UsageLabels{},
		},
		{
			Name:     "BazelToolName",
			MD:       &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			Expected: &tables.UsageLabels{Client: "bazel"},
		},
		{
			Name:     "NonBazelToolName",
			MD:       &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "unknown"}},
			Expected: &tables.UsageLabels{},
		},
		{
			Name:     "ExecutorDetails",
			MD:       &repb.RequestMetadata{ExecutorDetails: &repb.ExecutorDetails{}},
			Expected: &tables.UsageLabels{Client: "executor"},
		},
		{
			Name:     "OriginHeader",
			Origin:   "test-origin",
			Expected: &tables.UsageLabels{Origin: "test-origin"},
		},
		{
			Name:     "BazelToolNameAndOrigin",
			MD:       &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			Origin:   "test-origin",
			Expected: &tables.UsageLabels{Origin: "test-origin", Client: "bazel"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			md := metadata.MD{}
			if test.MD != nil {
				mdb, err := proto.Marshal(test.MD)
				require.NoError(t, err)
				md[bazel_request.RequestMetadataKey] = []string{string(mdb)}
			}
			if test.Origin != "" {
				md["x-buildbuddy-origin"] = []string{test.Origin}
			}
			ctx := metadata.NewIncomingContext(context.Background(), md)

			labels, err := usageutil.Labels(ctx)

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
		})
	}
}
