package usageutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestLabels(t *testing.T) {
	for _, test := range []struct {
		Name         string
		MDHeader     *repb.RequestMetadata
		ClientHeader string
		OriginHeader string
		Expected     *tables.UsageLabels
	}{
		{
			Name:     "NoLabels",
			Expected: &tables.UsageLabels{},
		},
		{
			Name:     "BazelToolName",
			MDHeader: &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			Expected: &tables.UsageLabels{Client: "bazel"},
		},
		{
			Name:     "NonBazelToolName",
			MDHeader: &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "unknown"}},
			Expected: &tables.UsageLabels{},
		},
		{
			Name:         "ClientHeader",
			ClientHeader: "executor",
			Expected:     &tables.UsageLabels{Client: "executor"},
		},
		{
			Name:         "OriginHeader",
			OriginHeader: "test-origin",
			Expected:     &tables.UsageLabels{Origin: "test-origin"},
		},
		{
			Name:         "BazelToolNameAndOrigin",
			MDHeader:     &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			OriginHeader: "test-origin",
			Expected:     &tables.UsageLabels{Origin: "test-origin", Client: "bazel"},
		},
		{
			Name:         "ClientOverridesRequestMetadata",
			MDHeader:     &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			OriginHeader: "internal",
			ClientHeader: "executor",
			Expected:     &tables.UsageLabels{Origin: "internal", Client: "executor"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			md := metadata.MD{}
			if test.MDHeader != nil {
				mdb, err := proto.Marshal(test.MDHeader)
				require.NoError(t, err)
				md[bazel_request.RequestMetadataKey] = []string{string(mdb)}
			}
			if test.OriginHeader != "" {
				md["x-buildbuddy-origin"] = []string{test.OriginHeader}
			}
			if test.ClientHeader != "" {
				md["x-buildbuddy-client"] = []string{test.ClientHeader}
			}
			ctx := metadata.NewIncomingContext(context.Background(), md)

			labels, err := usageutil.Labels(ctx)

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
		})
	}
}

func TestLabelPropagation(t *testing.T) {
	for _, test := range []struct {
		Name     string
		Client   string
		Origin   string
		Expected *tables.UsageLabels
	}{
		{
			Name:     "Empty",
			Expected: &tables.UsageLabels{},
		},
		{
			Name:     "Client",
			Client:   "executor",
			Expected: &tables.UsageLabels{Client: "executor"},
		},
		{
			Name:     "Origin",
			Origin:   "external",
			Expected: &tables.UsageLabels{Origin: "external"},
		},
		{
			Name:     "All",
			Client:   "app",
			Origin:   "internal",
			Expected: &tables.UsageLabels{Client: "app", Origin: "internal"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			flags.Set(t, "grpc_client_origin_header", test.Origin)
			usageutil.SetClientType(test.Client)

			ctx := context.Background()
			// Set some pre-existing bazel request metadata on the incoming
			// context; our propagated labels should always take precedence.
			bazelMD := &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}}
			ctx, err := bazel_request.WithRequestMetadata(ctx, bazelMD)
			ctx = usageutil.WithLocalServerLabels(ctx)
			require.NoError(t, err)
			outgoingMD, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)

			// Simulate an RPC by creating a new context with the incoming
			// metadata set to the previously applied outgoing metadata.
			ctx = context.Background()
			ctx = metadata.NewIncomingContext(ctx, outgoingMD)

			labels, err := usageutil.Labels(ctx)

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
		})
	}
}
