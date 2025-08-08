package usageutil_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

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
				md[usageutil.OriginHeaderName] = []string{test.OriginHeader}
			}
			if test.ClientHeader != "" {
				md[usageutil.ClientHeaderName] = []string{test.ClientHeader}
			}
			ctx := metadata.NewIncomingContext(t.Context(), md)

			labels, err := usageutil.LabelsForUsageRecording(ctx, "")

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
		})
	}
}

func TestLabelPropagation(t *testing.T) {
	for _, test := range []struct {
		Name     string
		Client   string
		Server   string
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
			Name:     "Server",
			Server:   "app",
			Expected: &tables.UsageLabels{Server: "app"},
		},
		{
			Name:     "Origin",
			Origin:   "external",
			Expected: &tables.UsageLabels{Origin: "external"},
		},
		{
			Name:     "All",
			Client:   "executor",
			Server:   "cache-proxy",
			Origin:   "internal",
			Expected: &tables.UsageLabels{Client: "executor", Server: "cache-proxy", Origin: "internal"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			flags.Set(t, "grpc_client_origin_header", test.Origin)
			usageutil.SetServerName(test.Client)

			ctx := t.Context()
			// Set some pre-existing bazel request metadata on the incoming
			// context; our propagated labels should always take precedence.
			bazelMD := &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}}
			ctx, err := bazel_request.WithRequestMetadata(ctx, bazelMD)
			ctx = usageutil.WithLocalServerLabels(ctx)
			require.NoError(t, err)

			// Simulate an RPC by creating a new context with the incoming
			// metadata set to the previously applied outgoing metadata.
			ctx = testgrpc.OutgoingToIncomingContext(t, ctx)

			// Simulate that we've arrived at the receiving server by
			// changing the server name on the fly.
			usageutil.SetServerName(test.Server)
			labels, err := usageutil.LabelsForUsageRecording(ctx, test.Server)

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
		})
	}
}
