package usageutil_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
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
		ExpectedOLAP sku.Labels
	}{
		{
			Name:         "NoLabels",
			Expected:     &tables.UsageLabels{},
			ExpectedOLAP: sku.Labels{},
		},
		{
			Name:     "BazelToolName",
			MDHeader: &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			Expected: &tables.UsageLabels{Client: "bazel"},
			ExpectedOLAP: sku.Labels{
				sku.Client: sku.ClientBazel,
			},
		},
		{
			Name:         "NonBazelToolName",
			MDHeader:     &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "unknown"}},
			Expected:     &tables.UsageLabels{},
			ExpectedOLAP: sku.Labels{},
		},
		{
			Name:         "ClientHeader",
			ClientHeader: "executor",
			Expected:     &tables.UsageLabels{Client: "executor"},
			ExpectedOLAP: sku.Labels{
				sku.Client: sku.ClientExecutor,
			},
		},
		{
			Name:         "OriginHeader",
			OriginHeader: "test-origin",
			Expected:     &tables.UsageLabels{Origin: "test-origin"},
			ExpectedOLAP: sku.Labels{
				sku.Origin: sku.LabelValue("test-origin"),
			},
		},
		{
			Name:         "BazelToolNameAndOrigin",
			MDHeader:     &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			OriginHeader: "test-origin",
			Expected:     &tables.UsageLabels{Origin: "test-origin", Client: "bazel"},
			ExpectedOLAP: sku.Labels{
				sku.Origin: sku.LabelValue("test-origin"),
				sku.Client: sku.ClientBazel,
			},
		},
		{
			Name:         "ClientOverridesRequestMetadata",
			MDHeader:     &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			OriginHeader: "internal",
			ClientHeader: "executor",
			Expected:     &tables.UsageLabels{Origin: "internal", Client: "executor"},
			ExpectedOLAP: sku.Labels{
				sku.Origin: sku.OriginInternal,
				sku.Client: sku.ClientExecutor,
			},
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

			labels, olapLabels, err := usageutil.LabelsForUsageRecording(ctx, "")

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
			require.Equal(t, test.ExpectedOLAP, olapLabels)
		})
	}
}

func TestLabelPropagation(t *testing.T) {
	for _, test := range []struct {
		Name         string
		Client       string
		Server       string
		Origin       string
		Expected     *tables.UsageLabels
		ExpectedOLAP sku.Labels
	}{
		{
			Name:         "Empty",
			Expected:     &tables.UsageLabels{},
			ExpectedOLAP: sku.Labels{},
		},
		{
			Name:         "Client",
			Client:       "executor",
			Expected:     &tables.UsageLabels{Client: "executor"},
			ExpectedOLAP: sku.Labels{sku.Client: "executor"},
		},
		{
			Name:         "Server",
			Server:       "app",
			Expected:     &tables.UsageLabels{Server: "app"},
			ExpectedOLAP: sku.Labels{sku.Server: "app"},
		},
		{
			Name:         "Origin",
			Origin:       "external",
			Expected:     &tables.UsageLabels{Origin: "external"},
			ExpectedOLAP: sku.Labels{sku.Origin: "external"},
		},
		{
			Name:         "All",
			Client:       "executor",
			Server:       "cache-proxy",
			Origin:       "internal",
			Expected:     &tables.UsageLabels{Client: "executor", Server: "cache-proxy", Origin: "internal"},
			ExpectedOLAP: sku.Labels{sku.Client: "executor", sku.Server: "cache-proxy", sku.Origin: "internal"},
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
			ctx = testgrpc.OutgoingToIncomingContext(t, ctx)

			// Simulate that we've arrived at the receiving server by
			// changing the server name on the fly.
			usageutil.SetServerName(test.Server)
			labels, olapLabels, err := usageutil.LabelsForUsageRecording(ctx, test.Server)

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
			require.Equal(t, test.ExpectedOLAP, olapLabels)
		})
	}
}
