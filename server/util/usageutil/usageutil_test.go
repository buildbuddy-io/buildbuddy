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
		ProxyHeader  string
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
		{
			Name:        "ProxyHeaderExternal",
			ProxyHeader: sku.ProxyExternal,
			Expected:    &tables.UsageLabels{Proxy: "external"},
			ExpectedOLAP: sku.Labels{
				sku.Proxy: sku.ProxyExternal,
			},
		},
		{
			Name:         "ProxyHeaderInternalWithOriginAndClient",
			MDHeader:     &repb.RequestMetadata{ToolDetails: &repb.ToolDetails{ToolName: "bazel"}},
			OriginHeader: "external",
			ProxyHeader:  sku.ProxyInternal,
			Expected:     &tables.UsageLabels{Origin: "external", Client: "bazel", Proxy: "internal"},
			ExpectedOLAP: sku.Labels{
				sku.Origin: sku.OriginExternal,
				sku.Client: sku.ClientBazel,
				sku.Proxy:  sku.ProxyInternal,
			},
		},
		{
			// An unrecognized proxy header value is recorded as "unknown" so it
			// can't pollute usage/billing labels with arbitrary values.
			Name:        "ProxyHeaderInvalidIsUnknown",
			ProxyHeader: "bogus",
			Expected:    &tables.UsageLabels{Proxy: "unknown"},
			ExpectedOLAP: sku.Labels{
				sku.Proxy: sku.ProxyUnknown,
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
			if test.ProxyHeader != "" {
				md[usageutil.ProxyHeaderName] = []string{test.ProxyHeader}
			}
			ctx := metadata.NewIncomingContext(t.Context(), md)

			labels, olapLabels, err := usageutil.LabelsForUsageRecording(ctx, "")

			require.NoError(t, err)
			require.Equal(t, test.Expected, labels)
			require.Equal(t, test.ExpectedOLAP, olapLabels)
		})
	}
}

func TestEncodeDecodeCollection(t *testing.T) {
	for _, test := range []struct {
		Name       string
		Collection *usageutil.Collection
	}{
		{
			Name:       "Empty",
			Collection: &usageutil.Collection{GroupID: "GR1"},
		},
		{
			Name:       "ProxyOnly",
			Collection: &usageutil.Collection{GroupID: "GR1", Proxy: "external"},
		},
		{
			Name: "AllFields",
			Collection: &usageutil.Collection{
				GroupID: "GR1",
				Origin:  "internal",
				Client:  "bazel",
				Server:  "cache-proxy",
				Proxy:   "internal",
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			encoded := usageutil.EncodeCollection(test.Collection)
			decoded, _, err := usageutil.DecodeCollection(encoded)
			require.NoError(t, err)
			require.Equal(t, test.Collection, decoded)
		})
	}
}

// TestProxyHeaderPropagation exercises the path a cache proxy uses to report
// its configured proxy type: the proxy captures ProxyType() into the collection
// (CollectionFromRPCContext), emits it as a header (AddUsageHeadersToContext),
// and the app reads it back into usage labels (LabelsForUsageRecording).
func TestProxyHeaderPropagation(t *testing.T) {
	usageutil.SetProxyType(sku.ProxyExternal)
	t.Cleanup(func() { usageutil.SetProxyType("") })

	// Proxy side: the collection captured for an incoming request should pick up
	// the proxy's configured type.
	c := usageutil.CollectionFromRPCContext(t.Context())
	require.Equal(t, sku.ProxyExternal, c.Proxy)

	// Proxy side: the proxy type is emitted as an outgoing header.
	ctx := usageutil.AddUsageHeadersToContext(t.Context(), c.Client, c.Origin, c.Proxy)
	ctx = testgrpc.OutgoingToIncomingContext(t, ctx)

	// App side: the header is read back into the recorded usage labels.
	labels, olapLabels, err := usageutil.LabelsForUsageRecording(ctx, "app")
	require.NoError(t, err)
	require.Equal(t, sku.ProxyExternal, labels.Proxy)
	require.Equal(t, sku.ProxyExternal, olapLabels[sku.Proxy])
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
