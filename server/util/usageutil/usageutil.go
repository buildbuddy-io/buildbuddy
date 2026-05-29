// TODO: rename to "usage" and rename enterprise/server/usage to
// "usagetracker"
package usageutil

import (
	"context"
	"flag"
	"net/url"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"google.golang.org/grpc/metadata"
)

const (
	// gRPC metadata header constants.
	ClientHeaderName            = "x-buildbuddy-client"
	OriginHeaderName            = "x-buildbuddy-origin"
	SkipUsageTrackingHeaderName = "x-buildbuddy-skip-tracking"

	// Client label constants.
	bazelClientLabel    = "bazel"
	executorClientLabel = "executor"

	SkipUsageTrackingEnabledValue = "1"
)

var (
	origin = flag.String("grpc_client_origin_header", "", "Header value to set for x-buildbuddy-origin.")

	// The server name to record in usage.  This will be used for the "client" usage label when sending RPCS
	// and the "server" usage label when a usage-generating request terminates at this server.
	serverName string
)

func DisableUsageTracking(ctx context.Context) context.Context {
	if ClientOrigin() != interfaces.ClientIdentityInternalOrigin || ServerName() != interfaces.ClientIdentityCacheProxy {
		alert.CtxUnexpectedEvent(ctx, "unexpected-tracking-disablement", "Tried to disable usage tracking from an unsupported origin: %s %s", ClientOrigin(), ServerName())
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, SkipUsageTrackingHeaderName, SkipUsageTrackingEnabledValue)
}

func LabelsForUsageRecording(ctx context.Context, server string) (*tables.UsageLabels, error) {
	return &tables.UsageLabels{
		Origin: originLabel(ctx),
		Client: clientLabel(ctx),
		Server: server,
	}, nil
}

// WithLocalServerLabels causes outgoing gRPC requests to be labeled with the
// configured client and origin of the local server instance (e.g. internal
// executor), overriding any labels from the client that initiated the current
// request (e.g. bazel).
//
// These labels will also be propagated across chained RPCs to BuildBuddy
// servers. e.g. if the current client calls app 1 which calls app 2, app 2 will
// see these label values. Note that if app 1 in this scenario also calls
// WithLocalServerLabels on its outgoing context to app 2, app 2 would see the
// values for both app 1, and the original client, but app 1's values would take
// precedence.
func WithLocalServerLabels(ctx context.Context) context.Context {
	// Note: we set the header values here even if they're empty so that they
	// override other header values, e.g. bazel request metadata.
	ctx = metadata.AppendToOutgoingContext(ctx, OriginHeaderName, *origin)
	ctx = metadata.AppendToOutgoingContext(ctx, ClientHeaderName, serverName)
	return ctx
}

// ClientOrigin returns the configured value of x-buildbuddy-origin that will be
// set on *outgoing* gRPC requests with label propagation enabled.
func ClientOrigin() string {
	return *origin
}

// SetServerName will be used for x-buildbuddy-client header for *outgoing* gRPC
// requests and recorded for usage-generating requests that terminated at this server.
func SetServerName(value string) {
	serverName = value
}

func ServerName() string {
	return serverName
}

func CollectionFromRPCContext(ctx context.Context) *Collection {
	groupID := interfaces.AuthAnonymousUser
	if claims, err := claims.ClaimsFromContext(ctx); err == nil {
		groupID = claims.GetGroupID()
	}
	c := &Collection{
		GroupID: groupID,
		Server:  ServerName(),
		Client:  clientLabel(ctx),
		Origin:  originLabel(ctx),
	}
	return c
}

func AddUsageHeadersToContext(ctx context.Context, client string, origin string) context.Context {
	if client != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, ClientHeaderName, client)
	}
	if origin != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, OriginHeaderName, origin)
	}
	return ctx
}

func originLabel(ctx context.Context) string {
	vals := metadata.ValueFromIncomingContext(ctx, OriginHeaderName)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func clientLabel(ctx context.Context) string {
	vals := metadata.ValueFromIncomingContext(ctx, ClientHeaderName)
	if len(vals) > 0 {
		return vals[0]
	}
	toolName := bazel_request.GetToolName(ctx)
	if toolName == "bazel" {
		return bazelClientLabel
	}
	return ""
}

// A Collection consists of all of the fields that we currently use to identify
// different types of usage--these fields are ultimately written out to the
// `Usages` table as `UsageLabels`, where they determine cost bucketing.
// See documentation on `UsageLabels` for an explanation of each field.
type Collection struct {
	// TODO: maybe make GroupID a field of tables.UsageLabels.
	GroupID string
	Origin  string
	Server  string
	Client  string
}

func (c *Collection) UsageLabels() *tables.UsageLabels {
	return &tables.UsageLabels{
		Origin: c.Origin,
		Client: c.Client,
		Server: c.Server,
	}
}

type LabeledSKUCount struct {
	Labels map[sku.LabelName]sku.LabelValue
	SKU    sku.SKU
	Count  int64
	Unit   string
}

func LabeledSKUCountsFromUsageCounts(baseLabels map[sku.LabelName]sku.LabelValue, counts *tables.UsageCounts) []LabeledSKUCount {
	if counts == nil {
		return nil
	}
	var items []LabeledSKUCount
	add := func(usageSKU sku.SKU, count int64, unit string, labels map[sku.LabelName]sku.LabelValue) {
		if count <= 0 {
			return
		}
		items = append(items, LabeledSKUCount{
			SKU:    usageSKU,
			Labels: labels,
			Count:  count,
			Unit:   unit,
		})
	}
	addExecution := func(usageSKU sku.SKU, count int64, unit, os, selfHosted string) {
		if count <= 0 {
			return
		}
		add(usageSKU, count, unit, executionLabels(baseLabels, os, selfHosted))
	}

	add(sku.BuildEventsBESCount, counts.Invocations, "count", baseLabels)
	add(sku.RemoteCacheACHits, counts.ActionCacheHits, "count", baseLabels)
	add(sku.RemoteCacheCASHits, counts.CASCacheHits, "count", baseLabels)
	add(sku.RemoteCacheCASDownloadedBytes, counts.TotalDownloadSizeBytes, "bytes", baseLabels)
	add(sku.RemoteCacheCASUploadedBytes, counts.TotalUploadSizeBytes, "bytes", baseLabels)
	add(sku.RemoteCacheACCachedExecDurationNanos, counts.TotalCachedActionExecUsec*1000, "nanos", baseLabels)
	addExecution(sku.RemoteExecutionExecuteWorkerDurationNanos, counts.LinuxExecutionDurationUsec*1000, "nanos", sku.OSLinux, sku.SelfHostedFalse)
	addExecution(sku.RemoteExecutionExecuteWorkerDurationNanos, counts.MacExecutionDurationUsec*1000, "nanos", sku.OSMac, sku.SelfHostedFalse)
	addExecution(sku.RemoteExecutionExecuteWorkerDurationNanos, counts.SelfHostedLinuxExecutionDurationUsec*1000, "nanos", sku.OSLinux, sku.SelfHostedTrue)
	addExecution(sku.RemoteExecutionExecuteWorkerDurationNanos, counts.SelfHostedMacExecutionDurationUsec*1000, "nanos", sku.OSMac, sku.SelfHostedTrue)
	addExecution(sku.RemoteExecutionExecuteWorkerCPUNanos, counts.CPUNanos, "nanos", sku.OSLinux, sku.SelfHostedFalse)
	addExecution(sku.RemoteExecutionExecuteWorkerMemoryGBNanos, counts.MemoryGBUsec*1000, "gb_nanos", sku.OSLinux, sku.SelfHostedFalse)
	return items
}

func executionLabels(labels map[sku.LabelName]sku.LabelValue, os, selfHosted string) map[sku.LabelName]sku.LabelValue {
	out := make(map[sku.LabelName]sku.LabelValue, len(labels)+2)
	for key, value := range labels {
		out[key] = value
	}
	out[sku.OS] = os
	out[sku.SelfHosted] = selfHosted
	return out
}

// EncodeCollection encodes the collection to a human readable format.
func EncodeCollection(c *Collection) string {
	// Using a handwritten encoding scheme for performance reasons (this
	// runs on every cache request).
	s := "group_id=" + c.GroupID
	if c.Origin != "" {
		s += "&origin=" + url.QueryEscape(c.Origin)
	}
	if c.Client != "" {
		s += "&client=" + url.QueryEscape(c.Client)
	}
	if c.Server != "" {
		s += "&server=" + url.QueryEscape(c.Server)
	}
	return s
}

// DecodeCollection decodes a string encoded using encodeCollection.
// It returns the raw url.Values so that apps can detect collections encoded
// by newer apps.
func DecodeCollection(s string) (*Collection, url.Values, error) {
	q, err := url.ParseQuery(s)
	if err != nil {
		return nil, nil, err
	}
	c := &Collection{
		GroupID: q.Get("group_id"),
		Origin:  q.Get("origin"),
		Client:  q.Get("client"),
		Server:  q.Get("server"),
	}
	return c, q, nil
}

// OLAPCollection consists of all of the fields that we currently use to
// identify different types of usage. These fields are ultimately written out to
// the `RawUsage` table, where they determine cost bucketing.
type OLAPCollection struct {
	GroupID string
	Labels  map[sku.LabelName]sku.LabelValue
}

// EncodeOLAPCollection encodes the OLAP collection to a deterministic string.
// The encoding is human-readable and uses sorted label keys for determinism.
// Format: "group_id=X&label=key1=val1&label=key2=val2..." (labels are
// encoded as repeated "label" parameters with key=value format).
func EncodeOLAPCollection(c *OLAPCollection) string {
	var b strings.Builder
	b.WriteString("group_id=" + url.QueryEscape(c.GroupID))

	if len(c.Labels) > 0 {
		// Sort label keys for deterministic encoding
		keys := make([]string, 0, len(c.Labels))
		for k := range c.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			b.WriteString("&label=" + url.QueryEscape(k) + "=" + url.QueryEscape(c.Labels[k]))
		}
	}

	return b.String()
}

// DecodeOLAPCollection decodes a string encoded using EncodeOLAPCollection.
// Labels are expected in the format "label=key=value".
func DecodeOLAPCollection(s string) (*OLAPCollection, error) {
	q, err := url.ParseQuery(s)
	if err != nil {
		return nil, err
	}
	c := &OLAPCollection{
		GroupID: q.Get("group_id"),
	}
	// Parse labels from "label" params (format: "key=value")
	for _, labelParam := range q["label"] {
		key, value, found := strings.Cut(labelParam, "=")
		if !found {
			continue
		}
		if c.Labels == nil {
			c.Labels = make(map[sku.LabelName]sku.LabelValue)
		}
		c.Labels[key] = value
	}
	return c, nil
}
