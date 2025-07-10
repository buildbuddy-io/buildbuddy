package usageutil

import (
	"context"
	"flag"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
)

const (
	// gRPC metadata header constants.
	ClientHeaderName = "x-buildbuddy-client"
	OriginHeaderName = "x-buildbuddy-origin"

	// Client label constants.
	bazelClientLabel    = "bazel"
	executorClientLabel = "executor"
)

var (
	origin = flag.String("grpc_client_origin_header", "", "Header value to set for x-buildbuddy-origin.")

	// The server name to record in usage.  This will be used for the "client" usage label when sending RPCS
	// and the "server" usage label when a usage-generating request terminates at this server.
	serverName string
)

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

func GetUsageHeaders(ctx context.Context) map[string][]string {
	headers := map[string][]string{}

	clientKeys := metadata.ValueFromIncomingContext(ctx, ClientHeaderName)
	if len(clientKeys) > 0 {
		if len(clientKeys) > 1 {
			log.CtxWarningf(ctx, "Expected at most 1 usage client header (found %d)", len(clientKeys))
		}
		headers[ClientHeaderName] = clientKeys
	}
	originKeys := metadata.ValueFromIncomingContext(ctx, OriginHeaderName)
	if len(originKeys) > 0 {
		if len(originKeys) > 1 {
			log.CtxWarningf(ctx, "Expected at most 1 usage origin header (found %d)", len(originKeys))
		}
		headers[OriginHeaderName] = originKeys
	}

	return headers
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
