package templates

import (
	"context"
	"strings"
	
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
)

const (
	prodInvocationPrefix = "buildbuddy:///invocation/"
	prodTargetPrefix = "buildbuddy:///target/"
)

type TemplateHandler struct {
        env environment.Env
}

func NewHandler(env environment.Env) (*TemplateHandler, error) {
        return &TemplateHandler{
		env: env,
        }, nil
}

func (r *TemplateHandler) Register(server *mcp.Server) error {
	server.AddResourceTemplate(
		&mcp.ResourceTemplate{
			Name:        "resources/invocation/details",
			MIMEType:    "text/template",
			URITemplate: prodInvocationPrefix+"{invocation_id}#details",
		},
		r.getInvocationHandler,
	)
	server.AddResourceTemplate(
		&mcp.ResourceTemplate{
			Name:        "resources/invocation/logs",
			MIMEType:    "text/template",
			URITemplate: prodInvocationPrefix+"{invocation_id}#logs",
		},
		r.getInvocationLogsHandler,
	)
	server.AddResourceTemplate(
		&mcp.ResourceTemplate{
			Name:        "resources/target/flake_samples",
			MIMEType:    "text/template",
			URITemplate: prodTargetPrefix+"{label}#flake_samples",
		},
		r.getFlakySamplesHandler,
	)
	return nil
}

func (r *TemplateHandler) getInvocationHandler(ctx context.Context, ss *mcp.ServerSession, params *mcp.ReadResourceParams) (_ *mcp.ReadResourceResult, err error) {
	invocationID := strings.TrimSuffix(strings.TrimPrefix(params.URI, prodInvocationPrefix), "#details")
	req := &apipb.GetInvocationRequest{
		Selector: &apipb.InvocationSelector{
			InvocationId: invocationID,
		},
	}
	rsp, err := r.env.GetApiClient().GetInvocation(ctx, req)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, mcp.ResourceNotFoundError(params.URI)
		}
		return nil, err
	}
	buf, err := protojson.MarshalOptions{Multiline: true}.Marshal(rsp)
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
		&mcp.ResourceContents{URI: params.URI, MIMEType: "application/json", Blob: buf},
	}}, nil
}

func (r *TemplateHandler) getInvocationLogsHandler(ctx context.Context, ss *mcp.ServerSession, params *mcp.ReadResourceParams) (_ *mcp.ReadResourceResult, err error) {
	invocationID := strings.TrimSuffix(strings.TrimPrefix(params.URI, prodInvocationPrefix), "#logs")

	req := &apipb.GetLogRequest{
		Selector: &apipb.LogSelector{
			InvocationId: invocationID,
		},
	}
	rsp, err := r.env.GetApiClient().GetLog(ctx, req)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, mcp.ResourceNotFoundError(params.URI)
		}
		return nil, err
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
		&mcp.ResourceContents{URI: params.URI, MIMEType: "text/plain", Blob: []byte(rsp.GetLog().GetContents())},
	}}, nil
}

func (r *TemplateHandler) getFlakySamplesHandler(ctx context.Context, ss *mcp.ServerSession, params *mcp.ReadResourceParams) (_ *mcp.ReadResourceResult, err error) {
	label := strings.TrimSuffix(strings.TrimPrefix(params.URI, prodTargetPrefix), "#flake_samples")

	req := &trpb.GetTargetFlakeSamplesRequest{
		Label: label,
	}
	rsp, err := r.env.GetBuildBuddyClient().GetTargetFlakeSamples(ctx, req)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, mcp.ResourceNotFoundError(params.URI)
		}
		return nil, err
	}

	content := make([]*mcp.ResourceContents, 0, len(rsp.GetSamples())*2)
	for _, sample := range rsp.GetSamples() {
		content = append(content, &mcp.ResourceContents{
			URI: prodInvocationPrefix + sample.GetInvocationId() + "#details",
			Text: "resources/invocation/details",
		})
		
		content	= append(content, &mcp.ResourceContents{
			URI: prodInvocationPrefix + sample.GetInvocationId() + "#logs",
			Text: "resources/invocation/logs",
		})
	}

	return &mcp.ReadResourceResult{Contents: content}, nil
}
