package execution_graph

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	egapb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph_analysis"
)

// GetExecutionGraphAnalysis returns the stored execution graph analysis for
// an invocation, or NotFound if the invocation was not analyzed (e.g. it had
// no execution graph log, or the analysis worker isn't running).
func GetExecutionGraphAnalysis(ctx context.Context, env environment.Env, req *egapb.GetExecutionGraphAnalysisRequest) (*egapb.GetExecutionGraphAnalysisResponse, error) {
	if req.GetInvocationId() == "" {
		return nil, status.InvalidArgumentError("invocation_id is required")
	}
	// Authorize access to the requested invocation.
	ti, err := env.GetInvocationDB().LookupInvocation(ctx, req.GetInvocationId())
	if err != nil {
		return nil, err
	}
	analysis, err := ReadAnalysis(ctx, env.GetBlobstore(), req.GetInvocationId(), ti.Attempt)
	if err != nil {
		return nil, err
	}
	return &egapb.GetExecutionGraphAnalysisResponse{Analysis: analysis}, nil
}
