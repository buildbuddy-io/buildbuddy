package execution_graph

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	egapb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph_analysis"
)

// blobName returns the blobstore path of an invocation's execution graph
// analysis. The analysis lives alongside the invocation's other derived blobs
// (like the cache scorecard) and shares their lifetime.
//
// WARNING: Things will break if this is changed, because we use this name to
// look up data from historical invocations. The name includes the analyzer
// version so that incompatible analyses never collide.
func blobName(invocationID string, invocationAttempt uint64) string {
	fileName := fmt.Sprintf("execution_graph_analysis_v%d.pb", AnalyzerVersion)
	return filepath.Join(invocationID, fmt.Sprint(invocationAttempt), fileName)
}

// WriteAnalysis stores the analysis proto in the blobstore.
func WriteAnalysis(ctx context.Context, blobStore interfaces.Blobstore, invocationAttempt uint64, analysis *egapb.ExecutionGraphAnalysis) error {
	buf, err := proto.Marshal(analysis)
	if err != nil {
		return err
	}
	_, err = blobStore.WriteBlob(ctx, blobName(analysis.GetInvocationId(), invocationAttempt), buf)
	return err
}

// ReadAnalysis looks up the stored analysis for an invocation attempt.
// Returns a NotFound error if the invocation has no stored analysis.
func ReadAnalysis(ctx context.Context, blobStore interfaces.Blobstore, invocationID string, invocationAttempt uint64) (*egapb.ExecutionGraphAnalysis, error) {
	buf, err := blobStore.ReadBlob(ctx, blobName(invocationID, invocationAttempt))
	if err != nil {
		return nil, err
	}
	analysis := &egapb.ExecutionGraphAnalysis{}
	if err := proto.Unmarshal(buf, analysis); err != nil {
		return nil, status.WrapError(err, "unmarshal execution graph analysis")
	}
	return analysis, nil
}

// HasAnalysis returns whether an analysis is already stored for the
// invocation attempt.
func HasAnalysis(ctx context.Context, blobStore interfaces.Blobstore, invocationID string, invocationAttempt uint64) (bool, error) {
	return blobStore.BlobExists(ctx, blobName(invocationID, invocationAttempt))
}
