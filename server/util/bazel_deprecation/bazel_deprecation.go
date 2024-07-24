package bazel_deprecation

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	bazelToolName   = "bazel"
	profileActionID = "bes-upload"

	warningMetricPrefix = "warning-"
	warningKey          = "warnings"
)

func isAnonymousBuild(ctx context.Context) bool {
	_, err := claims.ClaimsFromContext(ctx)
	return status.IsUnauthenticatedError(err)
}

type Warner struct {
	env environment.Env
}

func NewWarner(env environment.Env) *Warner {
	return &Warner{env: env}
}

func (w *Warner) getWarningCount(ctx context.Context) int64 {
	m := w.env.GetMetricsCollector()
	if m == nil {
		return 0
	}

	countKey := warningMetricPrefix + bazel_request.GetInvocationID(ctx)
	read, err := m.ReadCounts(ctx, countKey)
	log.Printf("warning count: %+v", read)
	if err == nil {
		return read[warningKey]
	}
	return 0
}

func (w *Warner) incrementWarningCount(ctx context.Context) {
	m := w.env.GetMetricsCollector()
	if m == nil {
		return
	}
	countKey := warningMetricPrefix + bazel_request.GetInvocationID(ctx)
	m.IncrementCount(ctx, countKey, warningKey, 1)
}

func (w *Warner) Warn(ctx context.Context) error {
	canWarn := bazel_request.GetToolName(ctx) == bazelToolName && bazel_request.GetActionID(ctx) == profileActionID
	if !canWarn {
		return nil
	}

	if w.getWarningCount(ctx) >= 1 {
		// Already warned.
		return nil
	}

	if isAnonymousBuild(ctx) {
		defer w.incrementWarningCount(ctx)
		return status.FailedPreconditionError("Anonymous access is deprecated; please create an account at https://app.buildbuddy.io/")
	}
	return nil
}
