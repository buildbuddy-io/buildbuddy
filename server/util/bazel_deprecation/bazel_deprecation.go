package bazel_deprecation

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/logrusorgru/aurora"
)

var (
	deprecateAnonymousAccess = flag.Bool("app.deprecate_anonymous_access", false, "If true, log a warning in the bazel console when clients are unauthenticated")
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

func deprecationError(msg string) error {
	buf := "\033[2K" // Clear the line

	// The line is cleared but cursor is in the wrong place.
	// Reset it to the left.
	buf += "\r"

	// Print a VISIBLE WARNING that will be clear on both dark/light
	// terminals.
	buf += aurora.Sprintf("%s: %s\n", aurora.BgBrightYellow(aurora.Black("BuildBuddy Notice")), aurora.Red(msg))
	return status.FailedPreconditionError(buf)
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

	if *deprecateAnonymousAccess && isAnonymousBuild(ctx) {
		defer w.incrementWarningCount(ctx)
		return deprecationError("Anonymous access is deprecated; please create an account at https://app.buildbuddy.io/")
	}
	return nil
}
