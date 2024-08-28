package bazel_deprecation

import (
	"context"
	"math"

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

func deprecationError(title string, description ...string) error {
	buf := "\033[2K" // Clear the line

	// The line is cleared but cursor is in the wrong place.
	// Reset it to the left.
	buf += "\r"

	header := "Deprecation Notice"
	padding := 2
	extraSpace := float64(len(title)-len(header)) / 2
	leftPadding := padding + int(math.Floor(extraSpace))
	rightPadding := padding + int(math.Ceil(extraSpace))
	width := len(title) + (2 * padding)
	blankLine := borderWrap(printN(width, " "))

	buf += "\n"
	buf += aurora.Sprintf(aurora.Cyan("  %s  \n"), printN(width, "_"))
	buf += blankLine
	buf += borderWrap(aurora.Sprintf("%s%s%s", printN(leftPadding, " "), aurora.Bold(aurora.Red(header)), printN(rightPadding, " ")))
	buf += blankLine
	buf += borderWrap(aurora.Sprintf("  %s  ", aurora.Bold(title)))
	buf += blankLine
	for _, d := range description {
		buf += borderWrap(aurora.Sprintf("  %s%s  ", aurora.White(d), printN(len(title)-len(d), " ")))
	}

	buf += blankLine
	buf += borderWrap(aurora.Sprintf(aurora.Cyan(printN(width, "_"))))
	buf += "\n"
	return status.FailedPreconditionError(buf)
}

func borderWrap(content string) string {
	return aurora.Sprintf(" %s%s%s \n", aurora.Cyan("|"), content, aurora.Cyan("|"))
}

func printN(n int, s string) string {
	buf := ""
	for range n {
		buf += s
	}
	return buf
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
		return deprecationError("Anonymous BuildBuddy access is deprecated, and will soon be disabled.",
			"No BuildBuddy API key was found attached to this build.", "", "To continue using BuildBuddy, create a free account at ",
			"https://app.buildbuddy.io/ and use the Quick Start guide ",
			"to configure your build to use an API key.")
	}
	return nil
}
