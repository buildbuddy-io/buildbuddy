package usagelimits_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usagelimits"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
)

const (
	download = usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES
	upload   = usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES
)

type queryOnlyOLAPDBHandle struct {
	interfaces.OLAPDBHandle
	db interfaces.DB
}

func (h *queryOnlyOLAPDBHandle) NewQuery(ctx context.Context, name string) interfaces.DBQuery {
	return h.db.NewQuery(ctx, name)
}

func (h *queryOnlyOLAPDBHandle) DialectName() string {
	return h.db.DialectName()
}

// setupEnv returns an env with a sqlite RawUsage table standing in for
// ClickHouse, an in-memory metrics collector standing in for Redis, the given
// usage.limits config, and a context authenticated as group GR001.
func setupEnv(t *testing.T, limits map[string]any) (*testenv.TestEnv, context.Context) {
	env := testenv.GetTestEnv(t)
	env.SetOLAPDBHandle(&queryOnlyOLAPDBHandle{db: env.GetDBHandle()})
	require.NoError(t, env.GetOLAPDBHandle().NewQuery(context.Background(), "test_create_raw_usage").Raw(`
		CREATE TABLE "RawUsage" (
			group_id TEXT,
			sku TEXT,
			labels TEXT DEFAULT '{}',
			period_start TIMESTAMP,
			count INTEGER
		)
	`).Exec().Error)
	mc, err := memory_metrics_collector.NewMemoryMetricsCollector()
	require.NoError(t, err)
	env.SetMetricsCollector(mc)

	provider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"usage.limits": {State: memprovider.Enabled, DefaultVariant: "custom", Variants: map[string]any{"custom": limits}},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), provider))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	user := testauth.User("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{user.UserID: user}))
	return env, testauth.WithAuthenticatedUserInfo(context.Background(), user)
}

func limit(metric usagepb.UsageAlertingMetric_Value, period string, limit int64) map[string]any {
	field, _ := usage_service.UsageFieldForAlertingMetric(metric)
	return map[string]any{field.Name: []any{map[string]any{"period": period, "limit": float64(limit)}}}
}

func insertUsage(t *testing.T, env environment.Env, usageSKU sku.SKU, periodStart time.Time, count int64) {
	require.NoError(t, env.GetOLAPDBHandle().NewQuery(context.Background(), "test_insert_usage").Raw(`
		INSERT INTO "RawUsage" (group_id, sku, period_start, count) VALUES (?, ?, ?, ?)
	`, "GR001", usageSKU.String(), periodStart, count).Exec().Error)
}

func newLimiter(t *testing.T, env environment.Env) interfaces.UsageLimiter {
	l, err := usagelimits.New(env)
	require.NoError(t, err)
	return l
}

func monthStart() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func TestMonthlyLimit(t *testing.T) {
	env, ctx := setupEnv(t, limit(download, "month", 100))
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(12*time.Hour), 95)
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(-time.Hour), 1000)
	l := newLimiter(t, env)

	assert.NoError(t, l.Check(ctx, download, 5))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, download, 6)))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, download, 101)))
	assert.NoError(t, l.Check(ctx, upload, 1000))
}

func TestMultipleLimits(t *testing.T) {
	limits := limit(download, "month", 100)
	for k, v := range limit(upload, "hour", 10) {
		limits[k] = v
	}
	env, ctx := setupEnv(t, limits)
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(12*time.Hour), 99)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, time.Now().UTC().Truncate(time.Hour), 9)
	l := newLimiter(t, env)

	assert.NoError(t, l.Check(ctx, download, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, download, 2)))
	assert.NoError(t, l.Check(ctx, upload, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, upload, 2)))
}

func TestHourlyLimit(t *testing.T) {
	env, ctx := setupEnv(t, limit(upload, "hour", 10))
	hourStart := time.Now().UTC().Truncate(time.Hour)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, hourStart, 9)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, hourStart.Add(-time.Hour), 100)
	l := newLimiter(t, env)

	assert.NoError(t, l.Check(ctx, upload, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, upload, 2)))
}

func TestWeeklyLimit(t *testing.T) {
	env, ctx := setupEnv(t, limit(upload, "week", 10))
	now := time.Now().UTC()
	weekStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -((int(now.Weekday()) + 6) % 7))
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, weekStart, 9)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, weekStart.Add(-time.Hour), 100)
	l := newLimiter(t, env)

	assert.NoError(t, l.Check(ctx, upload, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, upload, 2)))
}

func TestUsageIsCached(t *testing.T) {
	env, ctx := setupEnv(t, limit(download, "month", 100))
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(12*time.Hour), 50)
	l := newLimiter(t, env)
	assert.NoError(t, l.Check(ctx, download, 50))

	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(13*time.Hour), 50)
	assert.NoError(t, l.Check(ctx, download, 50), "in-memory cache still has the old usage")
	assert.NoError(t, newLimiter(t, env).Check(ctx, download, 50), "redis cache still has the old usage")
}

func TestUnauthenticatedAndImpersonatingRequestsAreNotLimited(t *testing.T) {
	env, _ := setupEnv(t, limit(download, "month", 100))
	l := newLimiter(t, env)

	assert.NoError(t, l.Check(context.Background(), download, 1000))
	impersonating := testauth.WithAuthenticatedUserInfo(context.Background(), &claims.Claims{UserID: "US001", GroupID: "GR001", Impersonating: true})
	assert.NoError(t, l.Check(impersonating, download, 1000))
}

func TestNoLimitsConfigured(t *testing.T) {
	env, ctx := setupEnv(t, map[string]any{})
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(12*time.Hour), 1000)
	assert.NoError(t, newLimiter(t, env).Check(ctx, download, 1000))
}

func TestFlagChangesApply(t *testing.T) {
	env, ctx := setupEnv(t, map[string]any{})
	field, _ := usage_service.UsageFieldForAlertingMetric(download)
	flagFile := func(limit int64, state string) string {
		return fmt.Sprintf(`{
  "flags": {
    "usage.limits": {
      "state": "%s",
      "defaultVariant": "custom",
      "variants": { "custom": { "%s": [ { "period": "month", "limit": %d } ] } }
    }
  }
}`, state, field.Name, limit)
	}
	f, err := os.CreateTemp(os.Getenv("TEST_TMPDIR"), "flags-*.json")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.Name(), []byte(flagFile(100, "ENABLED")), 0644))
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(f.Name()))
	require.NoError(t, err)
	t.Cleanup(provider.Shutdown)
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), provider))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart().Add(12*time.Hour), 101)
	l := newLimiter(t, env)
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, download, 1)))

	require.NoError(t, os.WriteFile(f.Name(), []byte(flagFile(200, "ENABLED")), 0644))
	assert.Eventually(t, func() bool { return l.Check(ctx, download, 1) == nil }, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, os.WriteFile(f.Name(), []byte(flagFile(100, "DISABLED")), 0644))
	assert.Eventually(t, func() bool { return l.Check(ctx, download, 1000) == nil }, 5*time.Second, 50*time.Millisecond)
}

func TestUnknownMetricFailsOpen(t *testing.T) {
	env, ctx := setupEnv(t, map[string]any{})
	assert.NoError(t, newLimiter(t, env).Check(ctx, usagepb.UsageAlertingMetric_UNKNOWN, 1))
}

func TestQueryErrorFailsOpen(t *testing.T) {
	// cloud_cpu_nanos filters on labels with ClickHouse syntax that the
	// sqlite test DB rejects, so the usage query fails.
	cpu := usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS
	env, ctx := setupEnv(t, limit(cpu, "month", 1))
	field, _ := usage_service.UsageFieldForAlertingMetric(cpu)
	errs := metrics.UsageLimitCheckErrors.WithLabelValues(field.Name)
	before := testutil.ToFloat64(errs)

	assert.NoError(t, newLimiter(t, env).Check(ctx, cpu, 1))
	assert.Equal(t, before+1, testutil.ToFloat64(errs))
}
