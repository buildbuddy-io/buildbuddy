package usagelimits_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
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

func limit(field, period string, limit int64) map[string]any {
	return map[string]any{field: []any{map[string]any{"period": period, "limit": float64(limit)}}}
}

func insertUsage(t *testing.T, env environment.Env, usageSKU sku.SKU, periodStart time.Time, count int64) {
	require.NoError(t, env.GetOLAPDBHandle().NewQuery(context.Background(), "test_insert_usage").Raw(`
		INSERT INTO "RawUsage" (group_id, sku, period_start, count) VALUES (?, ?, ?, ?)
	`, "GR001", usageSKU.String(), periodStart, count).Exec().Error)
}

func TestMonthlyLimit(t *testing.T) {
	env, ctx := setupEnv(t, limit("total_download_size_bytes", "month", 100))
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart.Add(12*time.Hour), 95)
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart.Add(-time.Hour), 1000)
	l, err := usagelimits.New(env)
	require.NoError(t, err)

	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 5))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 6)))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 101)))
	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 1000))
}

func TestHourlyLimit(t *testing.T) {
	env, ctx := setupEnv(t, limit("total_upload_size_bytes", "hour", 10))
	hourStart := time.Now().UTC().Truncate(time.Hour)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, hourStart, 9)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, hourStart.Add(-time.Hour), 100)
	l, err := usagelimits.New(env)
	require.NoError(t, err)

	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 2)))
}

func TestWeeklyLimit(t *testing.T) {
	env, ctx := setupEnv(t, limit("total_upload_size_bytes", "week", 10))
	now := time.Now().UTC()
	weekStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -((int(now.Weekday()) + 6) % 7))
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, weekStart, 9)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, weekStart.Add(-time.Hour), 100)
	l, err := usagelimits.New(env)
	require.NoError(t, err)

	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 2)))
}

func TestMultipleLimits(t *testing.T) {
	limits := limit("total_download_size_bytes", "month", 100)
	limits["total_upload_size_bytes"] = limit("total_upload_size_bytes", "hour", 10)["total_upload_size_bytes"]
	env, ctx := setupEnv(t, limits)
	now := time.Now().UTC()
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC), 99)
	insertUsage(t, env, sku.RemoteCacheCASUploadedBytes, now.Truncate(time.Hour), 9)
	l, err := usagelimits.New(env)
	require.NoError(t, err)

	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 2)))
	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 1))
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES, 2)))
}

func TestUsageIsCached(t *testing.T) {
	env, ctx := setupEnv(t, limit("total_download_size_bytes", "month", 100))
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart.Add(12*time.Hour), 50)
	l, err := usagelimits.New(env)
	require.NoError(t, err)
	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 50))

	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, monthStart.Add(13*time.Hour), 50)
	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 50), "in-memory cache still has the old usage")
	l2, err := usagelimits.New(env)
	require.NoError(t, err)
	assert.NoError(t, l2.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 50), "redis cache still has the old usage")
}

func TestUnauthenticatedAndImpersonatingRequestsAreNotLimited(t *testing.T) {
	env, _ := setupEnv(t, limit("total_download_size_bytes", "month", 100))
	l, err := usagelimits.New(env)
	require.NoError(t, err)

	assert.NoError(t, l.Check(context.Background(), usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1000))
	impersonating := testauth.WithAuthenticatedUserInfo(context.Background(), &claims.Claims{UserID: "US001", GroupID: "GR001", Impersonating: true})
	assert.NoError(t, l.Check(impersonating, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1000))
}

func TestNoLimitsConfigured(t *testing.T) {
	env, ctx := setupEnv(t, map[string]any{})
	now := time.Now().UTC()
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC), 1000)
	l, err := usagelimits.New(env)
	require.NoError(t, err)
	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1000))
}

func TestFlagChangesApply(t *testing.T) {
	env, ctx := setupEnv(t, map[string]any{})
	flagFile := func(limit int64, state string) string {
		return fmt.Sprintf(`{
  "flags": {
    "usage.limits": {
      "state": "%s",
      "defaultVariant": "custom",
      "variants": { "custom": { "total_download_size_bytes": [ { "period": "month", "limit": %d } ] } }
    }
  }
}`, state, limit)
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

	now := time.Now().UTC()
	insertUsage(t, env, sku.RemoteCacheCASDownloadedBytes, time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC), 101)
	l, err := usagelimits.New(env)
	require.NoError(t, err)
	assert.True(t, status.IsFailedPreconditionError(l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1)))

	require.NoError(t, os.WriteFile(f.Name(), []byte(flagFile(200, "ENABLED")), 0644))
	assert.Eventually(t, func() bool { return l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1) == nil }, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, os.WriteFile(f.Name(), []byte(flagFile(100, "DISABLED")), 0644))
	assert.Eventually(t, func() bool { return l.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1000) == nil }, 5*time.Second, 50*time.Millisecond)
}

func TestUnknownMetricFailsOpen(t *testing.T) {
	env, ctx := setupEnv(t, map[string]any{})
	l, err := usagelimits.New(env)
	require.NoError(t, err)
	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_UNKNOWN, 1))
}

func TestQueryErrorFailsOpen(t *testing.T) {
	// cloud_cpu_nanos filters on labels with ClickHouse syntax that the
	// sqlite test DB rejects, so the usage query fails.
	env, ctx := setupEnv(t, limit("cloud_cpu_nanos", "month", 1))
	errs := metrics.UsageLimitCheckErrors.WithLabelValues("cloud_cpu_nanos")
	before := testutil.ToFloat64(errs)
	l, err := usagelimits.New(env)
	require.NoError(t, err)

	assert.NoError(t, l.Check(ctx, usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS, 1))
	assert.Equal(t, before+1, testutil.ToFloat64(errs))
}
