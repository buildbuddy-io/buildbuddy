package usagelimits

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
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

func readCachedUsage(t testing.TB, mc interfaces.MetricsCollector, key string) int64 {
	t.Helper()
	values, err := mc.GetAll(context.Background(), key)
	require.NoError(t, err)
	require.Len(t, values, 1)
	used, err := strconv.ParseInt(values[0], 10, 64)
	require.NoError(t, err)
	return used
}

func setupEnv(t *testing.T) *testenv.TestEnv {
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
	return env
}

func insertUsage(t *testing.T, env environment.Env, groupID string, usageSKU sku.SKU, periodStart time.Time, count int64) {
	require.NoError(t, env.GetOLAPDBHandle().NewQuery(context.Background(), "test_insert_usage").Raw(`
		INSERT INTO "RawUsage" (group_id, sku, period_start, count)
		VALUES (?, ?, ?, ?)
	`, groupID, usageSKU.String(), periodStart, count).Exec().Error)
}

func TestUsageLimitFlagdConfig(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)

	user := testauth.User("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		user.UserID: user,
	}))
	authCtx := testauth.WithAuthenticatedUserInfo(ctx, user)
	field, ok := usage_service.UsageFieldForAlertingMetric(usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES)
	require.True(t, ok)

	flags := map[string]memprovider.InMemoryFlag{
		usageLimitsExperimentName: {
			State:          memprovider.Enabled,
			DefaultVariant: "custom",
			Variants: map[string]any{
				"custom": map[string]any{
					field.Name: []any{
						map[string]any{
							"period": "month",
							"limit":  float64(100),
						},
					},
				},
			},
		},
	}
	provider := memprovider.NewInMemoryProvider(flags)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	start, _ := usageWindow(time.Now().UTC(), "month")
	insertUsage(t, env, user.GroupID, sku.RemoteCacheCASDownloadedBytes, start.Add(12*time.Hour), 95)

	limiter := newLimiterForTesting(env)
	require.NoError(t, limiter.Check(authCtx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 5))
	require.True(t, status.IsResourceExhaustedError(limiter.Check(authCtx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 6)))
	require.True(t, status.IsResourceExhaustedError(limiter.Check(authCtx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 101)))
	assert.Equal(t, int64(95), readCachedUsage(t, env.GetMetricsCollector(),
		usageRedisKey(user.GroupID, start, "month", field.Name)))
}

func TestUsageLimitAnonymousGroupTargeting(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)
	field, ok := usage_service.UsageFieldForAlertingMetric(usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES)
	require.True(t, ok)

	flagPath := writeFlagConfig(t, targetedUsageLimitFlagConfig(field.Name, interfaces.AuthAnonymousUser, 100))
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(flagPath))
	require.NoError(t, err)
	t.Cleanup(provider.Shutdown)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))
	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	start, _ := usageWindow(time.Now().UTC(), "month")
	insertUsage(t, env, interfaces.AuthAnonymousUser, sku.RemoteCacheCASDownloadedBytes, start.Add(12*time.Hour), 95)

	limiter := newLimiterForTesting(env)
	require.NoError(t, limiter.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 5))
	require.True(t, status.IsResourceExhaustedError(limiter.Check(ctx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 6)))
	require.NoError(t, limiter.Check(testauth.WithAuthenticatedUserInfo(ctx, testauth.User("US001", "GR001")), usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 101))
	assert.Equal(t, int64(95), readCachedUsage(t, env.GetMetricsCollector(),
		usageRedisKey(interfaces.AuthAnonymousUser, start, "month", field.Name)))
}

func TestUsageLimitCacheInitializesFromClickHouse(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)

	start, end := usageWindow(time.Now().UTC(), "month")
	insertUsage(t, env, "GR001", sku.RemoteCacheCASDownloadedBytes, start.Add(12*time.Hour), 10)

	field, ok := usage_service.UsageFieldForAlertingMetric(usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES)
	require.True(t, ok)

	limiter := newLimiterForTesting(env)
	key := usageRedisKey("GR001", start, "month", field.Name)
	used, err := limiter.getUsage(ctx, key, "GR001", field, start, end, "month")
	require.NoError(t, err)
	assert.Equal(t, int64(10), used)

	// Second call should hit the Redis cache (not re-query ClickHouse).
	used, err = limiter.getUsage(ctx, key, "GR001", field, start, end, "month")
	require.NoError(t, err)
	assert.Equal(t, int64(10), used)
	assert.Equal(t, int64(10), readCachedUsage(t, env.GetMetricsCollector(), key))
}

func TestUsageLimitMultipleMetricsAndPeriods(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)

	user := testauth.User("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		user.UserID: user,
	}))
	execField, ok := usage_service.UsageFieldForAlertingMetric(usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS)
	require.True(t, ok)
	downloadField, ok := usage_service.UsageFieldForAlertingMetric(usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES)
	require.True(t, ok)

	flags := map[string]memprovider.InMemoryFlag{
		usageLimitsExperimentName: {
			State:          memprovider.Enabled,
			DefaultVariant: "custom",
			Variants: map[string]any{
				"custom": map[string]any{
					execField.Name: []any{
						map[string]any{"period": "hour", "limit": float64(10)},
					},
					downloadField.Name: []any{
						map[string]any{"period": "week", "limit": float64(50)},
					},
				},
			},
		},
	}
	provider := memprovider.NewInMemoryProvider(flags)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))
	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	now := time.Now().UTC()
	weekStart, _ := usageWindow(now, "week")

	insertUsage(t, env, user.GroupID, sku.RemoteCacheCASDownloadedBytes, weekStart, 49)

	limiter := newLimiterForTesting(env)
	authCtx := testauth.WithAuthenticatedUserInfo(ctx, user)

	require.NoError(t, limiter.Check(authCtx, usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS, 10))
	require.True(t, status.IsResourceExhaustedError(limiter.Check(authCtx, usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS, 11)))
	require.NoError(t, limiter.Check(authCtx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 1))
	require.True(t, status.IsResourceExhaustedError(limiter.Check(authCtx, usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES, 2)))
}

func TestUsageLimitWeeklyPeriod(t *testing.T) {
	start, end := usageWindow(time.Date(2026, 5, 13, 15, 30, 0, 0, time.UTC), "week")
	assert.Equal(t, time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC), end)

	limits, err := usageLimitsFromConfig(map[string]any{
		"total_download_size_bytes": []any{
			map[string]any{"period": "week", "limit": float64(100)},
		},
	}, "total_download_size_bytes")
	require.NoError(t, err)
	require.Equal(t, []usageLimit{{period: "week", limit: 100}}, limits)
}

func TestUsageLimitFlagdUpdateClearsLimitsCache(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)
	field, ok := usage_service.UsageFieldForAlertingMetric(usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES)
	require.True(t, ok)

	flagPath := writeFlagConfig(t, usageLimitFlagConfig(field.Name, 100))
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(flagPath))
	require.NoError(t, err)
	t.Cleanup(provider.Shutdown)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))
	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	limiter := newLimiterForTesting(env)
	limits, err := limiter.limits(ctx, field.Name, false)
	require.NoError(t, err)
	require.Equal(t, []usageLimit{{period: "month", limit: 100}}, limits)

	require.NoError(t, os.WriteFile(flagPath, []byte(usageLimitFlagConfig(field.Name, 200)), 0644))
	require.Eventually(t, func() bool {
		limits, err := limiter.limits(ctx, field.Name, false)
		return err == nil && len(limits) == 1 && limits[0] == (usageLimit{period: "month", limit: 200})
	}, 5*time.Second, 50*time.Millisecond)
}

func TestUsageLimitUnknownMetricFailsOpen(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)

	user := testauth.User("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		user.UserID: user,
	}))
	authCtx := testauth.WithAuthenticatedUserInfo(ctx, user)

	// An unrecognized metric should fail open rather than blocking traffic.
	limiter := newLimiterForTesting(env)
	require.NoError(t, limiter.Check(authCtx, usagepb.UsageAlertingMetric_UNKNOWN, 1))
}

func newLimiterForTesting(env environment.Env) *limiter {
	l := &limiter{env: env}
	// Only start the flagd listener if a provider is available; most test
	// cases don't set one up.
	if fp := env.GetExperimentFlagProvider(); fp != nil {
		l.listenForUpdates(context.Background(), fp)
	}
	return l
}

func usageLimitFlagConfig(fieldName string, limit int64) string {
	return fmt.Sprintf(`{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "%s": {
      "state": "ENABLED",
      "defaultVariant": "custom",
      "variants": {
        "custom": {
          "%s": [
            { "period": "month", "limit": %d }
          ]
        }
      }
    }
  }
}`, usageLimitsExperimentName, fieldName, limit)
}

func targetedUsageLimitFlagConfig(fieldName, groupID string, limit int64) string {
	return fmt.Sprintf(`{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "%s": {
      "state": "ENABLED",
      "defaultVariant": "default",
      "variants": {
        "custom": {
          "%s": [
            { "period": "month", "limit": %d }
          ]
        },
        "default": {}
      },
      "targeting": {
        "if": [
          { "==": [{ "var": "group_id" }, "%s"] },
          "custom",
          "default"
        ]
      }
    }
  }
}`, usageLimitsExperimentName, fieldName, limit, groupID)
}

func writeFlagConfig(t testing.TB, data string) string {
	t.Helper()
	f, err := os.CreateTemp(os.Getenv("TEST_TMPDIR"), "buildbuddy-test-file-*.flagd.json")
	require.NoError(t, err)
	path := f.Name()
	_, err = f.Write([]byte(data))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	t.Cleanup(func() {
		os.RemoveAll(path)
	})
	return path
}
