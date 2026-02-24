package usagelimits

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

var usageLimitsEnabled = flag.Bool("app.enable_usage_limits", false, "If set, usage limit enforcement will be enabled")

const (
	usageLimitsExperimentName = "usage.limits"
	usageLimitRedisKeyPrefix  = "usage_limits/v1"
	usageCacheTTL             = 5 * time.Minute

	usageLimitExceededMessageTemplate = "usage limit exceeded for %q - to increase usage limits, request a quote at https://buildbuddy.io/request-quote"
)

type limiter struct {
	env         environment.Env
	limitsCache sync.Map // map[limitsCacheKey][]usageLimit
}

type limitsCacheKey struct {
	fieldName string
	variant   string
}

type usageLimit struct {
	period string
	limit  int64
}

// usageLimitEntry is the parsed shape of each entry in the experiment config
// array for a given field name key.
type usageLimitEntry struct {
	Limit  int64  `json:"limit"`
	Period string `json:"period"`
}

func Register(env *real_environment.RealEnv) error {
	if !*usageLimitsEnabled {
		return nil
	}
	if env.GetServerContext() == nil {
		return status.FailedPreconditionError("usage limits require server context")
	}
	if env.GetExperimentFlagProvider() == nil {
		return status.FailedPreconditionError("usage limits require experiment flag provider")
	}
	if env.GetMetricsCollector() == nil {
		return status.FailedPreconditionError("usage limits require metrics collector")
	}
	if env.GetOLAPDBHandle() == nil {
		return status.FailedPreconditionError("usage limits require ClickHouse")
	}
	env.SetUsageLimiter(newLimiter(env, env.GetServerContext(), env.GetExperimentFlagProvider()))
	return nil
}

func newLimiter(env environment.Env, ctx context.Context, fp interfaces.ExperimentFlagProvider) interfaces.UsageLimiter {
	l := &limiter{env: env}
	l.listenForUpdates(ctx, fp)
	return l
}

// Check enforces usage-backed fixed-window limits. Each check floors the
// current UTC time to the configured period and reads usage through a
// short-lived Redis cache so hot request paths do not repeatedly query
// ClickHouse.
//
// This is less precise than burst/rate limiting, but more accurate for durable
// usage over larger windows. It is intended for hour/week/month usage limits,
// not short-window burst control.
func (l *limiter) Check(ctx context.Context, metric usagepb.UsageAlertingMetric_Value, quantity int64) error {
	field, ok := usage_service.UsageFieldForAlertingMetric(metric)
	if !ok {
		log.CtxWarningf(ctx, "Misconfigured usage limit check for unknown metric %q failed open", metric)
		return nil
	}
	allow, groupID, err := l.check(ctx, field, quantity)
	if err != nil {
		log.CtxWarningf(ctx, "Usage limit check for %q failed: %s", field.Name, err)
		// Do not block traffic when the usage limit system has issues.
		return nil
	}
	if allow {
		return nil
	}
	metrics.UsageLimitExceeded.With(prometheus.Labels{
		metrics.GroupID:    groupID,
		metrics.UsageField: field.Name,
	}).Inc()
	return status.ResourceExhaustedErrorf(usageLimitExceededMessageTemplate, field.Name)
}

func (l *limiter) check(ctx context.Context, field *usage_service.UsageField, quantity int64) (bool, string, error) {
	if quantity <= 0 {
		return true, "", nil
	}

	groupID := l.groupID(ctx)
	if groupID == "" {
		return true, "", nil
	}

	limits, err := l.limits(ctx, field.Name, groupID == interfaces.AuthAnonymousUser)
	if err != nil {
		return true, groupID, err
	}
	if len(limits) == 0 {
		return true, groupID, nil
	}

	now := time.Now().UTC()
	for _, limit := range limits {
		if quantity > limit.limit {
			return false, groupID, nil
		}
		start, end := usageWindow(now, limit.period)
		cacheKey := usageRedisKey(groupID, start, limit.period, field.Name)

		used, err := l.getUsage(ctx, cacheKey, groupID, field, start, end, limit.period)
		if err != nil {
			return true, groupID, err
		}
		if used >= limit.limit || quantity > limit.limit-used {
			return false, groupID, nil
		}
	}
	return true, groupID, nil
}

func (l *limiter) groupID(ctx context.Context) string {
	c, err := claims.ClaimsFromContext(ctx)
	if err == nil {
		if c.IsImpersonating() {
			return ""
		}
		return c.GetGroupID()
	}

	authenticator := l.env.GetAuthenticator()
	if authenticator != nil && authutil.IsAnonymousUserError(err) && authenticator.AnonymousUsageEnabled(ctx) {
		return interfaces.AuthAnonymousUser
	}
	return ""
}

func (l *limiter) limits(ctx context.Context, fieldName string, isAnonymous bool) ([]usageLimit, error) {
	fp := l.env.GetExperimentFlagProvider()
	if fp == nil {
		return nil, nil
	}
	var opts []any
	if isAnonymous {
		opts = append(opts, experiments.WithContext("group_id", interfaces.AuthAnonymousUser))
	}
	config, details := fp.ObjectDetails(ctx, usageLimitsExperimentName, make(map[string]any), opts...)
	variant := details.Variant()
	if variant != "" {
		// Cache parsed experiment definitions by variant. Per-group usage values
		// are cached separately in Redis.
		key := limitsCacheKey{fieldName: fieldName, variant: variant}
		if cached, ok := l.limitsCache.Load(key); ok {
			return cached.([]usageLimit), nil
		}
	}

	limits, err := usageLimitsFromConfig(config, fieldName)
	if err != nil {
		return nil, err
	}
	if variant != "" {
		key := limitsCacheKey{fieldName: fieldName, variant: variant}
		l.limitsCache.Store(key, limits)
	}
	return limits, nil
}

func usageLimitsFromConfig(config map[string]any, fieldName string) ([]usageLimit, error) {
	columnConfig, ok := config[fieldName]
	if !ok {
		return nil, nil
	}
	entriesRaw, ok := columnConfig.([]interface{})
	if !ok {
		return nil, status.InvalidArgumentErrorf("%s[%q] must be an array, got %T", usageLimitsExperimentName, fieldName, columnConfig)
	}
	limits := make([]usageLimit, 0, len(entriesRaw))
	for _, raw := range entriesRaw {
		entryMap, ok := raw.(map[string]any)
		if !ok {
			return nil, status.InvalidArgumentErrorf("%s[%q] entries must be objects, got %T", usageLimitsExperimentName, fieldName, raw)
		}
		var entry usageLimitEntry
		if err := experiments.ObjectToStruct(entryMap, &entry); err != nil {
			return nil, status.InvalidArgumentErrorf("invalid %s entry: %s", usageLimitsExperimentName, err)
		}
		if entry.Limit <= 0 {
			return nil, status.InvalidArgumentErrorf("%s.limit (%d) must be positive", usageLimitsExperimentName, entry.Limit)
		}
		period := strings.ToLower(strings.TrimSpace(entry.Period))
		if period != "hour" && period != "week" && period != "month" {
			return nil, status.InvalidArgumentErrorf("unsupported usage limit period %q", entry.Period)
		}
		limits = append(limits, usageLimit{period: period, limit: entry.Limit})
	}
	return limits, nil
}

func (l *limiter) getPersistedUsage(ctx context.Context, groupID string, field *usage_service.UsageField, start, end time.Time, period string) (int64, error) {
	return usage_service.QueryUsage(ctx, l.env.GetOLAPDBHandle(), "usage_limits_get_"+period, field, groupID, start, end)
}

func (l *limiter) getUsage(ctx context.Context, key, groupID string, field *usage_service.UsageField, start, end time.Time, period string) (int64, error) {
	var used int64
	values, err := l.env.GetMetricsCollector().GetAll(ctx, key)
	if err == nil && len(values) > 0 {
		used, err = strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return 0, err
		}
	} else {
		if err != nil && !isUsageCacheMiss(err) {
			return 0, err
		}
		used, err = l.getPersistedUsage(ctx, groupID, field, start, end, period)
		if err != nil {
			return 0, err
		}
		if err := l.env.GetMetricsCollector().Set(ctx, key, strconv.FormatInt(used, 10), usageCacheTTL); err != nil {
			log.CtxWarningf(ctx, "Failed to cache usage limit value for %q: %s", key, err)
		}
	}
	return used, nil
}

func isUsageCacheMiss(err error) bool {
	return errors.Is(err, redis.Nil) || status.IsNotFoundError(err)
}

func usageWindow(t time.Time, period string) (time.Time, time.Time) {
	t = t.UTC()
	switch period {
	case "hour":
		start := t.Truncate(time.Hour)
		return start, start.Add(time.Hour)
	case "week":
		start := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		daysSinceMonday := (int(start.Weekday()) + 6) % 7
		start = start.AddDate(0, 0, -daysSinceMonday)
		return start, start.AddDate(0, 0, 7)
	case "month":
		start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0)
	}
	return t, t
}

func usageRedisKey(groupID string, start time.Time, period string, fieldName string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%d", usageLimitRedisKeyPrefix, period, fieldName, groupID, start.Unix())
}

func (l *limiter) listenForUpdates(ctx context.Context, fp interfaces.ExperimentFlagProvider) {
	flagdChanges := make(chan struct{}, 1)
	unsubscribe := fp.Subscribe(flagdChanges)
	go func() {
		defer unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-flagdChanges:
				l.clearLimitsCache()
			}
		}
	}()
}

func (l *limiter) clearLimitsCache() {
	l.limitsCache.Range(func(key, value interface{}) bool {
		l.limitsCache.Delete(key)
		return true
	})
}
