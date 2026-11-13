package quota

import (
	"context"
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/quota"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/goredisstore.v8"

	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

var (
	quotaManagerEnabled = flag.Bool("app.enable_quota_management", false, "If set, quota management will be enabled")
)

const (
	maxRedisRetry                = 3
	redisQuotaKeyPrefix          = "quota"
	bucketQuotaExperimentName    = "quota.buckets"
	quotaExceededMessageTemplate = "quota exceeded for %q - to increase quota, request a quote at https://buildbuddy.io/request-quote"
)

func Register(env *real_environment.RealEnv) error {
	qm, err := NewQuotaManager(env)
	if err != nil {
		return err
	}
	env.SetQuotaManager(qm)
	return nil
}

func NewQuotaManager(env environment.Env) (*QuotaManager, error) {
	return newQuotaManager(env, createGCRABucket)
}

type QuotaManager struct {
	env           environment.Env
	namespaces    sync.Map // map[string]*namespace
	bucketFactory bucketFactory
}

// Allow checks whether the quota has enough room for quantity and consumes it
// only if allowed. Pass quantity 0 for a read-only check. If force is true,
// quota is applied even if quota management is globally disabled.
func (qm *QuotaManager) Allow(ctx context.Context, namespace string, quantity int64, force bool) error {
	allow, err := qm.applyQuota(ctx, namespace, quantity, false /* recordUsageOverages */, force)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to check quota for %q: %s", namespace, err)
		// Do not block traffic when the quota system has issues.
		return nil
	}
	if !allow {
		return status.ResourceExhaustedErrorf(quotaExceededMessageTemplate, namespace)
	}
	return nil
}

// RecordUsage always consumes quantity, even if it exceeds quota. Returns false
// if the usage went over quota. If force is true, usage is recorded even if
// quota management is globally disabled.
func (qm *QuotaManager) RecordUsage(ctx context.Context, namespace string, quantity int64, force bool) (bool, error) {
	// Account for partial work even when the client canceled the request.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return qm.applyQuota(ctx, namespace, quantity, true /* recordUsageOverages */, force)
}

// applyQuota returns whether quantity fit within quota. Errors indicate that
// checking or recording usage failed. If recordUsageOverages is true, usage is
// recorded even if it exceeds quota. If force is true, quota is applied even if
// quota management is globally disabled.
func (qm *QuotaManager) applyQuota(ctx context.Context, namespace string, quantity int64, recordUsageOverages, force bool) (bool, error) {
	if !*quotaManagerEnabled && !force {
		return true, nil
	}

	key, err := quota.GetKey(ctx, qm.env)
	if err != nil {
		metrics.QuotaKeyEmptyCount.With(prometheus.Labels{
			metrics.QuotaNamespace: namespace,
		}).Inc()
		// Generating a quota key fails for unauthenticated requests.
		// Auth should gate these, not the quota system so allow the request here.
		return true, nil
	}

	if err := qm.loadQuotasFromFlagd(ctx, key, namespace); err != nil {
		return false, status.WrapError(err, "load quotas from flagd")
	}

	b := qm.findBucket(namespace, key)
	if b == nil {
		return true, nil
	}

	allow, err := b.Allow(ctx, key, quantity, recordUsageOverages)
	if err != nil {
		return false, err
	}
	if allow {
		return true, nil
	}

	metrics.QuotaExceeded.With(prometheus.Labels{
		metrics.QuotaNamespace: namespace,
		metrics.QuotaKey:       key,
	}).Inc()
	return false, nil
}

type namespace struct {
	name         string
	bucketsByKey sync.Map // map[string]Bucket
}

type bucketConfig struct {
	namespace          string
	name               string
	numRequests        int64
	periodDurationUsec int64
	maxBurst           int64
}

func (b *bucketConfig) validate() error {
	if b.name == "" {
		return status.InvalidArgumentError("bucket.name cannot be empty")
	}
	if b.numRequests <= 0 {
		return status.InvalidArgumentErrorf("bucket.numRequests (%d) must be positive", b.numRequests)
	}
	if b.periodDurationUsec <= 0 {
		return status.InvalidArgumentError("bucket.periodDurationUsec must be positive")
	}
	if b.maxBurst < 0 {
		return status.InvalidArgumentErrorf("bucket.maxBurst (%d) must be non-negative", b.maxBurst)
	}
	return nil
}

type assignedBucket struct {
	bucket    *bucketConfig
	quotaKeys []string
}

type Bucket interface {
	Config() bucketConfig
	// Allow returns whether the requested quantity fits within quota.
	// If recordUsageOverages is true, quota is always consumed, even if it exceeds the limit.
	// If it is false, quota is only consumed if it fits within the limit.
	Allow(ctx context.Context, key string, quantity int64, recordUsageOverages bool) (bool, error)
}

type rateLimitedBucket struct {
	config      *bucketConfig
	rateLimiter *throttled.GCRARateLimiterCtx
}

func (b *rateLimitedBucket) Config() bucketConfig {
	return *b.config
}

func (b *rateLimitedBucket) Allow(ctx context.Context, key string, quantity int64, recordUsageOverages bool) (bool, error) {
	if quantity > math.MaxInt {
		return false, status.InternalErrorf("quantity (%d) exceeds the limit", quantity)
	}
	limitExceeded, _, err := b.rateLimiter.RateLimitCtx(ctx, key, int(quantity), recordUsageOverages)
	return !limitExceeded, err
}

type bucketFactory func(environment.Env, *bucketConfig) (Bucket, error)

func createGCRABucket(env environment.Env, config *bucketConfig) (Bucket, error) {
	prefix := strings.Join([]string{redisQuotaKeyPrefix, config.namespace, config.name, ""}, ":")
	store, err := goredisstore.NewCtx(env.GetDefaultRedisClient(), prefix)
	if err != nil {
		return nil, status.InternalErrorf("unable to init redis store: %s", err)
	}

	period := time.Duration(config.periodDurationUsec) * time.Microsecond
	quota := throttled.RateQuota{
		MaxRate:  throttled.PerDuration(int(config.numRequests), period),
		MaxBurst: int(config.maxBurst),
	}

	rateLimiter, err := throttled.NewGCRARateLimiterCtx(store, quota)
	if err != nil {
		return nil, status.InternalErrorf("unable to create GCRARateLimiter: %s", err)
	}
	rateLimiter.SetMaxCASAttemptsLimit(maxRedisRetry)

	return &rateLimitedBucket{
		config:      config,
		rateLimiter: rateLimiter,
	}, nil
}

func newQuotaManager(env environment.Env, factory bucketFactory) (*QuotaManager, error) {
	qm := &QuotaManager{
		env:           env,
		bucketFactory: factory,
	}

	qm.listenForUpdates(env.GetServerContext())
	return qm, nil
}

func (qm *QuotaManager) createNamespaceWithBucket(env environment.Env, name string, key string, config *bucketConfig) (*namespace, error) {
	bucket, err := qm.bucketFactory(env, config)
	if err != nil {
		return nil, err
	}

	ns := &namespace{
		name: name,
	}
	ns.bucketsByKey.Store(key, bucket)
	return ns, nil
}

// Flagd quotas are loaded lazily from request context on first use.
// Configuration format:
//
//	"rpc:/google.bytestream.ByteStream/Read": {
//	  "maxRate": {
//	    "numRequests": 10,
//	    "periodUsec": 60000000
//	  },
//	  "maxBurst": 5
//	}
func (qm *QuotaManager) loadQuotasFromFlagd(ctx context.Context, key, nsString string) error {
	if qm.env.GetExperimentFlagProvider() == nil {
		return status.InternalError("experiment flag provider not configured")
	}

	if nsInterface, ok := qm.namespaces.Load(nsString); ok {
		ns := nsInterface.(*namespace)
		if _, exists := ns.bucketsByKey.Load(key); exists {
			return nil
		}
	}

	config, err := qm.parseFlagdBucketConfig(ctx, nsString)
	if err != nil {
		return err
	}
	if config == nil {
		return nil
	}

	ns, err := qm.createNamespaceWithBucket(qm.env, nsString, key, config)
	if err != nil {
		return status.InternalErrorf("failed to create namespace for namespace %q: %s", nsString, err)
	}
	qm.mergeIntoNamespace(ns)
	return nil
}

func (qm *QuotaManager) parseFlagdBucketConfig(ctx context.Context, nsString string) (*bucketConfig, error) {
	flagdBucketsConfig := qm.env.GetExperimentFlagProvider().Object(ctx, bucketQuotaExperimentName, make(map[string]any))

	keySpecificFlagdNamespaceConfig, ok := flagdBucketsConfig[nsString]
	if !ok {
		return nil, nil
	}

	bucketMap, ok := keySpecificFlagdNamespaceConfig.(map[string]any)
	if !ok {
		return nil, status.InvalidArgumentErrorf("invalid quota.buckets config for namespace %q: expected object, got %T", nsString, keySpecificFlagdNamespaceConfig)
	}

	bucketConfig, err := bucketConfigFromMap(nsString, bucketMap)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse quota bucket config for namespace %q: %s", nsString, err)
	}

	return bucketConfig, nil
}

func (qm *QuotaManager) mergeIntoNamespace(ns *namespace) {
	existingInterface, loaded := qm.namespaces.LoadOrStore(ns.name, ns)
	if !loaded {
		return
	}

	existing := existingInterface.(*namespace)
	ns.bucketsByKey.Range(func(k, v any) bool {
		existing.bucketsByKey.Store(k, v)
		return true
	})
}

func (qm *QuotaManager) findBucket(nsName string, key string) Bucket {
	nsInterface, ok := qm.namespaces.Load(nsName)
	if !ok {
		return nil
	}

	ns := nsInterface.(*namespace)
	if b, ok := ns.bucketsByKey.Load(key); ok {
		return b.(Bucket)
	}

	return nil
}

func (qm *QuotaManager) reloadNamespaces() {
	qm.namespaces.Range(func(key, value any) bool {
		qm.namespaces.Delete(key)
		return true
	})
}

func (qm *QuotaManager) ReloadBucketsAndNotify(ctx context.Context) error {
	qm.reloadNamespaces()
	if sns := qm.env.GetServerNotificationService(); sns != nil {
		if err := sns.Publish(ctx, &snpb.ReloadQuotaBuckets{}); err != nil {
			return err
		}
	}
	return nil
}

func (qm *QuotaManager) listenForUpdates(ctx context.Context) {
	if fp := qm.env.GetExperimentFlagProvider(); fp != nil {
		flagdChanges := make(chan struct{}, 1)
		unsubscribe := fp.Subscribe(flagdChanges)
		go func() {
			defer unsubscribe()
			for range flagdChanges {
				qm.reloadNamespaces()
			}
		}()
	}

	if sns := qm.env.GetServerNotificationService(); sns != nil {
		go func() {
			for msg := range sns.Subscribe(&snpb.ReloadQuotaBuckets{}) {
				_, ok := msg.(*snpb.ReloadQuotaBuckets)
				if !ok {
					alert.UnexpectedEvent("reset_group_quota_buckets_invalid_proto_type", "received proto type %T", msg)
					continue
				}
				qm.reloadNamespaces()
			}
		}()
	}
}

func bucketConfigFromMap(namespace string, bucketMap map[string]any) (*bucketConfig, error) {
	maxRateInterface, ok := bucketMap["maxRate"]
	if !ok {
		return nil, status.InvalidArgumentError("bucket.maxRate is required")
	}
	maxRateMap, ok := maxRateInterface.(map[string]any)
	if !ok {
		return nil, status.InvalidArgumentError("bucket.maxRate must be an object")
	}

	numRequests, err := interfaceToInt64(maxRateMap["numRequests"])
	if err != nil {
		return nil, status.InvalidArgumentErrorf("bucket.maxRate.numRequests is invalid: %s", err)
	}

	period, err := interfaceToInt64(maxRateMap["periodUsec"])
	if err != nil {
		return nil, status.InvalidArgumentErrorf("bucket.maxRate.periodUsec is invalid: %s", err)
	}

	maxBurst, err := interfaceToInt64(bucketMap["maxBurst"])
	if err != nil {
		return nil, status.InvalidArgumentErrorf("bucket.maxBurst is invalid: %s", err)
	}

	name := fmt.Sprintf("flagd:%s:%d:%d:%d", namespace, numRequests, period, maxBurst)
	config := &bucketConfig{
		namespace:          namespace,
		name:               name,
		numRequests:        numRequests,
		periodDurationUsec: period,
		maxBurst:           maxBurst,
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func interfaceToInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case float64:
		return int64(val), nil
	default:
		return 0, status.InvalidArgumentErrorf("expected number type, got %T", v)
	}
}
