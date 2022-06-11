package quota

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/goredisstore.v8"
)

const (
	maxRedisRetry       = 3
	defaultPrefix       = "default"
	redisQuotaKeyPrefix = "quota"
)

type config struct {
	bucketsByName       map[string]map[string]*tables.QuotaBucket
	bucketsByID         map[string]*tables.QuotaBucket
	quotaKeysByBucketID map[string][]string
}

func fetchConfigFromDB(env environment.Env) (*config, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("quota manager is not configured")
	}
	buckets := []*tables.QuotaBucket{}
	ctx := env.GetServerContext()
	result := env.GetDBHandle().DB(ctx).Find(&buckets)
	if result.Error != nil {
		return nil, status.InternalErrorf("failed to read quota config: %s", result.Error)
	}
	quotaGroups := []*tables.QuotaGroup{}
	result = env.GetDBHandle().DB(ctx).Find(&quotaGroups)
	if result.Error != nil {
		return nil, status.InternalErrorf("failed to read quota config: %s", result.Error)
	}

	config := &config{
		bucketsByID:         make(map[string]*tables.QuotaBucket),
		bucketsByName:       make(map[string]map[string]*tables.QuotaBucket),
		quotaKeysByBucketID: make(map[string][]string),
	}
	for _, b := range buckets {
		if err := validateBucket(b); err != nil {
			return nil, status.InternalErrorf("bucket is invalid: %v", b)
		}
		config.bucketsByID[b.QuotaBucketID] = b
		if config.bucketsByName[b.Namespace] == nil {
			config.bucketsByName[b.Namespace] = make(map[string]*tables.QuotaBucket)
		}
		config.bucketsByName[b.Namespace][b.Prefix] = b
	}
	for _, g := range quotaGroups {
		if keys := config.quotaKeysByBucketID[g.QuotaBucketID]; keys == nil {
			config.quotaKeysByBucketID[g.QuotaBucketID] = []string{g.QuotaKey}
		} else {
			config.quotaKeysByBucketID[g.QuotaBucketID] = append(keys, g.QuotaKey)
		}
	}
	return config, nil
}

func validateBucket(bucket *tables.QuotaBucket) error {
	if num := bucket.NumRequests; num <= 0 || num > math.MaxInt {
		return status.InvalidArgumentErrorf("bucket.NumRequests(%d) must be positive and less than %d", num, math.MaxInt)
	}

	if bucket.PeriodDurationUsec == 0 {
		return status.InvalidArgumentError("max_rate.period is zero")
	}

	if burst := bucket.MaxBurst; burst < 0 || burst > math.MaxInt {
		return status.InvalidArgumentErrorf("max_burst(%d) must be non-negative and less than %d", burst, math.MaxInt)
	}

	return nil
}

type namespace struct {
	name      string
	prefixMap map[string]*tables.QuotaBucket

	defaultRateLimiter *throttled.GCRARateLimiterCtx

	rateLimitersByKey map[string]*throttled.GCRARateLimiterCtx
}

type QuotaManager struct {
	env        environment.Env
	config     *config
	namespaces map[string]*namespace
}

func NewQuotaManager(env environment.Env) (*QuotaManager, error) {
	config, err := fetchConfigFromDB(env)
	if err != nil {
		return nil, err
	}
	qm := &QuotaManager{
		env:        env,
		config:     config,
		namespaces: make(map[string]*namespace),
	}

	for namespaceName, prefixMap := range config.bucketsByName {
		ns, err := createNamespace(env, namespaceName, prefixMap, config.quotaKeysByBucketID)
		if err != nil {
			return nil, err
		}
		qm.namespaces[namespaceName] = ns
	}

	return qm, nil
}

func createNamespace(env environment.Env, name string, prefixMap map[string]*tables.QuotaBucket, quotaKeysByBucketID map[string][]string) (*namespace, error) {
	defaultBucket := prefixMap[defaultPrefix]
	if defaultBucket == nil {
		return nil, status.InvalidArgumentErrorf("default quota bucket is unset in namespace: %q", name)
	}

	defaultRateLimiter, err := createRateLimiter(env, defaultBucket)
	if err != nil {
		return nil, err
	}

	ns := &namespace{
		name:               name,
		prefixMap:          prefixMap,
		defaultRateLimiter: defaultRateLimiter,
		rateLimitersByKey:  make(map[string]*throttled.GCRARateLimiterCtx),
	}

	for _, b := range prefixMap {
		rl, err := createRateLimiter(env, b)
		if err != nil {
			return nil, err
		}

		keys := quotaKeysByBucketID[b.QuotaBucketID]
		for _, key := range keys {
			ns.rateLimitersByKey[key] = rl
		}
	}
	return ns, nil
}

func createRateLimiter(env environment.Env, bucket *tables.QuotaBucket) (*throttled.GCRARateLimiterCtx, error) {
	prefix := strings.Join([]string{redisQuotaKeyPrefix, bucket.Namespace, bucket.Prefix, ""}, ":")
	// TODO: set up a dedicated redis client for quota
	store, err := goredisstore.NewCtx(env.GetDefaultRedisClient(), prefix)
	if err != nil {
		return nil, status.InternalErrorf("unable to init redis store: %s", err)
	}

	period := time.Duration(bucket.PeriodDurationUsec)
	quota := throttled.RateQuota{
		MaxRate:  throttled.PerDuration(int(bucket.NumRequests), period),
		MaxBurst: int(bucket.MaxBurst),
	}

	rateLimiter, err := throttled.NewGCRARateLimiterCtx(store, quota)
	rateLimiter.SetMaxCASAttemptsLimit(maxRedisRetry)
	if err != nil {
		return nil, status.InternalErrorf("unable to create GCRARateLimiter: %s", err)
	}
	return rateLimiter, nil
}

// findRateLimiter finds the rate limiter given a namespace and key. If the key is found in
// rateLimitersByKey map, return the corresponding rateLimiter. Otherwise, return the default rate
// limiter. Return nil if the namespace is not found.
func (qm *QuotaManager) findRateLimiter(namespace string, key string) *throttled.GCRARateLimiterCtx {
	ns, ok := qm.namespaces[namespace]
	if !ok {
		log.Warningf("namespace %q not found", namespace)
		return nil
	}

	if rl, ok := ns.rateLimitersByKey[key]; ok {
		return rl
	}

	return ns.defaultRateLimiter
}

func (qm *QuotaManager) getGroupID(ctx context.Context) string {
	if a := qm.env.GetAuthenticator(); a != nil {
		user, err := a.AuthenticatedUser(ctx)
		if err != nil {
			return interfaces.AuthAnonymousUser
		}
		return user.GetGroupID()
	}
	return ""
}

func (qm *QuotaManager) getKey(ctx context.Context) string {
	groupID := qm.getGroupID(ctx)
	if groupID != "" {
		return groupID
	}
	//TODO(lulu): find IP address
	return ""
}

func (qm *QuotaManager) Allow(ctx context.Context, namespace string, quantity int64) (bool, error) {
	key := qm.getKey(ctx)
	rl := qm.findRateLimiter(namespace, key)
	if rl == nil {
		log.Warningf("rate limiter for namespace %q and key %q not found", namespace, key)
		return true, nil
	}
	if quantity > math.MaxInt {
		return false, status.InternalErrorf("quantity (%d) exceeds the limit", quantity)
	}
	limitExceeded, _, err := rl.RateLimit(key, int(quantity))
	if err != nil {
		return false, status.InternalErrorf("rate limiter failed %s", err)
	}
	return !limitExceeded, err
}

func Register(env environment.Env) error {
	qm, err := NewQuotaManager(env)
	if err != nil {
		return err
	}
	env.SetQuotaManager(qm)
	return nil
}
