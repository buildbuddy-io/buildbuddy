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

type Bucket interface {
	Config() *tables.QuotaBucket
	Allow(ctx context.Context, key string, quantity int64) (bool, error)
}

type gcraBucket struct {
	config      *tables.QuotaBucket
	rateLimiter *throttled.GCRARateLimiterCtx
}

func (b *gcraBucket) Config() *tables.QuotaBucket {
	return b.config
}

func (b *gcraBucket) Allow(ctx context.Context, key string, quantity int64) (bool, error) {
	if quantity > math.MaxInt {
		return false, status.InternalErrorf("quantity (%d) exceeds the limit", quantity)
	}
	limitExceeded, _, err := b.rateLimiter.RateLimitCtx(ctx, key, int(quantity))
	return !limitExceeded, err
}

func createGCRABucket(env environment.Env, config *tables.QuotaBucket) (Bucket, error) {
	prefix := strings.Join([]string{redisQuotaKeyPrefix, config.Namespace, config.Prefix, ""}, ":")
	// TODO: set up a dedicated redis client for quota
	store, err := goredisstore.NewCtx(env.GetDefaultRedisClient(), prefix)
	if err != nil {
		return nil, status.InternalErrorf("unable to init redis store: %s", err)
	}

	period := time.Duration(config.PeriodDurationUsec) * time.Microsecond
	quota := throttled.RateQuota{
		MaxRate:  throttled.PerDuration(int(config.NumRequests), period),
		MaxBurst: int(config.MaxBurst),
	}

	rateLimiter, err := throttled.NewGCRARateLimiterCtx(store, quota)
	rateLimiter.SetMaxCASAttemptsLimit(maxRedisRetry)
	if err != nil {
		return nil, status.InternalErrorf("unable to create GCRARateLimiter: %s", err)
	}

	bucket := &gcraBucket{
		config:      config,
		rateLimiter: rateLimiter,
	}

	return bucket, nil
}

type namespace struct {
	name           string
	configByPrefix map[string]*tables.QuotaBucket

	defaultBucket Bucket
	bucketsByKey  map[string]Bucket
}

type bucketCreatorFn func(environment.Env, *tables.QuotaBucket) (Bucket, error)

type QuotaManager struct {
	env           environment.Env
	config        *config
	namespaces    map[string]*namespace
	bucketCreator bucketCreatorFn
}

func NewQuotaManager(env environment.Env) (*QuotaManager, error) {
	return newQuotaManager(env, createGCRABucket)
}

func newQuotaManager(env environment.Env, bucketCreator bucketCreatorFn) (*QuotaManager, error) {
	config, err := fetchConfigFromDB(env)
	if err != nil {
		return nil, err
	}
	qm := &QuotaManager{
		env:           env,
		config:        config,
		namespaces:    make(map[string]*namespace),
		bucketCreator: bucketCreator,
	}

	for namespaceName, prefixMap := range config.bucketsByName {
		ns, err := qm.createNamespace(env, namespaceName, prefixMap, config.quotaKeysByBucketID)
		if err != nil {
			return nil, err
		}
		qm.namespaces[namespaceName] = ns
	}

	return qm, nil
}

func (qm *QuotaManager) createNamespace(env environment.Env, name string, prefixMap map[string]*tables.QuotaBucket, quotaKeysByBucketID map[string][]string) (*namespace, error) {
	defaultBucketConfig := prefixMap[defaultPrefix]
	if defaultBucketConfig == nil {
		return nil, status.InvalidArgumentErrorf("default quota bucket is unset in namespace: %q", name)
	}

	defaultBucket, err := qm.bucketCreator(env, defaultBucketConfig)
	if err != nil {
		return nil, err
	}

	ns := &namespace{
		name:           name,
		configByPrefix: prefixMap,
		defaultBucket:  defaultBucket,
		bucketsByKey:   make(map[string]Bucket),
	}

	for _, bc := range prefixMap {
		bucket, err := qm.bucketCreator(env, bc)
		if err != nil {
			return nil, err
		}

		keys := quotaKeysByBucketID[bc.QuotaBucketID]
		for _, key := range keys {
			ns.bucketsByKey[key] = bucket
		}
	}
	return ns, nil
}

// findBucket finds the bucket given a namespace and key. If the key is found in
// bucketsByKey map, return the corresponding bucket. Otherwise, return the default bucket. Return
// nil if the namespace is not found.
func (qm *QuotaManager) findBucket(namespace string, key string) Bucket {
	ns, ok := qm.namespaces[namespace]
	if !ok {
		log.Warningf("namespace %q not found", namespace)
		return nil
	}

	if b, ok := ns.bucketsByKey[key]; ok {
		return b
	}

	return ns.defaultBucket
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
	b := qm.findBucket(namespace, key)
	if b == nil {
		log.Warningf("quota bucket for namespace %q and key %q not found", namespace, key)
		return true, nil
	}
	return b.Allow(ctx, key, quantity)
}

func Register(env environment.Env) error {
	qm, err := NewQuotaManager(env)
	if err != nil {
		return err
	}
	env.SetQuotaManager(qm)
	return nil
}
