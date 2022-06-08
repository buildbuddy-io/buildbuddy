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
	"google.golang.org/grpc/metadata"
)

const (
	maxRedisRetry       = 3
	defaultBucketName   = "default"
	redisQuotaKeyPrefix = "quota"
)

type namespaceConfig struct {
	bucketsByName         map[string]*tables.QuotaBucket
	quotaKeysByBucketName map[string][]string
}

func fetchConfigFromDB(env environment.Env) (map[string]*namespaceConfig, error) {
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

	config := make(map[string]*namespaceConfig)
	for _, b := range buckets {
		if err := validateBucket(b); err != nil {
			return nil, status.InternalErrorf("bucket is invalid: %v", b)
		}
		ns := config[b.Namespace]
		if ns == nil {
			ns = &namespaceConfig{
				bucketsByName:         make(map[string]*tables.QuotaBucket),
				quotaKeysByBucketName: make(map[string][]string),
			}
			config[b.Namespace] = ns
		}
		ns.bucketsByName[b.Name] = b
	}

	for _, g := range quotaGroups {
		ns := config[g.Namespace]
		if ns == nil {
			return nil, status.InternalErrorf("namespace %q doesn't exist", g.Namespace)
		}
		if _, ok := ns.bucketsByName[g.BucketName]; !ok {
			return nil, status.InternalErrorf("namespace %q bucket name %q doesn't exist", g.Namespace, g.BucketName)
		}
		if g.BucketName == defaultBucketName {
			log.Warningf("doesn't need to create QuotaGroup for default bucket in namespace %q", g.Namespace)
		}
		if keys := ns.quotaKeysByBucketName[g.BucketName]; keys == nil {
			ns.quotaKeysByBucketName[g.BucketName] = []string{g.QuotaKey}
		} else {
			ns.quotaKeysByBucketName[g.BucketName] = append(keys, g.QuotaKey)
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
	prefix := strings.Join([]string{redisQuotaKeyPrefix, config.Namespace, config.Name, ""}, ":")
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
	name   string
	config *namespaceConfig

	defaultBucket Bucket
	bucketsByKey  map[string]Bucket
}

type bucketCreatorFn func(environment.Env, *tables.QuotaBucket) (Bucket, error)

type QuotaManager struct {
	env           environment.Env
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
		namespaces:    make(map[string]*namespace),
		bucketCreator: bucketCreator,
	}

	for nsName, nsConfig := range config {
		ns, err := qm.createNamespace(env, nsName, nsConfig)
		if err != nil {
			return nil, err
		}
		qm.namespaces[nsName] = ns
	}

	return qm, nil
}

func (qm *QuotaManager) createNamespace(env environment.Env, name string, config *namespaceConfig) (*namespace, error) {
	defaultBucketConfig := config.bucketsByName[defaultBucketName]
	if defaultBucketConfig == nil {
		return nil, status.InvalidArgumentErrorf("default quota bucket is unset in namespace: %q", name)
	}

	defaultBucket, err := qm.bucketCreator(env, defaultBucketConfig)
	if err != nil {
		return nil, err
	}

	ns := &namespace{
		name:          name,
		config:        config,
		defaultBucket: defaultBucket,
		bucketsByKey:  make(map[string]Bucket),
	}

	for _, bc := range config.bucketsByName {
		bucket, err := qm.bucketCreator(env, bc)
		if err != nil {
			return nil, err
		}

		keys := config.quotaKeysByBucketName[bc.Name]
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

func getIP(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := md.Get("X-Forwarded-For")

	if len(vals) == 0 {
		return ""
	}
	ips := strings.Split(vals[0], ",")
	return ips[0]
}

func (qm *QuotaManager) getKey(ctx context.Context) string {
	if groupID := qm.getGroupID(ctx); groupID != "" {
		return groupID
	}
	return getIP(ctx)
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
