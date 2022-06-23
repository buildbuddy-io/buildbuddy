package quota

import (
	"context"
	"flag"
	"math"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/goredisstore.v8"
	"google.golang.org/grpc/metadata"
)

var (
	quotaManagerEnabled = flag.Bool("app.enable_quota_management", false, "If set, quota management will be enabled")
)

const (
	// The maximum number of attempts to update rate limit data in redis.
	maxRedisRetry = 3

	// The name of the default bucket in the namespace. If you change this field,
	// you also need to update the QuotaBucket and QuotaGroup table.
	defaultBucketName = "default"

	// THe prefix we use to all quota-related redis entries.
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
	ctx := env.GetServerContext()
	config := make(map[string]*namespaceConfig)
	err := env.GetDBHandle().TransactionWithOptions(ctx, db.Opts().WithQueryName("fetch_quota_config"), func(tx *db.DB) error {
		bucketRows, err := tx.Raw(`SELECT * FROM QuotaBuckets`).Rows()
		if err != nil {
			return status.InternalErrorf("failed to read table QuotaBuckets: %s", err)
		}
		defer bucketRows.Close()

		for bucketRows.Next() {
			var tb tables.QuotaBucket
			if err := tx.ScanRows(bucketRows, &tb); err != nil {
				return status.InternalErrorf("failed to scan QuotaBuckets: %s", err)
			}
			if err = validateBucket(&tb); err != nil {
				return status.InternalErrorf("invalid bucket: %v", tb)
			}
			ns := config[tb.Namespace]
			if ns == nil {
				ns = &namespaceConfig{
					bucketsByName:         make(map[string]*tables.QuotaBucket),
					quotaKeysByBucketName: make(map[string][]string),
				}
				config[tb.Namespace] = ns
			}
			ns.bucketsByName[tb.Name] = &tb
		}

		groupRows, err := tx.Raw(`SELECT * FROM QuotaGroups`).Rows()
		if err != nil {
			return status.InternalErrorf("failed to read table QuotaGroups: %s", err)
		}
		defer groupRows.Close()

		for groupRows.Next() {
			var tg tables.QuotaGroup
			if err := tx.ScanRows(groupRows, &tg); err != nil {
				return status.InternalErrorf("failed to scan QuotaGroups: %s", err)
			}
			ns := config[tg.Namespace]
			if ns == nil {
				alert.UnexpectedEvent("invalid_quota_config", "namespace %q doesn't exist", tg.Namespace)
				continue
			}
			if _, ok := ns.bucketsByName[tg.BucketName]; !ok {
				alert.UnexpectedEvent("invalid_quota_config", "namespace %q bucket name %q doesn't exist", tg.Namespace, tg.BucketName)
				continue
			}
			if tg.BucketName == defaultBucketName {
				log.Warningf("Doesn't need to create QuotaGroup for default bucket in namespace %q", tg.Namespace)
				continue
			}
			if keys := ns.quotaKeysByBucketName[tg.BucketName]; keys == nil {
				ns.quotaKeysByBucketName[tg.BucketName] = []string{tg.QuotaKey}
			} else {
				ns.quotaKeysByBucketName[tg.BucketName] = append(keys, tg.QuotaKey)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return config, nil
}

func validateBucket(bucket *tables.QuotaBucket) error {
	if num := bucket.NumRequests; num <= 0 || num > math.MaxInt {
		return status.InvalidArgumentErrorf("bucket.NumRequests(%d) must be positive and less than %d", num, math.MaxInt)
	}

	if bucket.PeriodDurationUsec == 0 {
		return status.InvalidArgumentError("bucket.PeriodDurationUsec is zero")
	}

	if burst := bucket.MaxBurst; burst < 0 || burst > math.MaxInt {
		return status.InvalidArgumentErrorf("bucket.MaxBurst(%d) must be non-negative and less than %d", burst, math.MaxInt)
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
// bucketsByKey map, return the corresponding bucket. Otherwise, return the
// default bucket. Returns nil if the namespace is not found.
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
	if key == "" {
		log.Warningf("Key is empty.")
		return true, nil
	}
	b := qm.findBucket(namespace, key)
	if b == nil {
		log.Warningf("Quota bucket for namespace %q and key %q not found", namespace, key)
		return true, nil
	}
	return b.Allow(ctx, key, quantity)
}

func Register(env environment.Env) error {
	if !*quotaManagerEnabled {
		return nil
	}
	qm, err := NewQuotaManager(env)
	if err != nil {
		return err
	}
	env.SetQuotaManager(qm)
	return nil
}
