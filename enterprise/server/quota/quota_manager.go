package quota

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/quota"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/goredisstore.v8"
	"google.golang.org/protobuf/types/known/durationpb"

	qpb "github.com/buildbuddy-io/buildbuddy/proto/quota"
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

	// The prefix we use for all quota-related redis entries.
	redisQuotaKeyPrefix = "quota"

	// The channel name where quota manager publishes and subscribes the messages
	// when there is an update.
	pubSubChannelName = "quota-change-notifications"

	namespaceSeperator = ":"
)

type assignedBucket struct {
	bucket    *tables.QuotaBucket
	quotaKeys []string
}

type namespaceConfig struct {
	name            string
	assignedBuckets map[string]*assignedBucket
}

func bucketToProto(from *tables.QuotaBucket) *qpb.Bucket {
	res := &qpb.Bucket{
		Name: from.Name,
		MaxRate: &qpb.Rate{
			NumRequests: from.NumRequests,
			Period:      durationpb.New(time.Duration(from.PeriodDurationUsec) * time.Microsecond),
		},
		MaxBurst: from.MaxBurst,
	}
	return res
}

func bucketToRow(namespace string, from *qpb.Bucket) *tables.QuotaBucket {
	res := &tables.QuotaBucket{
		Namespace:          namespace,
		Name:               from.GetName(),
		NumRequests:        from.GetMaxRate().GetNumRequests(),
		PeriodDurationUsec: int64(from.GetMaxRate().GetPeriod().AsDuration() / time.Microsecond),
		MaxBurst:           from.GetMaxBurst(),
	}
	return res
}

func namespaceConfigToProto(from *namespaceConfig) *qpb.Namespace {
	res := &qpb.Namespace{
		Name: from.name,
	}
	for _, fromAssignedBucket := range from.assignedBuckets {
		assignedBucket := &qpb.AssignedBucket{
			Bucket:    bucketToProto(fromAssignedBucket.bucket),
			QuotaKeys: fromAssignedBucket.quotaKeys,
		}
		res.AssignedBuckets = append(res.GetAssignedBuckets(), assignedBucket)
	}
	return res
}

func fetchAllConfigFromDB(env environment.Env) (map[string]*namespaceConfig, error) {
	return fetchConfigFromDB(env, "")
}

func fetchConfigFromDB(env environment.Env, namespace string) (map[string]*namespaceConfig, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("quota manager is not configured")
	}
	ctx := env.GetServerContext()
	config := make(map[string]*namespaceConfig)
	var rq interfaces.DBRawQuery
	if namespace == "" {
		rq = env.GetDBHandle().NewQuery(ctx, "quota_manager_fetch_all_configs_from_db").Raw(`
			SELECT qb.*, qg.*
			FROM "QuotaBuckets" AS qb
				LEFT JOIN "QuotaGroups" AS qg
					ON qb.namespace = qg.namespace
					AND qb.name = qg.bucket_name
			ORDER BY
				qb.namespace,
				qb.name ASC,
				qg.quota_key ASC`,
		)
	} else {
		rq = env.GetDBHandle().NewQuery(ctx, "quota_manager_fetch_config_from_db").Raw(`
			SELECT qb.*, qg.*
			FROM "QuotaBuckets" AS qb
				LEFT JOIN "QuotaGroups" AS qg
					ON qb.namespace = qg.namespace
					AND qb.name = qg.bucket_name
			WHERE qb.namespace = ?
			ORDER BY
				qb.name ASC,
				qg.quota_key ASC`,
			namespace,
		)
	}
	err := db.ScanEach(
		rq,
		func(ctx context.Context, qbg *struct {
			tables.QuotaBucket
			*tables.QuotaGroup
		}) error {
			qb := &qbg.QuotaBucket
			ns, ok := config[qb.Namespace]
			if !ok {
				ns = &namespaceConfig{
					name:            qb.Namespace,
					assignedBuckets: make(map[string]*assignedBucket),
				}
				config[qb.Namespace] = ns
			}
			bucket, ok := ns.assignedBuckets[qb.Name]
			if !ok {
				if err := validateBucket(bucketToProto(qb)); err != nil {
					return status.InternalErrorf("invalid bucket: %v", qbg.QuotaBucket)
				}
				bucket = &assignedBucket{
					bucket: &qbg.QuotaBucket,
				}
				ns.assignedBuckets[qb.Name] = bucket
			}

			qg := qbg.QuotaGroup
			if qg == nil {
				// No Quota Group for this bucket
				return nil
			}
			if qg.BucketName == defaultBucketName {
				log.Warningf("Doesn't need to create QuotaGroup for default bucket in namespace %q", qg.Namespace)
				return nil
			}
			bucket.quotaKeys = append(bucket.quotaKeys, qg.QuotaKey)
			return nil
		},
	)
	if err != nil {
		return nil, status.InternalErrorf("fetchConfigFromDB query failed: %s", err)
	}
	return config, nil
}

func validateBucket(bucket *qpb.Bucket) error {
	if bucket.GetName() == "" {
		return status.InvalidArgumentError("bucket.name cannot be empty")
	}
	if num := bucket.GetMaxRate().GetNumRequests(); num <= 0 || num > math.MaxInt {
		return status.InvalidArgumentErrorf("bucket.max_rate.num_requests(%d) must be positive and less than %d", num, math.MaxInt)
	}

	if bucket.GetMaxRate().GetPeriod().AsDuration() == 0 {
		return status.InvalidArgumentError("bucket.max_rate.period is zero")
	}

	if burst := bucket.MaxBurst; burst < 0 || burst > math.MaxInt {
		return status.InvalidArgumentErrorf("bucket.max_burst(%d) must be non-negative and less than %d", burst, math.MaxInt)
	}

	return nil
}

type Bucket interface {
	// Config returns a copy of the QuotaBucket. Used for testing.
	Config() tables.QuotaBucket
	Allow(ctx context.Context, key string, quantity int64) (bool, error)
}

type gcraBucket struct {
	config      *tables.QuotaBucket
	rateLimiter *throttled.GCRARateLimiterCtx
}

func (b *gcraBucket) Config() tables.QuotaBucket {
	return *b.config
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
	namespaces    sync.Map // map of string namespace name -> *namespace
	bucketCreator bucketCreatorFn
	ps            interfaces.PubSub
	// Streams an event after each successful reload.
	// For testing only.
	reloaded chan struct{}
}

func NewQuotaManager(env environment.Env, ps interfaces.PubSub) (*QuotaManager, error) {
	return newQuotaManager(env, ps, createGCRABucket)
}

func newQuotaManager(env environment.Env, ps interfaces.PubSub, bucketCreator bucketCreatorFn) (*QuotaManager, error) {
	qm := &QuotaManager{
		env:           env,
		namespaces:    sync.Map{},
		bucketCreator: bucketCreator,
		ps:            ps,
		reloaded:      make(chan struct{}, 1),
	}
	err := qm.reloadNamespaces()
	if err != nil {
		return nil, err
	}

	qm.listenForUpdates(env.GetServerContext())

	return qm, nil
}

func (qm *QuotaManager) createNamespace(env environment.Env, name string, config *namespaceConfig) (*namespace, error) {
	ns := &namespace{
		name:         name,
		config:       config,
		bucketsByKey: make(map[string]Bucket),
	}
	defaultAssignedBucket := config.assignedBuckets[defaultBucketName]
	if defaultAssignedBucket != nil {
		defaultBucket, err := qm.bucketCreator(env, defaultAssignedBucket.bucket)
		if err != nil {
			return nil, err
		}
		ns.defaultBucket = defaultBucket
	}

	for _, assignedBucket := range config.assignedBuckets {
		bucket, err := qm.bucketCreator(env, assignedBucket.bucket)
		if err != nil {
			return nil, err
		}

		for _, key := range assignedBucket.quotaKeys {
			ns.bucketsByKey[key] = bucket
		}
	}
	return ns, nil
}

// findBucket finds the bucket given a namespace and key. If the key is found in
// bucketsByKey map, return the corresponding bucket. Otherwise, return the
// default bucket. Returns nil if the namespace is not found or the default bucket
// is not defined.
func (qm *QuotaManager) findBucket(nsName string, key string) Bucket {
	nsInterface, ok := qm.namespaces.Load(nsName)
	if !ok {
		return nil
	}
	ns := nsInterface.(*namespace)

	if b, ok := ns.bucketsByKey[key]; ok {
		return b
	}

	return ns.defaultBucket
}

func (qm *QuotaManager) Allow(ctx context.Context, namespace string, quantity int64) error {
	key, err := quota.GetKey(ctx, qm.env)

	if err != nil {
		metrics.QuotaKeyEmptyCount.With(prometheus.Labels{
			metrics.QuotaNamespace: namespace,
		}).Inc()
		return nil
	}
	b := qm.findBucket(namespace, key)
	if b == nil {
		// The bucket is not found, b/c either the namespace or the default bucket
		// is not defined.
		return nil
	}
	allow, err := b.Allow(ctx, key, quantity)
	if err != nil {
		log.CtxWarningf(ctx, "Quota check for %q failed: %s", namespace, err)
		// There is some error when determining whether the request should be
		// allowed. Do not block the traffic when the quota system has issues.
		return nil
	}
	if allow {
		return nil
	} else {
		metrics.QuotaExceeded.With(prometheus.Labels{
			metrics.QuotaNamespace: namespace,
			metrics.QuotaKey:       key,
		}).Inc()
		return status.ResourceExhaustedErrorf("quota exceeded for %q", namespace)
	}
}

func Register(env *real_environment.RealEnv) error {
	if !*quotaManagerEnabled {
		return nil
	}
	ps := pubsub.NewPubSub(env.GetDefaultRedisClient())
	qm, err := NewQuotaManager(env, ps)
	if err != nil {
		return err
	}
	env.SetQuotaManager(qm)
	return nil
}

func (qm *QuotaManager) GetNamespace(ctx context.Context, req *qpb.GetNamespaceRequest) (*qpb.GetNamespaceResponse, error) {
	configs, err := fetchConfigFromDB(qm.env, req.GetNamespace())
	if err != nil {
		return nil, err
	}
	res := &qpb.GetNamespaceResponse{}
	for _, c := range configs {
		res.Namespaces = append(res.GetNamespaces(), namespaceConfigToProto(c))
	}
	return res, nil
}

func (qm *QuotaManager) RemoveNamespace(ctx context.Context, req *qpb.RemoveNamespaceRequest) (*qpb.RemoveNamespaceResponse, error) {
	if qm.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	err := qm.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		ns := req.GetNamespace()
		if err := tx.NewQuery(ctx, "quota_manager_delete_namespace_groups").Raw(
			`DELETE FROM "QuotaGroups" WHERE namespace = ?`, ns).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "quota_manager_delete_namespace_buckets").Raw(
			`DELETE FROM "QuotaBuckets" WHERE namespace = ?`, ns).Exec().Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	qm.notifyListeners()
	return &qpb.RemoveNamespaceResponse{}, nil
}

func (qm *QuotaManager) ApplyBucket(ctx context.Context, req *qpb.ApplyBucketRequest) (*qpb.ApplyBucketResponse, error) {
	if qm.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if req.GetNamespace() == "" || req.GetBucketName() == "" {
		return nil, status.FailedPreconditionError("namespace and bucket_name cannot be empty")
	}

	quotaKey := ""
	if groupID := req.GetKey().GetGroupId(); groupID != "" {
		_, err := qm.env.GetUserDB().GetGroupByID(ctx, groupID)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid group id: %s", err)
		} else {
			quotaKey = groupID
		}
	} else if ipStr := req.GetKey().GetIpAddress(); ipStr != "" {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return nil, status.InvalidArgumentErrorf("invalid IP address: %s", ipStr)
		} else {
			quotaKey = ipStr
		}
	} else {
		return nil, status.InvalidArgumentError("quota key is empty")
	}

	dbh := qm.env.GetDBHandle()
	err := dbh.Transaction(ctx, func(tx interfaces.DB) error {
		// apply_bucket
		row := &struct{ Count int64 }{}
		err := tx.NewQuery(ctx, "quota_manager_get_num_buckets").Raw(
			`SELECT COUNT(*) AS count FROM "QuotaBuckets" WHERE namespace = ? AND name = ?`, req.GetNamespace(), req.GetBucketName(),
		).Take(row)
		if err != nil {
			return err
		}
		if row.Count == 0 && req.GetBucketName() != defaultBucketName {
			return status.FailedPreconditionErrorf("namespace(%q) and bucket_name(%q) doesn't exist", req.GetNamespace(), req.GetBucketName())
		}

		quotaGroup := &tables.QuotaGroup{
			Namespace:  req.GetNamespace(),
			QuotaKey:   quotaKey,
			BucketName: req.GetBucketName(),
		}
		var existing tables.QuotaGroup
		if err := tx.GORM(ctx, "quota_manager_get_existing_quota_group").Where(
			"namespace = ? AND quota_key = ?", req.GetNamespace(), quotaKey).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				if req.GetBucketName() == defaultBucketName {
					return nil
				}
				return tx.NewQuery(ctx, "quota_manager_create_quota_group").Create(quotaGroup)
			}
			return err
		}
		if req.GetBucketName() == defaultBucketName {
			return tx.NewQuery(ctx, "quota_manager_apply_bucket_delete_key").Raw(
				`DELETE FROM "QuotaGroups" WHERE namespace = ? AND quota_key = ?`, req.GetNamespace(), quotaKey).Exec().Error
		} else {
			return tx.GORM(ctx, "quota_manager_apply_bucket_update_group").Model(&existing).Where(
				"namespace = ? AND quota_key = ?", req.GetNamespace(), quotaKey).Updates(quotaGroup).Error
		}
	})
	if err != nil {
		return nil, err
	}

	qm.notifyListeners()

	return &qpb.ApplyBucketResponse{}, nil
}

func (qm *QuotaManager) ModifyNamespace(ctx context.Context, req *qpb.ModifyNamespaceRequest) (*qpb.ModifyNamespaceResponse, error) {
	if req.GetNamespace() == "" {
		return nil, status.InvalidArgumentError("namespace cannot be empty")
	}

	if req.GetAddBucket() == nil && req.GetUpdateBucket() == nil && req.GetRemoveBucket() == "" {
		return nil, status.InvalidArgumentError("one of add_bucket, update_bucket and remove_bucket should be set")
	}

	if req.GetAddBucket() != nil {
		if err := qm.addBucket(ctx, req.GetNamespace(), req.GetAddBucket()); err != nil {
			return nil, err
		}
	}

	if req.GetUpdateBucket() != nil {
		if err := qm.updateBucket(ctx, req.GetNamespace(), req.GetUpdateBucket()); err != nil {
			return nil, err
		}
	}

	if req.GetRemoveBucket() != "" {
		if err := qm.removeBucket(ctx, req.GetNamespace(), req.GetRemoveBucket()); err != nil {
			return nil, err
		}
	}

	qm.notifyListeners()

	return &qpb.ModifyNamespaceResponse{}, nil
}

func (qm *QuotaManager) addBucket(ctx context.Context, namespace string, bucket *qpb.Bucket) error {
	if err := validateBucket(bucket); err != nil {
		return status.InvalidArgumentErrorf("invalid add_bucket: %s", err)
	}
	row := bucketToRow(namespace, bucket)

	return qm.env.GetDBHandle().NewQuery(ctx, "quota_manager_add_bucket").Create(&row)
}

func (qm *QuotaManager) updateBucket(ctx context.Context, namespace string, bucket *qpb.Bucket) error {
	if err := validateBucket(bucket); err != nil {
		return status.InvalidArgumentErrorf("invalid update_bucket: %s", err)
	}
	bucketRow := bucketToRow(namespace, bucket)

	err := qm.env.GetDBHandle().NewQuery(ctx, "quota_manager_update_bucket").Update(bucketRow)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return status.InvalidArgumentErrorf("bucket %q doesn't exist", bucket.GetName())
		}
		return err
	}
	return nil
}

func (qm *QuotaManager) removeBucket(ctx context.Context, namespace string, bucketName string) error {
	dbh := qm.env.GetDBHandle()
	return dbh.Transaction(ctx, func(tx interfaces.DB) error {
		if err := tx.NewQuery(ctx, "quota_manager_delete_group").Raw(
			`DELETE FROM "QuotaGroups" WHERE namespace = ? AND bucket_name = ?`, namespace, bucketName).Exec().Error; err != nil {
			return err
		}
		return tx.NewQuery(ctx, "quota_manager_delete_bucket").Raw(
			`DELETE FROM "QuotaBuckets" WHERE namespace = ? AND name = ?`, namespace, bucketName).Exec().Error
	})
}

func (qm *QuotaManager) reloadNamespaces() error {
	config, err := fetchAllConfigFromDB(qm.env)
	if err != nil {
		return err
	}
	for nsName, nsConfig := range config {
		ns, err := qm.createNamespace(qm.env, nsName, nsConfig)
		if err != nil {
			return err

		}
		qm.namespaces.Store(nsName, ns)
	}
	qm.namespaces.Range(func(k, v interface{}) bool {
		nsName := k.(string)
		if _, ok := config[nsName]; !ok {
			qm.namespaces.Delete(nsName)
		}
		return true
	})
	log.Info("quota manager reloaded namespaces")
	return nil
}

// listenForUpdates sets up a subscription to quota bucket updates, and then
// starts a background goroutine to reload namespaces on each update.
func (qm *QuotaManager) listenForUpdates(ctx context.Context) {
	subscriber := qm.ps.Subscribe(ctx, pubSubChannelName)
	go func() {
		defer subscriber.Close()
		for range subscriber.Chan() {
			err := qm.reloadNamespaces()
			if err != nil {
				alert.UnexpectedEvent("quota-cannot-reload", " quota manager failed to reload configs: %s", err)
				continue
			}
			select {
			case qm.reloaded <- struct{}{}:
			default:
			}
		}
	}()
}

func (qm *QuotaManager) notifyListeners() {
	err := qm.ps.Publish(qm.env.GetServerContext(), pubSubChannelName, fmt.Sprintf("updated-%d", time.Now().UnixNano()))
	if err != nil {
		alert.UnexpectedEvent("quota-cannot-notify", "quota manager failed to publish: %s", err)
	}
}
