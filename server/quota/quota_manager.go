package quota

import (
	"context"
	"flag"
	"math"
	"net"
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

	// THe prefix we use to all quota-related redis entries.
	redisQuotaKeyPrefix = "quota"
)

type namespaceConfig struct {
	name                  string
	bucketsByName         map[string]*tables.QuotaBucket
	quotaKeysByBucketName map[string][]string
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
	for _, fromBucket := range from.bucketsByName {
		assignedBucket := &qpb.AssignedBucket{
			Bucket:    bucketToProto(fromBucket),
			QuotaKeys: from.quotaKeysByBucketName[fromBucket.Name],
		}
		res.AssignedBuckets = append(res.GetAssignedBuckets(), assignedBucket)
	}
	return res
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
			ns := config[tb.Namespace]
			if ns == nil {
				ns = &namespaceConfig{
					name:                  tb.Namespace,
					bucketsByName:         make(map[string]*tables.QuotaBucket),
					quotaKeysByBucketName: make(map[string][]string),
				}
				config[tb.Namespace] = ns
			}
			if err := validateBucket(bucketToProto(&tb)); err != nil {
				return status.InternalErrorf("invalid bucket: %v", tb)
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

func validateBucket(bucket *qpb.Bucket) error {
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

func (qm *QuotaManager) GetNamespace(ctx context.Context, req *qpb.GetNamespaceRequest) (*qpb.GetNamespaceResponse, error) {
	configs, err := fetchConfigFromDB(qm.env)
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
	err := qm.env.GetDBHandle().TransactionWithOptions(ctx, db.Opts().WithQueryName("query_manager_insert_buckets"), func(tx *db.DB) error {
		ns := req.GetNamespace()
		if err := tx.Exec(`DELETE FROM QuotaGroups WHERE namespace = ?`, ns).Error; err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM QuotaBuckets WHERE namespace = ?`, ns).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
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
	err := dbh.TransactionWithOptions(ctx, db.Opts().WithQueryName("apply_bucket"), func(tx *db.DB) error {
		row := &struct{ Count int64 }{}
		err := tx.Raw(
			`SELECT COUNT(*) AS count FROM QuotaBuckets WHERE namespace = ? AND name = ?`, req.GetNamespace(), req.GetBucketName(),
		).Take(row).Error
		if err != nil {
			return err
		}
		if row.Count == 0 {
			return status.FailedPreconditionErrorf("namespace(%q) and bucket_name(%q) doesn't exist", req.GetNamespace(), req.GetBucketName())
		}

		quotaGroup := &tables.QuotaGroup{
			Namespace:  req.GetNamespace(),
			QuotaKey:   quotaKey,
			BucketName: req.GetBucketName(),
		}
		var existing tables.QuotaGroup
		if err := tx.Where("namespace = ? AND quota_key = ?", req.GetNamespace(), quotaKey).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				if req.GetBucketName() == defaultBucketName {
					return nil
				}
				return tx.Create(quotaGroup).Error
			}
			return err
		}
		if req.GetBucketName() == defaultBucketName {
			return tx.Exec(`DELETE FROM QuotaGroups WHERE namespace = ? AND quota_key = ?`, req.GetNamespace(), quotaKey).Error
		} else {
			return tx.Model(&existing).Where("namespace = ? AND quota_key = ?", req.GetNamespace(), quotaKey).Updates(quotaGroup).Error
		}
	})
	if err != nil {
		return nil, err
	}

	return &qpb.ApplyBucketResponse{}, nil
}

func (qm *QuotaManager) ModifyNamespace(ctx context.Context, req *qpb.ModifyNamespaceRequest) (*qpb.ModifyNamespaceResponse, error) {
	if req.GetNamespace() == "" {
		return nil, status.InvalidArgumentError("namespace cannot empty")
	}

	if req.GetAddBucket() != nil {
		return qm.addBucket(ctx, req.GetNamespace(), req.GetAddBucket())
	}

	if req.GetUpdateBucket() != nil {
		return qm.updateBucket(ctx, req.GetNamespace(), req.GetUpdateBucket())
	}

	if req.GetRemoveBucket() != "" {
		return qm.removeBucket(ctx, req.GetNamespace(), req.GetRemoveBucket())
	}

	return nil, status.InvalidArgumentError("one of add_bucket, update_bucket and remove_bucket should be set")
}

func (qm *QuotaManager) addBucket(ctx context.Context, namespace string, bucket *qpb.Bucket) (*qpb.ModifyNamespaceResponse, error) {
	if err := validateBucket(bucket); err != nil {
		return nil, status.InvalidArgumentErrorf("invalid add_bucket: %s", err)
	}
	row := bucketToRow(namespace, bucket)

	err := qm.env.GetDBHandle().DB(ctx).Create(&row).Error
	if err != nil {
		return nil, err
	}
	return &qpb.ModifyNamespaceResponse{}, nil
}

func (qm *QuotaManager) updateBucket(ctx context.Context, namespace string, bucket *qpb.Bucket) (*qpb.ModifyNamespaceResponse, error) {
	if err := validateBucket(bucket); err != nil {
		return nil, status.InvalidArgumentErrorf("invalid update_bucket: %s", err)
	}
	bucketRow := bucketToRow(namespace, bucket)

	db := qm.env.GetDBHandle().DB(ctx)
	res := db.Model(bucketRow).Where("namespace = ? AND name= ?", bucketRow.Namespace, bucketRow.Name).Updates(bucketRow)

	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, status.InvalidArgumentErrorf("bucket %s doesn't exist", bucket.GetName())
	}
	return &qpb.ModifyNamespaceResponse{}, nil
}

func (qm *QuotaManager) removeBucket(ctx context.Context, namespace string, bucketName string) (*qpb.ModifyNamespaceResponse, error) {
	dbh := qm.env.GetDBHandle()
	err := dbh.TransactionWithOptions(ctx, db.Opts().WithQueryName("remove_bucket"), func(tx *db.DB) error {
		if err := tx.Exec(`DELETE FROM QuotaGroups WHERE namespace = ? AND bucket_name = ?`, namespace, bucketName).Error; err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM QuotaBuckets WHERE namespace = ? AND name = ?`, namespace, bucketName).Error
	})
	if err != nil {
		return nil, err
	}
	return &qpb.ModifyNamespaceResponse{}, nil
}
