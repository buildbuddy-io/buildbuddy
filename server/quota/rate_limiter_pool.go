package quota

import (
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/quota/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/goredisstore.v8"

	qcpb "github.com/buildbuddy-io/buildbuddy/proto/quota_config"
)

const (
	maxRedisRetry = 3
)

type namespace struct {
	name string

	defaultRateLimiter *throttled.GCRARateLimiterCtx

	rateLimiters map[string]*throttled.GCRARateLimiterCtx

	config *qcpb.NamespaceConfig
}

type rateLimiterPool struct {
	config     *qcpb.ServiceConfig
	namespaces map[string]*namespace
}

func CreateRateLimiterPool(env environment.Env, config *qcpb.ServiceConfig) (*rateLimiterPool, error) {
	pool := &rateLimiterPool{
		config:     config,
		namespaces: make(map[string]*namespace),
	}

	for _, nsConfig := range config.GetNamespaces() {
		ns, err := createNamespace(env, nsConfig)
		if err != nil {
			return nil, err
		}
		pool.namespaces[nsConfig.GetName()] = ns
	}

	return pool, nil
}

func createNamespace(env environment.Env, nsConfig *qcpb.NamespaceConfig) (*namespace, error) {
	if len(nsConfig.GetName()) == 0 {
		return nil, status.InvalidArgumentError("namespace name is empty")
	}
	defaultRateLimiterConfig := nsConfig.GetDefaultDynamicBucketTemplate()
	if defaultRateLimiterConfig == nil {
		return nil, status.InvalidArgumentError("default_namespace dynamic_bucket_template is unset")
	}

	defaultRateLimiter, err := createRateLimiter(env, nsConfig.GetName(), defaultRateLimiterConfig)
	if err != nil {
		return nil, err
	}

	ns := &namespace{
		config:             nsConfig,
		defaultRateLimiter: defaultRateLimiter,
		rateLimiters:       make(map[string]*throttled.GCRARateLimiterCtx),
		// TODO: set rateLimiters
	}

	for _, bc := range nsConfig.GetOverriddenDynamicBucketTemplates() {
		rl, err := createRateLimiter(env, nsConfig.GetName(), bc)
		if err != nil {
			return nil, err
		}
		ns.rateLimiters[bc.GetName()] = rl
	}

	return ns, nil
}

func createRateLimiter(env environment.Env, namespace string, bc *qcpb.BucketConfig) (*throttled.GCRARateLimiterCtx, error) {
	err := config.ValidateBucketConfig(bc)
	if err != nil {
		return nil, err
	}
	prefix := strings.Join([]string{namespace, bc.GetName()}, ":")
	// TODO: set up a dedicated redis client for quota
	store, err := goredisstore.NewCtx(env.GetDefaultRedisClient(), prefix)
	if err != nil {
		return nil, status.InternalErrorf("unable to init redist store: %s", err)
	}

	rate := bc.GetMaxRate()
	quota := throttled.RateQuota{
		MaxRate:  throttled.PerDuration(int(rate.GetCount()), rate.GetPeriod().AsDuration()),
		MaxBurst: int(bc.GetMaxBurst()),
	}

	rateLimiter, err := throttled.NewGCRARateLimiterCtx(store, quota)
	rateLimiter.SetMaxCASAttemptsLimit(maxRedisRetry)
	if err != nil {
		return nil, status.InternalErrorf("unable to create GCRARateLimiter: %s", err)
	}
	return rateLimiter, nil
}
