package quota

import (
	"context"
	"math"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/quota/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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

	rateLimitersByKey map[string]*throttled.GCRARateLimiterCtx

	config *qcpb.NamespaceConfig
}

type QuotaManager struct {
	env        environment.Env
	config     *qcpb.ServiceConfig
	namespaces map[string]*namespace
}

func NewQuotaManager(env environment.Env, config *qcpb.ServiceConfig) (*QuotaManager, error) {
	qm := &QuotaManager{
		env:        env,
		config:     config,
		namespaces: make(map[string]*namespace),
	}

	for _, nsConfig := range config.GetNamespaces() {
		ns, err := createNamespace(env, nsConfig)
		if err != nil {
			return nil, err
		}
		qm.namespaces[nsConfig.GetName()] = ns
	}

	return qm, nil
}

func createNamespace(env environment.Env, nsConfig *qcpb.NamespaceConfig) (*namespace, error) {
	if nsConfig.GetName() == "" {
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
		rateLimitersByKey:  make(map[string]*throttled.GCRARateLimiterCtx),
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
		return nil, status.InternalErrorf("unable to init redis store: %s", err)
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
	c := config.DummyConfig()
	qm, err := NewQuotaManager(env, c)
	if err != nil {
		return err
	}
	env.SetQuotaManager(qm)
	return nil
}
