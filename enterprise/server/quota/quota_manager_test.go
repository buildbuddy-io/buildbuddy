package quota

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBucket struct {
	config *bucketConfig
	err    error
}

func (tb *testBucket) Config() bucketConfig {
	return *tb.config
}

func (tb *testBucket) Allow(ctx context.Context, key string, quantity int64, recordUsageOverages bool) (bool, error) {
	return tb.err == nil, tb.err
}

func createTestBucket(env environment.Env, config *bucketConfig) (Bucket, error) {
	return &testBucket{
		config: config,
	}, nil
}

type testLimitingBucket struct {
	numRequests int64
	maxRequests int64
	config      *bucketConfig
}

func (tb *testLimitingBucket) Config() bucketConfig {
	return *tb.config
}

func (tb *testLimitingBucket) Allow(ctx context.Context, key string, quantity int64, recordUsageOverages bool) (bool, error) {
	withinQuota := tb.numRequests+quantity <= tb.maxRequests
	if withinQuota || recordUsageOverages {
		tb.numRequests += quantity
	}
	return withinQuota, nil
}

func createTestLimitingBucket(env environment.Env, config *bucketConfig, limit int64) (Bucket, error) {
	return &testLimitingBucket{
		config:      config,
		numRequests: 0,
		maxRequests: limit,
	}, nil
}

func TestQuotaService_Errors(t *testing.T) {
	flags.Set(t, "app.enable_quota_management", true)
	for _, failure := range []string{"missing key", "missing provider", "bucket error"} {
		t.Run(failure, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			users := testauth.TestUsers("US001", "GR001")
			env.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
			ctx := testauth.WithAuthenticatedUserInfo(t.Context(), users["US001"])
			fp, err := experiments.NewFlagProvider(t.Name())
			require.NoError(t, err)
			env.SetExperimentFlagProvider(fp)
			qm := &QuotaManager{env: env}
			bucketErr := fmt.Errorf("quota storage unavailable")
			ns := &namespace{name: "test"}
			ns.bucketsByKey.Store("GR001", &testBucket{err: bucketErr})
			qm.namespaces.Store("test", ns)
			switch failure {
			case "missing key":
				ctx = t.Context()
			case "missing provider":
				env.SetExperimentFlagProvider(nil)
			}

			// Allow should not fail open on quota service errors.
			require.NoError(t, qm.Allow(ctx, "test", 1, false /* force */))
		})
	}
}

func TestQuotaFlagdBuckets(t *testing.T) {
	flags.Set(t, "app.enable_quota_management", true)
	env := testenv.GetTestEnv(t)
	testUsers := testauth.TestUsers("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testUsers))
	ctx := testauth.WithAuthenticatedUserInfo(context.Background(), testUsers["US001"])

	adb, err := authdb.NewAuthDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetAuthDB(adb)
	udb, err := userdb.NewUserDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetUserDB(udb)

	alreadyCreated := false

	// This might get called if the test is run with --config=race from
	// the subscribed hook to the flagd config. Debounce the bucket creation.
	var cb Bucket
	customBucketCreator := func(env environment.Env, config *bucketConfig) (Bucket, error) {
		if alreadyCreated {
			return cb, nil
		}
		cb, err = createTestLimitingBucket(env, config, 5)
		if err != nil {
			return nil, err
		}
		alreadyCreated = true
		return cb, nil
	}

	flags := map[string]memprovider.InMemoryFlag{
		bucketQuotaExperimentName: {
			State:          memprovider.Enabled,
			DefaultVariant: "custom",
			Variants: map[string]any{
				"custom": map[string]any{
					"rpc:/google.bytestream.ByteStream/Read": map[string]any{
						"name": "restrictRead",
						"maxRate": map[string]any{
							"numRequests": int64(10),
							"periodUsec":  int64(60 * 1000 * 1000),
						},
						"maxBurst": int64(5),
					},
				},
			},
		},
	}
	provider := memprovider.NewInMemoryProvider(flags)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	qm, err := newQuotaManager(env, customBucketCreator)
	require.NoError(t, err)
	usageOperation := "rpc:/google.bytestream.ByteStream/Read"

	// Read-only checks do not consume quota.
	assert.NoError(t, qm.Allow(ctx, usageOperation, 0, false /* force */))
	assert.NoError(t, qm.Allow(ctx, usageOperation, 0, false /* force */))

	// The first 5 requests are allowed, but the 6th is blocked.
	for range 5 {
		assert.NoError(t, qm.Allow(ctx, usageOperation, 1, false /* force */))
	}
	assert.Error(t, qm.Allow(ctx, usageOperation, 1, false /* force */))

	// Usage is recorded even when over quota.
	fit, err := qm.RecordUsage(ctx, usageOperation, 1, false /* force */)
	require.NoError(t, err)
	assert.False(t, fit)
	assert.Error(t, qm.Allow(ctx, usageOperation, 0, false /* force */))

	bucket := qm.findBucket(usageOperation, "GR001")
	require.NotNil(t, bucket)
	config := bucket.Config()
	assert.Equal(t, usageOperation, config.namespace)
	assert.Equal(t, "flagd:"+usageOperation+":10:60000000:5", config.name)
	assert.Equal(t, int64(10), config.numRequests)
	assert.Equal(t, int64(60000000), config.periodDurationUsec)
	assert.Equal(t, int64(5), config.maxBurst)
}

func TestLoadQuotasFromFlagd(t *testing.T) {
	env := testenv.GetTestEnv(t)
	testUsers := testauth.TestUsers("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testUsers))
	ctx := testauth.WithAuthenticatedUserInfo(context.Background(), testUsers["US001"])

	adb, err := authdb.NewAuthDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetAuthDB(adb)
	udb, err := userdb.NewUserDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetUserDB(udb)

	flags := map[string]memprovider.InMemoryFlag{
		bucketQuotaExperimentName: {
			State:          memprovider.Enabled,
			DefaultVariant: "custom",
			Variants: map[string]any{
				"custom": map[string]any{
					"rpc:/namespace1": map[string]any{
						"maxRate": map[string]any{
							"numRequests": int64(10),
							"periodUsec":  int64(60000000),
						},
						"maxBurst": int64(5),
					},
					"rpc:/namespace2": map[string]any{
						"maxRate": map[string]any{
							"numRequests": int64(20),
							"periodUsec":  int64(120000000),
						},
						"maxBurst": int64(10),
					},
				},
			},
		},
	}
	provider := memprovider.NewInMemoryProvider(flags)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	qm, err := newQuotaManager(env, createTestBucket)
	require.NoError(t, err)

	t.Run("insert new bucket for first time", func(t *testing.T) {
		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key1", "rpc:/namespace1"))

		bucket := qm.findBucket("rpc:/namespace1", "key1")
		require.NotNil(t, bucket)
		config := bucket.Config()
		assert.Equal(t, "rpc:/namespace1", config.namespace)
		assert.Equal(t, int64(10), config.numRequests)
		assert.Equal(t, int64(60000000), config.periodDurationUsec)
		assert.Equal(t, int64(5), config.maxBurst)
	})

	t.Run("skip loading if b	ucket already exists", func(t *testing.T) {
		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key1", "rpc:/namespace1"))

		originalBucket := qm.findBucket("rpc:/namespace1", "key1")
		require.NotNil(t, originalBucket)

		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key1", "rpc:/namespace1"))

		bucket := qm.findBucket("rpc:/namespace1", "key1")
		assert.Equal(t, originalBucket, bucket)
	})

	t.Run("add new bucket to existing namespace without removing others", func(t *testing.T) {
		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key1", "rpc:/namespace1"))
		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key2", "rpc:/namespace1"))

		bucket1 := qm.findBucket("rpc:/namespace1", "key1")
		require.NotNil(t, bucket1, "original bucket should still exist")

		bucket2 := qm.findBucket("rpc:/namespace1", "key2")
		require.NotNil(t, bucket2, "new bucket should exist")

		assert.Equal(t, bucket1.Config().namespace, bucket2.Config().namespace)
	})

	t.Run("different namespaces are independent", func(t *testing.T) {
		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key1", "rpc:/namespace1"))
		require.NoError(t, qm.loadQuotasFromFlagd(ctx, "key1", "rpc:/namespace2"))

		ns1Bucket := qm.findBucket("rpc:/namespace1", "key1")
		ns2Bucket := qm.findBucket("rpc:/namespace2", "key1")

		require.NotNil(t, ns1Bucket)
		require.NotNil(t, ns2Bucket)

		ns1Config := ns1Bucket.Config()
		ns2Config := ns2Bucket.Config()

		assert.Equal(t, "rpc:/namespace1", ns1Config.namespace)
		assert.Equal(t, "rpc:/namespace2", ns2Config.namespace)
		assert.Equal(t, int64(10), ns1Config.numRequests)
		assert.Equal(t, int64(20), ns2Config.numRequests)
	})
}

func TestBucketRowFromMap(t *testing.T) {
	testCases := []struct {
		name       string
		namespace  string
		bucketMap  map[string]any
		wantBucket *bucketConfig
		wantError  bool
	}{
		{
			name:      "valid bucket",
			namespace: "rpc:/google.bytestream.ByteStream/Read",
			bucketMap: map[string]any{
				"maxRate": map[string]any{
					"numRequests": float64(10),
					"periodUsec":  float64(60 * 1000 * 1000),
				},
				"maxBurst": float64(5),
			},
			wantBucket: &bucketConfig{
				namespace:          "rpc:/google.bytestream.ByteStream/Read",
				name:               "flagd:rpc:/google.bytestream.ByteStream/Read:10:60000000:5",
				numRequests:        10,
				periodDurationUsec: 60000000,
				maxBurst:           5,
			},
			wantError: false,
		},
		{
			name:      "missing maxRate",
			namespace: "test",
			bucketMap: map[string]any{
				"maxBurst": int64(5),
			},
			wantError: true,
		},
		{
			name:      "invalid numRequests",
			namespace: "test",
			bucketMap: map[string]any{
				"maxRate": map[string]any{
					"numRequests": int64(-1),
					"periodUsec":  int64(60 * 1000 * 1000),
				},
				"maxBurst": int64(5),
			},
			wantError: true,
		},
		{
			name:      "zero periodUsec",
			namespace: "test",
			bucketMap: map[string]any{
				"maxRate": map[string]any{
					"numRequests": int64(10),
					"periodUsec":  int64(0),
				},
				"maxBurst": int64(5),
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, err := bucketConfigFromMap(tc.namespace, tc.bucketMap)
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantBucket, bucket)
			}
		})
	}
}

func TestForced(t *testing.T) {
	flags.Set(t, "app.enable_quota_management", false)
	env := testenv.GetTestEnv(t)
	users := testauth.TestUsers("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
	ctx := testauth.WithAuthenticatedUserInfo(t.Context(), users["US001"])
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
	qm := &QuotaManager{env: env}
	bucket := &testLimitingBucket{maxRequests: 1}
	ns := &namespace{name: "test"}
	ns.bucketsByKey.Store("GR001", bucket)
	qm.namespaces.Store("test", ns)

	// Quota is not applied when globally disabled.
	require.NoError(t, qm.Allow(ctx, "test", 2, false /* force */))
	withinQuota, err := qm.RecordUsage(ctx, "test", 2, false /* force */)
	require.NoError(t, err)
	require.True(t, withinQuota)
	require.Equal(t, int64(0), bucket.numRequests)

	// Forcing applies quota even when globally disabled.
	err = qm.Allow(ctx, "test", 2, true /* force */)
	require.True(t, status.IsResourceExhaustedError(err), "expected ResourceExhausted, got %v", err)
	withinQuota, err = qm.RecordUsage(ctx, "test", 2, true /* force */)
	require.NoError(t, err)
	require.False(t, withinQuota)
	require.Equal(t, int64(2), bucket.numRequests)
}

func TestConcurrentBucketAccess(t *testing.T) {
	env := testenv.GetTestEnv(t)
	testUsers := testauth.TestUsers("US001", "GR001")
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testUsers))
	ctx := testauth.WithAuthenticatedUserInfo(context.Background(), testUsers["US001"])

	adb, err := authdb.NewAuthDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetAuthDB(adb)
	udb, err := userdb.NewUserDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetUserDB(udb)

	flags := map[string]memprovider.InMemoryFlag{
		bucketQuotaExperimentName: {
			State:          memprovider.Enabled,
			DefaultVariant: "custom",
			Variants: map[string]any{
				"custom": map[string]any{
					"rpc:/test.Service/Method": map[string]any{
						"maxRate": map[string]any{
							"numRequests": int64(100),
							"periodUsec":  int64(60000000),
						},
						"maxBurst": int64(50),
					},
				},
			},
		},
	}
	provider := memprovider.NewInMemoryProvider(flags)
	domain := t.Name()
	require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

	fp, err := experiments.NewFlagProvider(domain)
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	qm, err := newQuotaManager(env, createTestBucket)
	require.NoError(t, err)

	const numGoroutines = 10
	const numIterations = 100

	done := make(chan bool)
	for i := range numGoroutines {
		keyID := i
		go func() {
			defer func() { done <- true }()
			key := fmt.Sprintf("key%d", keyID)
			for range numIterations {
				qm.loadQuotasFromFlagd(ctx, key, "rpc:/test.Service/Method")
				qm.findBucket("rpc:/test.Service/Method", key)
			}
		}()
	}

	for range numGoroutines {
		<-done
	}
}

func TestCreateGCRABucket_RateLimit(t *testing.T) {
	redisHandle := testredis.Start(t)
	rdb := redisHandle.Client()
	t.Cleanup(func() { require.NoError(t, rdb.Close()) })

	env := testenv.GetTestEnv(t)
	env.SetDefaultRedisClient(rdb)
	bucket, err := createGCRABucket(env, &bucketConfig{
		namespace:          "test-namespace",
		name:               "test-bucket",
		numRequests:        1,
		periodDurationUsec: int64(time.Hour.Microseconds()),
		maxBurst:           2,
	})
	require.NoError(t, err)
	ctx := t.Context()

	// Read-only checks shouldn't create the key in redis.
	allowed, err := bucket.Allow(ctx, "key", 0, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.True(t, allowed)
	count, err := rdb.Exists(ctx, "quota:test-namespace:test-bucket:key").Result()
	require.NoError(t, err)
	require.Zero(t, count)

	for range 3 {
		allowed, err := bucket.Allow(ctx, "key", 1, false /* recordUsageOverages */)
		require.NoError(t, err)
		require.True(t, allowed)
	}
	allowed, err = bucket.Allow(ctx, "key", 1, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.False(t, allowed)

	// Rate limit state is independent for each key.
	allowed, err = bucket.Allow(ctx, "other-key", 1, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestCreateGCRABucket_RecordUsageBeyondQuota(t *testing.T) {
	rdb := testredis.Start(t).Client()
	env := testenv.GetTestEnv(t)
	env.SetDefaultRedisClient(rdb)
	bucket, err := createGCRABucket(env, &bucketConfig{
		namespace: "test", name: "test", numRequests: 1,
		periodDurationUsec: time.Hour.Microseconds(), maxBurst: 2,
	})
	require.NoError(t, err)
	ctx := t.Context()

	// Calling Allow with recordUsageOverages=false should not consume quota if it's blocked.
	allowed, err := bucket.Allow(ctx, "allow-key", 10, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.False(t, allowed)
	// Subsequent calls to Allow should fit, if the lower request is within quota.
	allowed, err = bucket.Allow(ctx, "allow-key", 1, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.True(t, allowed)

	// Calling Allow with recordUsageOverages=true should consume quota if it's blocked.
	withinQuota, err := bucket.Allow(ctx, "record-key", 10, true /* recordUsageOverages */)
	require.NoError(t, err)
	require.False(t, withinQuota)
	// Subsequent calls to Allow should not fit, even if they are small or read-only checks, because the previous
	// request consumed all available quota.
	allowed, err = bucket.Allow(ctx, "record-key", 1, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.False(t, allowed)
	allowed, err = bucket.Allow(ctx, "record-key", 0, false /* recordUsageOverages */)
	require.NoError(t, err)
	require.False(t, allowed)

	// When recordUsageOverages=true, the usage is still counted even while already over quota.
	before, err := rdb.Get(ctx, "quota:test:test:record-key").Int64()
	require.NoError(t, err)
	withinQuota, err = bucket.Allow(ctx, "record-key", 5, true /* recordUsageOverages */)
	require.NoError(t, err)
	require.False(t, withinQuota)
	after, err := rdb.Get(ctx, "quota:test:test:record-key").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(5*time.Hour), after-before)
}

func TestCreateGCRABucket_VeryLargePeriod(t *testing.T) {
	redisHandle := testredis.Start(t)
	env := testenv.GetTestEnv(t)
	env.SetDefaultRedisClient(redisHandle.Client())

	overflowConfig := &bucketConfig{
		namespace:          "test-namespace",
		name:               "test-bucket-overflow",
		numRequests:        1,
		periodDurationUsec: 1e16, // will overflow when converted to nanoseconds
		maxBurst:           0,
	}

	// make sure this doesn't panic
	_, err := createGCRABucket(env, overflowConfig)
	require.ErrorContains(t, err, "unable to create GCRARateLimiter: invalid RateQuota throttled.RateQuota")
}
