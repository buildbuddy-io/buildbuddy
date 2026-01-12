package quota

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

type testBucket struct {
	config *bucketConfig
}

func (tb *testBucket) Config() bucketConfig {
	return *tb.config
}

func (tb *testBucket) Allow(ctx context.Context, key string, quantity int64) (bool, error) {
	return true, nil
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

func (tb *testLimitingBucket) Allow(ctx context.Context, key string, quantity int64) (bool, error) {
	tb.numRequests += quantity
	return tb.numRequests <= tb.maxRequests, nil
}

func createTestLimitingBucket(env environment.Env, config *bucketConfig, limit int64) (Bucket, error) {
	return &testLimitingBucket{
		config:      config,
		numRequests: 0,
		maxRequests: limit,
	}, nil
}

func TestQuotaFlagdBuckets(t *testing.T) {
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

	fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	qm, err := newQuotaManager(env, customBucketCreator)
	require.NoError(t, err)

	assert.NoError(t, qm.Allow(ctx, "rpc:/google.bytestream.ByteStream/Read", 1))

	// The next 4 requests are allowed, but the 5th is blocked.
	for i := 0; i < 4; i++ {
		assert.NoError(t, qm.Allow(ctx, "rpc:/google.bytestream.ByteStream/Read", 1))
	}
	assert.Error(t, qm.Allow(ctx, "rpc:/google.bytestream.ByteStream/Read", 1))

	bucket := qm.findBucket("rpc:/google.bytestream.ByteStream/Read", "GR001")
	require.NotNil(t, bucket)
	config := bucket.Config()
	assert.Equal(t, "rpc:/google.bytestream.ByteStream/Read", config.namespace)
	assert.Equal(t, "flagd:rpc:/google.bytestream.ByteStream/Read:10:60000000:5", config.name)
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

	fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
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
		bucketMap  map[string]interface{}
		wantBucket *bucketConfig
		wantError  bool
	}{
		{
			name:      "valid bucket",
			namespace: "rpc:/google.bytestream.ByteStream/Read",
			bucketMap: map[string]interface{}{
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
			bucketMap: map[string]interface{}{
				"maxBurst": int64(5),
			},
			wantError: true,
		},
		{
			name:      "invalid numRequests",
			namespace: "test",
			bucketMap: map[string]interface{}{
				"maxRate": map[string]interface{}{
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
			bucketMap: map[string]interface{}{
				"maxRate": map[string]interface{}{
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

	fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	qm, err := newQuotaManager(env, createTestBucket)
	require.NoError(t, err)

	const numGoroutines = 10
	const numIterations = 100

	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		keyID := i
		go func() {
			defer func() { done <- true }()
			key := fmt.Sprintf("key%d", keyID)
			for j := 0; j < numIterations; j++ {
				qm.loadQuotasFromFlagd(ctx, key, "rpc:/test.Service/Method")
				qm.findBucket("rpc:/test.Service/Method", key)
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

func TestCheckGroupBlocked(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()
	qm, err := newQuotaManager(env, createTestBucket)
	require.NoError(t, err)

	t.Run("blocked group with flag enabled", func(t *testing.T) {
		flags := map[string]memprovider.InMemoryFlag{
			disallowBlockedGroupsFlagKey: {
				State:          memprovider.Enabled,
				DefaultVariant: "on",
				Variants: map[string]any{
					"on": true,
				},
			},
		}
		provider := memprovider.NewInMemoryProvider(flags)
		domain := t.Name()
		require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

		fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
		require.NoError(t, err)
		env.SetExperimentFlagProvider(fp)

		blockedClaims := &claims.Claims{
			APIKeyID:    "AK001",
			UserID:      "US001",
			GroupID:     "GR001",
			GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS,
		}
		authedCtx := testauth.WithAuthenticatedUserInfo(ctx, blockedClaims)

		err = qm.checkGroupBlocked(authedCtx)
		assert.Error(t, err)
		assert.Equal(t, errBlocked, err)
	})

	t.Run("enterprise group with flag enabled", func(t *testing.T) {
		flags := map[string]memprovider.InMemoryFlag{
			disallowBlockedGroupsFlagKey: {
				State:          memprovider.Enabled,
				DefaultVariant: "on",
				Variants: map[string]any{
					"on": true,
				},
			},
		}
		provider := memprovider.NewInMemoryProvider(flags)
		domain := t.Name()
		require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

		fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
		require.NoError(t, err)
		env.SetExperimentFlagProvider(fp)

		enterpriseClaims := &claims.Claims{
			APIKeyID:    "AK001",
			UserID:      "US001",
			GroupID:     "GR001",
			GroupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS,
		}
		authedCtx := testauth.WithAuthenticatedUserInfo(ctx, enterpriseClaims)
		assert.NoError(t, qm.checkGroupBlocked(authedCtx))
	})

	t.Run("blocked group with flag disabled", func(t *testing.T) {
		flags := map[string]memprovider.InMemoryFlag{
			disallowBlockedGroupsFlagKey: {
				State:          memprovider.Enabled,
				DefaultVariant: "off",
				Variants: map[string]any{
					"off": false,
				},
			},
		}
		provider := memprovider.NewInMemoryProvider(flags)
		domain := t.Name()
		require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

		fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
		require.NoError(t, err)
		env.SetExperimentFlagProvider(fp)

		blockedClaims := &claims.Claims{
			APIKeyID:    "AK001",
			UserID:      "US001",
			GroupID:     "GR001",
			GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS,
		}
		authedCtx := testauth.WithAuthenticatedUserInfo(ctx, blockedClaims)
		assert.NoError(t, qm.checkGroupBlocked(authedCtx))
	})

	t.Run("request without API key", func(t *testing.T) {
		flags := map[string]memprovider.InMemoryFlag{
			disallowBlockedGroupsFlagKey: {
				State:          memprovider.Enabled,
				DefaultVariant: "on",
				Variants: map[string]any{
					"on": true,
				},
			},
		}
		provider := memprovider.NewInMemoryProvider(flags)
		domain := t.Name()
		require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

		fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
		require.NoError(t, err)
		env.SetExperimentFlagProvider(fp)

		webClaims := &claims.Claims{
			UserID:      "US001",
			GroupID:     "GR001",
			GroupStatus: grpb.Group_BLOCKED_GROUP_STATUS,
		}
		authedCtx := testauth.WithAuthenticatedUserInfo(ctx, webClaims)
		assert.NoError(t, qm.checkGroupBlocked(authedCtx))
	})

	t.Run("impersonating request", func(t *testing.T) {
		flags := map[string]memprovider.InMemoryFlag{
			disallowBlockedGroupsFlagKey: {
				State:          memprovider.Enabled,
				DefaultVariant: "on",
				Variants: map[string]any{
					"on": true,
				},
			},
		}
		provider := memprovider.NewInMemoryProvider(flags)
		domain := t.Name()
		require.NoError(t, openfeature.SetNamedProviderAndWait(domain, provider))

		fp, err := experiments.NewFlagProvider(domain, env.GetJWTParser())
		require.NoError(t, err)
		env.SetExperimentFlagProvider(fp)

		impersonatingClaims := &claims.Claims{
			APIKeyID:      "AK001",
			UserID:        "US001",
			GroupID:       "GR001",
			GroupStatus:   grpb.Group_BLOCKED_GROUP_STATUS,
			Impersonating: true,
		}
		authedCtx := testauth.WithAuthenticatedUserInfo(ctx, impersonatingClaims)
		assert.NoError(t, qm.checkGroupBlocked(authedCtx))
	})
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
