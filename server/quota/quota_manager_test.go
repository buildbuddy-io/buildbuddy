package quota

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBucket struct {
	config *tables.QuotaBucket
}

func (tb *testBucket) Config() *tables.QuotaBucket {
	return tb.config
}

func (tb *testBucket) Allow(ctx context.Context, key string, quantity int64) (bool, error) {
	return true, nil
}

func createTestBucket(env environment.Env, config *tables.QuotaBucket) (Bucket, error) {
	return &testBucket{
		config: config,
	}, nil
}

func TestQuotaManagerFindRateLimiter(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()

	db := env.GetDBHandle().DB(ctx)
	buckets := []*tables.QuotaBucket{
		{
			Namespace:          "remote_execution",
			Name:               "default",
			NumRequests:        100,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           105,
		},
		{
			Namespace:          "remote_execution",
			Name:               "restricted",
			NumRequests:        10,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           12,
		},
	}

	quotaGroup := &tables.QuotaGroup{
		Namespace:  "remote_execution",
		QuotaKey:   "GR123456",
		BucketName: "restricted",
	}
	result := db.Create(&buckets)
	require.NoError(t, result.Error)
	result = db.Create(quotaGroup)
	require.NoError(t, result.Error)

	qm, err := newQuotaManager(env, createTestBucket)
	require.NoError(t, err)

	testCases := []struct {
		name       string
		quotaKey   string
		namespace  string
		wantConfig *tables.QuotaBucket
	}{
		{
			name:       "restricted bucket",
			quotaKey:   "GR123456",
			namespace:  "remote_execution",
			wantConfig: buckets[1],
		},
		{
			name:       "default bucket",
			quotaKey:   "GR0000",
			namespace:  "remote_execution",
			wantConfig: buckets[0],
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bucket := qm.findBucket(tc.namespace, tc.quotaKey)
			config := bucket.Config()
			assert.Equal(t, config, tc.wantConfig)
		})
	}
}
