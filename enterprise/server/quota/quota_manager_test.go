package quota

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/pubsub"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	qpb "github.com/buildbuddy-io/buildbuddy/proto/quota"
)

type testBucket struct {
	config *tables.QuotaBucket
}

func (tb *testBucket) Config() tables.QuotaBucket {
	return *tb.config
}

func (tb *testBucket) Allow(ctx context.Context, key string, quantity int64) (bool, error) {
	return true, nil
}

func stringPtr(str string) *string {
	return &str
}

func createTestBucket(env environment.Env, config *tables.QuotaBucket) (Bucket, error) {
	return &testBucket{
		config: config,
	}, nil
}

func fetchAllQuotaBuckets(t *testing.T, env *testenv.TestEnv, ctx context.Context) []*tables.QuotaBucket {
	return fetchQuotaBuckets(t, env, ctx, "")
}

func fetchQuotaBuckets(t *testing.T, env *testenv.TestEnv, ctx context.Context, namespace string) []*tables.QuotaBucket {
	res := []*tables.QuotaBucket{}
	dbh := env.GetDBHandle()
	q := query_builder.NewQuery(`SELECT * FROM QuotaBuckets`)
	if namespace != "" {
		q.AddWhereClause("namespace = ?", namespace)
	}
	qStr, qArgs := q.Build()
	rows, err := dbh.DB(ctx).Raw(qStr, qArgs...).Rows()
	require.NoError(t, err)

	for rows.Next() {
		tb := &tables.QuotaBucket{}
		err := dbh.DB(ctx).ScanRows(rows, tb)
		require.NoError(t, err)
		// Throw out Model timestamps to simplify assertions.
		tb.Model = tables.Model{}
		res = append(res, tb)
	}
	return res
}

func fetchAllQuotaGroups(t *testing.T, env *testenv.TestEnv, ctx context.Context) []*tables.QuotaGroup {
	res := []*tables.QuotaGroup{}
	dbh := env.GetDBHandle()
	rows, err := dbh.DB(ctx).Raw(`SELECT * FROM QuotaGroups`).Rows()
	require.NoError(t, err)

	for rows.Next() {
		tg := &tables.QuotaGroup{}
		err := dbh.DB(ctx).ScanRows(rows, tg)
		require.NoError(t, err)
		// Throw out Model timestamps to simplify assertions.
		tg.Model = tables.Model{}
		res = append(res, tg)
	}
	return res
}

func TestQuotaManagerFindBucket(t *testing.T) {
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

	qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
	require.NoError(t, err)

	testCases := []struct {
		name       string
		quotaKey   string
		namespace  string
		wantConfig tables.QuotaBucket
	}{
		{
			name:       "restricted bucket",
			quotaKey:   "GR123456",
			namespace:  "remote_execution",
			wantConfig: *buckets[1],
		},
		{
			name:       "default bucket",
			quotaKey:   "GR0000",
			namespace:  "remote_execution",
			wantConfig: *buckets[0],
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

func TestGetNamespace(t *testing.T) {
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

	qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
	require.NoError(t, err)

	testCases := []struct {
		name string
		req  *qpb.GetNamespaceRequest
	}{
		{
			name: "get all namespaces",
			req:  &qpb.GetNamespaceRequest{},
		},
		{
			name: "get one namespace",
			req: &qpb.GetNamespaceRequest{
				Namespace: "remote_execution",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			resp, err := qm.GetNamespace(ctx, tc.req)
			require.NoError(t, err)
			// use sortProtos to ignore orders
			sortProtos := cmpopts.SortSlices(func(m1, m2 protocmp.Message) bool { return m1.String() < m2.String() })
			assert.Empty(t, cmp.Diff([]*qpb.Namespace{
				{
					Name: "remote_execution",
					AssignedBuckets: []*qpb.AssignedBucket{
						{
							Bucket: &qpb.Bucket{
								Name: "default",
								MaxRate: &qpb.Rate{
									NumRequests: 100,
									Period:      durationpb.New(time.Second),
								},
								MaxBurst: 105,
							},
						},
						{

							Bucket: &qpb.Bucket{
								Name: "restricted",
								MaxRate: &qpb.Rate{
									NumRequests: 10,
									Period:      durationpb.New(time.Second),
								},
								MaxBurst: 12,
							},
							QuotaKeys: []string{"GR123456"},
						},
					},
				},
			}, resp.GetNamespaces(), protocmp.Transform(), sortProtos))
		})
	}
}

func TestModifyNamespace_AddBucket(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()

	bucket := &tables.QuotaBucket{
		Namespace:          "remote_execution",
		Name:               "default",
		NumRequests:        100,
		PeriodDurationUsec: int64(time.Second / time.Microsecond),
		MaxBurst:           105,
	}
	db := env.GetDBHandle().DB(ctx)
	result := db.Create(&bucket)
	require.NoError(t, result.Error)
	testCases := []struct {
		name        string
		req         *qpb.ModifyNamespaceRequest
		wantError   bool
		wantBuckets []*tables.QuotaBucket
	}{
		{
			name: "add a new bucket to an existing namespace",
			req: &qpb.ModifyNamespaceRequest{
				Namespace: "remote_execution",
				AddBucket: &qpb.Bucket{
					Name: "restricted",
					MaxRate: &qpb.Rate{
						NumRequests: 10,
						Period:      durationpb.New(time.Second),
					},
					MaxBurst: 10,
				},
			},
			wantError: false,
			wantBuckets: []*tables.QuotaBucket{
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
					MaxBurst:           10,
				},
			},
		},
		{
			name: "add an existing bucket",
			req: &qpb.ModifyNamespaceRequest{
				Namespace: "remote_execution",
				AddBucket: &qpb.Bucket{
					Name: "default",
					MaxRate: &qpb.Rate{
						NumRequests: 10,
						Period:      durationpb.New(time.Second),
					},
					MaxBurst: 10,
				},
			},
			wantError: true,
		},
		{
			name: "add a new bucket to a new namespace",
			req: &qpb.ModifyNamespaceRequest{
				Namespace: "GetTargets",
				AddBucket: &qpb.Bucket{
					Name: "default",
					MaxRate: &qpb.Rate{
						NumRequests: 1000,
						Period:      durationpb.New(time.Second),
					},
					MaxBurst: 10,
				},
			},
			wantBuckets: []*tables.QuotaBucket{
				{
					Namespace:          "GetTargets",
					Name:               "default",
					NumRequests:        1000,
					PeriodDurationUsec: int64(time.Second / time.Microsecond),
					MaxBurst:           10,
				},
			},
		},
	}
	for _, tc := range testCases {
		qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
		require.NoError(t, err)
		t.Run(tc.name, func(t *testing.T) {
			_, err := qm.ModifyNamespace(ctx, tc.req)
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				got := fetchQuotaBuckets(t, env, ctx, tc.req.GetNamespace())
				require.NoError(t, result.Error)
				// use sortProtos to ignore orders
				sortProtos := cmpopts.SortSlices(func(m1, m2 protocmp.Message) bool { return m1.String() < m2.String() })
				assert.Empty(t, cmp.Diff(tc.wantBuckets, got, protocmp.Transform(), sortProtos))
			}
		})
	}
}

func TestModifyNamespace_UpdateBucket(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ctx := context.Background()

	bucket := &tables.QuotaBucket{
		Namespace:          "remote_execution",
		Name:               "default",
		NumRequests:        100,
		PeriodDurationUsec: int64(time.Second / time.Microsecond),
		MaxBurst:           105,
	}
	db := env.GetDBHandle().DB(ctx)
	result := db.Create(&bucket)
	require.NoError(t, result.Error)
	testCases := []struct {
		name       string
		req        *qpb.ModifyNamespaceRequest
		wantError  bool
		wantBucket *tables.QuotaBucket
	}{
		{
			name: "modify a non-existent bucket",
			req: &qpb.ModifyNamespaceRequest{
				Namespace: "remote_execution",
				UpdateBucket: &qpb.Bucket{
					Name: "bar",
					MaxRate: &qpb.Rate{
						NumRequests: 10,
						Period:      durationpb.New(time.Second),
					},
					MaxBurst: 10,
				},
			},
			wantError: true,
			wantBucket: &tables.QuotaBucket{
				Namespace:          "remote_execution",
				Name:               "default",
				NumRequests:        100,
				PeriodDurationUsec: int64(time.Second / time.Microsecond),
				MaxBurst:           105,
			},
		},
		{
			name: "modify an existing bucket",
			req: &qpb.ModifyNamespaceRequest{
				Namespace: "remote_execution",
				UpdateBucket: &qpb.Bucket{
					Name: "default",
					MaxRate: &qpb.Rate{
						NumRequests: 105,
						Period:      durationpb.New(time.Second),
					},
					MaxBurst: 10,
				},
			},
			wantError: false,
			wantBucket: &tables.QuotaBucket{
				Namespace:          "remote_execution",
				Name:               "default",
				NumRequests:        105,
				PeriodDurationUsec: int64(time.Second / time.Microsecond),
				MaxBurst:           10,
			},
		},
	}
	for _, tc := range testCases {
		qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
		require.NoError(t, err)
		t.Run(tc.name, func(t *testing.T) {
			_, err := qm.ModifyNamespace(ctx, tc.req)
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			got := fetchQuotaBuckets(t, env, ctx, tc.req.GetNamespace())
			require.NoError(t, result.Error)
			// use sortProtos to ignore orders
			sortProtos := cmpopts.SortSlices(func(m1, m2 protocmp.Message) bool { return m1.String() < m2.String() })
			assert.Empty(t, cmp.Diff([]*tables.QuotaBucket{tc.wantBucket}, got, protocmp.Transform(), sortProtos))
			time.Sleep(500 * time.Millisecond)
			b := qm.findBucket("remote_execution", "GR123456")
			config := b.Config()
			config.Model = tables.Model{}
			assert.Empty(t, cmp.Diff(config, *tc.wantBucket, protocmp.Transform()))
		})
	}
}

func TestModifyNamespace_RemoveBucket(t *testing.T) {
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

	quotaGroups := []*tables.QuotaGroup{
		{
			Namespace:  "remote_execution",
			QuotaKey:   "GR123456",
			BucketName: "restricted",
		},
		{
			Namespace:  "remote_execution",
			QuotaKey:   "GR1234567",
			BucketName: "restricted",
		},
	}
	result := db.Create(&buckets)
	require.NoError(t, result.Error)
	result = db.Create(&quotaGroups)
	require.NoError(t, result.Error)

	testCases := []struct {
		name        string
		req         *qpb.ModifyNamespaceRequest
		wantError   bool
		wantBuckets []*tables.QuotaBucket
	}{
		{
			name: "remove a non-existent bucket",
			req: &qpb.ModifyNamespaceRequest{
				Namespace:    "remote_execution",
				RemoveBucket: "foo",
			},
			wantBuckets: []*tables.QuotaBucket{
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
			},
		},
		{
			name: "remove an existing bucket",
			req: &qpb.ModifyNamespaceRequest{
				Namespace:    "remote_execution",
				RemoveBucket: "restricted",
			},
			wantError: false,
			wantBuckets: []*tables.QuotaBucket{
				{
					Namespace:          "remote_execution",
					Name:               "default",
					NumRequests:        100,
					PeriodDurationUsec: int64(time.Second / time.Microsecond),
					MaxBurst:           105,
				},
			},
		},
	}
	for _, tc := range testCases {
		qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
		require.NoError(t, err)
		t.Run(tc.name, func(t *testing.T) {
			_, err := qm.ModifyNamespace(ctx, tc.req)
			assert.NoError(t, err)
			got := fetchQuotaBuckets(t, env, ctx, tc.req.GetNamespace())
			require.NoError(t, result.Error)
			// use sortProtos to ignore orders
			sortProtos := cmpopts.SortSlices(func(m1, m2 protocmp.Message) bool { return m1.String() < m2.String() })
			assert.Empty(t, cmp.Diff(tc.wantBuckets, got, protocmp.Transform(), sortProtos))
			row := &struct{ Count int }{}
			result := db.Raw(
				`SELECT COUNT(*) as count FROM QuotaGroups WHERE namespace = ? AND bucket_name = ?`,
				tc.req.GetNamespace(), tc.req.GetRemoveBucket(),
			).Scan(row)
			require.NoError(t, result.Error)
			assert.Equal(t, 0, row.Count)
		})
	}
}

func TestRemoveNamespace(t *testing.T) {
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
		{
			Namespace:          "bytestream/read",
			Name:               "default",
			NumRequests:        100,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           6,
		},
		{
			Namespace:          "bytestream/read",
			Name:               "restricted",
			NumRequests:        10,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           2,
		},
	}

	quotaGroups := []*tables.QuotaGroup{
		{
			Namespace:  "remote_execution",
			QuotaKey:   "GR123456",
			BucketName: "restricted",
		},
		{
			Namespace:  "remote_execution",
			QuotaKey:   "GR1234567",
			BucketName: "restricted",
		},
		{
			Namespace:  "bytestream/read",
			QuotaKey:   "GR123456",
			BucketName: "restricted",
		},
	}
	result := db.Create(&buckets)
	require.NoError(t, result.Error)
	result = db.Create(&quotaGroups)
	require.NoError(t, result.Error)

	qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
	require.NoError(t, err)

	_, err = qm.RemoveNamespace(ctx, &qpb.RemoveNamespaceRequest{
		Namespace: "remote_execution",
	})

	gotBuckets := fetchAllQuotaBuckets(t, env, ctx)
	assert.ElementsMatch(t, gotBuckets, []*tables.QuotaBucket{
		{
			Namespace:          "bytestream/read",
			Name:               "default",
			NumRequests:        100,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           6,
		},
		{
			Namespace:          "bytestream/read",
			Name:               "restricted",
			NumRequests:        10,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           2,
		},
	})

	gotGroups := fetchAllQuotaGroups(t, env, ctx)
	assert.ElementsMatch(t, gotGroups, []*tables.QuotaGroup{
		{
			Namespace:  "bytestream/read",
			QuotaKey:   "GR123456",
			BucketName: "restricted",
		},
	})
}

func TestQuotaManagerApplyBucket(t *testing.T) {
	env := testenv.GetTestEnv(t)
	udb, err := userdb.NewUserDB(env, env.GetDBHandle())
	require.NoError(t, err)
	env.SetUserDB(udb)
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
		{
			Namespace:          "remote_execution",
			Name:               "banned",
			NumRequests:        1,
			PeriodDurationUsec: int64(time.Second / time.Microsecond),
			MaxBurst:           1,
		},
	}

	quotaGroups := []*tables.QuotaGroup{
		{
			Namespace:  "remote_execution",
			QuotaKey:   "GR123456",
			BucketName: "restricted",
		},
	}

	groups := []*tables.Group{
		{
			GroupID: "GR123456",
		},
		{
			GroupID: "GR123457",
		},
		{
			GroupID: "GR123458",
		},
	}
	result := db.Create(&buckets)
	require.NoError(t, result.Error)
	result = db.Create(&quotaGroups)
	require.NoError(t, result.Error)
	result = db.Create(&groups)
	require.NoError(t, result.Error)

	testCases := []struct {
		name           string
		req            *qpb.ApplyBucketRequest
		wantError      bool
		wantQuotaKey   string
		wantBucketName *string
	}{
		{
			name: "group doesn't exist",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					GroupId: "GR111111",
				},
				Namespace:  "remote_execution",
				BucketName: "restricted",
			},
			wantError: true,
		},
		{
			name: "invalid ip",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					IpAddress: "123.1",
				},
				Namespace:  "remote_execution",
				BucketName: "restricted",
			},
			wantError: true,
		},
		{
			name: "bucket doesn't exist",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					GroupId: "GR123456",
				},
				Namespace:  "remote_execution",
				BucketName: "foo",
			},
			wantError: true,
		},
		{
			name: "insert a new quota group",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					GroupId: "GR123457",
				},
				Namespace:  "remote_execution",
				BucketName: "restricted",
			},
			wantError:      false,
			wantQuotaKey:   "GR123457",
			wantBucketName: stringPtr("restricted"),
		},
		{
			name: "modify an existing quota group",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					GroupId: "GR123456",
				},
				Namespace:  "remote_execution",
				BucketName: "banned",
			},
			wantError:      false,
			wantQuotaKey:   "GR123456",
			wantBucketName: stringPtr("banned"),
		},
		{
			name: "modify an existing quota group to default",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					GroupId: "GR123456",
				},
				Namespace:  "remote_execution",
				BucketName: "default",
			},
			wantError:      false,
			wantQuotaKey:   "GR123456",
			wantBucketName: nil,
		},
		{
			name: "insert a quota group to default",
			req: &qpb.ApplyBucketRequest{
				Key: &qpb.QuotaKey{
					GroupId: "GR123457",
				},
				Namespace:  "remote_execution",
				BucketName: "default",
			},
			wantError:      false,
			wantQuotaKey:   "GR123458",
			wantBucketName: nil,
		},
	}
	for _, tc := range testCases {
		qm, err := newQuotaManager(env, pubsub.NewTestPubSub(), createTestBucket)
		require.NoError(t, err)
		t.Run(tc.name, func(t *testing.T) {
			_, err := qm.ApplyBucket(ctx, tc.req)
			if tc.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				row := &struct{ BucketName string }{}
				result := db.Raw(
					`SELECT bucket_name FROM QuotaGroups WHERE namespace = ? AND quota_key = ?`,
					tc.req.GetNamespace(), tc.wantQuotaKey,
				).Scan(row)
				require.NoError(t, result.Error)
				if tc.wantBucketName == nil {
					assert.Equal(t, result.RowsAffected, int64(0))
				} else {
					assert.Equal(t, row.BucketName, *tc.wantBucketName)
				}
			}
		})
	}

}
