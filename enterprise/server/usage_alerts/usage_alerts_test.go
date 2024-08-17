package usage_alerts_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_alerts"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	nfpb "github.com/buildbuddy-io/buildbuddy/proto/notification"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

type Notification struct {
	GroupID   string
	ContentID string
	Payload   *nfpb.Notification
}

type fakeNotifier struct {
	C    chan *Notification
	seen map[string]bool
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{
		C:    make(chan *Notification, 128),
		seen: map[string]bool{},
	}
}

func (f *fakeNotifier) Create(ctx context.Context, tx notifications.TX, groupID, contentID string, n *nfpb.Notification) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// For simplicity, clear out email body contents.
	n = n.CloneVT()
	n.Email = nil

	idempotencyKey := hash.Strings(groupID, contentID)
	if f.seen[idempotencyKey] {
		return false, nil
	}
	f.seen[idempotencyKey] = true
	f.C <- &Notification{GroupID: groupID, ContentID: contentID, Payload: n}
	return true, nil
}

func TestAlerting(t *testing.T) {
	for _, test := range []struct {
		Name                  string
		Now                   string
		Usage                 []*tables.Usage
		AlertingRules         []*tables.UsageAlertingRule
		ExpectedNotifications []*Notification
	}{
		{
			Name: "DailyAlert_YesterdaysUsageExceededThreshold_Trigger",
			Now:  "2024-03-01 12:00",
			Usage: []*tables.Usage{
				{
					GroupID:         "GR1",
					UsageCounts:     tables.UsageCounts{TotalCachedActionExecUsec: (1e7 * time.Minute).Microseconds()},
					PeriodStartUsec: utc("2024-03-01 10:00").UnixMicro(),
				},
			},
			AlertingRules: []*tables.UsageAlertingRule{
				{
					UsageAlertingRuleID: "UR1",
					GroupID:             "GR1",
					AlertingPeriod:      usagepb.AlertingPeriod_DAILY,
					UsageMetric:         usagepb.UsageMetric_TOTAL_CACHED_ACTION_EXEC_USEC,
					Threshold:           (1e6 * time.Minute).Microseconds(),
				},
			},
			ExpectedNotifications: []*Notification{
				{
					GroupID: "GR1",
					Payload: &nfpb.Notification{
						Text: "TestCorp: daily cached build duration usage exceeded 1000000 minutes",
					},
				},
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()
			env := testenv.GetTestEnv(t)
			rdb := testredis.Start(t).Client()
			clock := clockwork.NewFakeClockAt(utc(test.Now))
			notifier := newFakeNotifier()

			// Populate test DB
			env.GetDBHandle().NewQuery(ctx, "test").Create(&tables.Group{
				GroupID:       "GR1",
				Name:          "TestCorp",
				URLIdentifier: "testcorp",
			})
			for _, rule := range test.AlertingRules {
				err := env.GetDBHandle().NewQuery(ctx, "test").Create(rule)
				require.NoError(t, err)
			}
			for _, usage := range test.Usage {
				err := env.GetDBHandle().NewQuery(ctx, "test").Create(usage)
				require.NoError(t, err)
			}

			evaluator, err := usage_alerts.NewEvaluator(ctx, clock, env.GetDBHandle(), rdb, notifier)
			require.NoError(t, err)
			evaluator.Start(ctx)

			// Trigger the main alerting loop several times, advancing the clock
			// each time, but staying within the current alerting period.
			for range 3 {
				// Delete the ticker key if it exists, then wait for it to get
				// recreated. Each time it gets recreated, the scheduler must
				// have completed one loop iteration.
				_, err := rdb.Del(ctx, "usage/alerting/evaluation/ticker").Result()
				require.NoError(t, err)
				// Advance past the evaluation interval
				clock.Advance(1*time.Minute + 1)
				for {
					n, err := rdb.Exists(ctx, "usage/alerting/evaluation/ticker").Result()
					require.NoError(t, err)
					if n > 0 {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}

			// Wait for work queue to be emptied.
			for {
				count, err := rdb.ZCard(ctx, "usage/alerting/evaluation/queue").Result()
				require.NoError(t, err)
				if count == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			// Shut down to make sure we finish processing any remaining
			// notifications.
			evaluator.Shutdown()

			for _, expected := range test.ExpectedNotifications {
				// TODO: make assertions order-independent.
				select {
				case n := <-notifier.C:
					assert.Equal(t, expected.GroupID, n.GroupID)
					assert.Empty(t, cmp.Diff(expected.Payload, n.Payload, protocmp.Transform()))
				default:
					require.FailNow(t, "missing expected notification")
				}
			}

			// There should be no new notifications at this point.
			select {
			case n := <-notifier.C:
				require.FailNowf(t, "got unexpected notification", "%v", n)
			default:
			}
		})
	}
}

func utc(value string) time.Time {
	const layout = "2006-01-02 15:04"
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err.Error())
	}
	return t.UTC()
}
