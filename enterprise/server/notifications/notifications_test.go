package notifications_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	nfpb "github.com/buildbuddy-io/buildbuddy/proto/notification"
)

type DeliveredEmail struct {
	FromName    string
	FromAddress string
	ToAddresses []string
	Subject     string
	ContentType string
	Content     string
}

type fakeEmailClient struct {
	C    chan *DeliveredEmail
	seen map[string]bool
}

func NewFakeEmailClient() *fakeEmailClient {
	return &fakeEmailClient{
		C:    make(chan *DeliveredEmail, 128),
		seen: map[string]bool{},
	}
}

type fakeUserDB struct {
	interfaces.UserDB
	emails []string
}

func NewFakeUserDB(emails ...string) *fakeUserDB {
	return &fakeUserDB{emails: emails}
}

func (f *fakeUserDB) GetGroupAdminUserEmailsForAlerts(ctx context.Context, tx interfaces.DB, groupID string) ([]string, error) {
	return f.emails, nil
}

func (f *fakeEmailClient) SendEmail(ctx context.Context, idempotencyKey, fromName, fromAddress string, toAddresses []string, subject, contentType, content string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if f.seen[idempotencyKey] {
		return nil
	}
	f.seen[idempotencyKey] = true
	f.C <- &DeliveredEmail{
		FromName:    fromName,
		FromAddress: fromAddress,
		ToAddresses: toAddresses,
		Subject:     subject,
		ContentType: contentType,
		Content:     content,
	}
	return nil
}

func TestNotificationDelivery(t *testing.T) {
	flags.Set(t, "notifications.email.from_address", "noreply@buildbuddy.io")
	ctx := context.Background()
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: testredis.Start(t).Target,
	})
	service, err := notifications.NewService(env.GetBlobstore(), env.GetDefaultRedisClient())
	require.NoError(t, err)
	fakeUserDB := NewFakeUserDB("admin1@test.io", "admin2@test.io")
	emailService := NewFakeEmailClient()
	pool, err := notifications.NewDeliveryWorkerPool(ctx, env.GetDBHandle(), fakeUserDB, env.GetDefaultRedisClient(), env.GetBlobstore(), emailService)
	require.NoError(t, err)
	pool.Start(ctx)

	err = notifications.Transaction(ctx, env.GetDBHandle(), func(tx notifications.TX) error {
		created, err := service.Create(ctx, tx, "GR1", "test-content-id", &nfpb.Notification{
			Email: &nfpb.Email{
				Subject: "Test email subject",
				Body:    "Test email body",
			},
		})
		require.True(t, created)
		return err
	})
	require.NoError(t, err)

	// The email should have been sent to all org admins.
	email := <-emailService.C

	require.Equal(t, &DeliveredEmail{
		FromName:    "BuildBuddy",
		FromAddress: "noreply@buildbuddy.io",
		ToAddresses: []string{"admin1@test.io", "admin2@test.io"},
		Subject:     "[BuildBuddy] Test email subject",
		ContentType: "text/html",
		Content:     "Test email body",
	}, email)

	time.Sleep(100 * time.Millisecond)
	select {
	case v := <-emailService.C:
		require.FailNow(t, "unexpected email", "%v", v)
	default:
	}
}
