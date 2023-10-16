package server_notification_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/server_notification"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

func TestPubSub(t *testing.T) {
	h := testredis.Start(t)
	app1 := server_notification.New("app", h.Client())
	app2 := server_notification.New("app", h.Client())
	ch1 := app1.Subscribe(&snpb.InvalidateIPRulesCache{})
	ch2 := app2.Subscribe(&snpb.InvalidateIPRulesCache{})

	ctx := context.Background()

	// Publish a message.
	msg := &snpb.InvalidateIPRulesCache{GroupId: "123"}
	err := app1.Publish(ctx, msg)
	require.NoError(t, err)

	// Make sure it's received by both subscribers.
	require.Empty(t, cmp.Diff(msg, <-ch1, protocmp.Transform()))
	require.Empty(t, cmp.Diff(msg, <-ch2, protocmp.Transform()))
}
