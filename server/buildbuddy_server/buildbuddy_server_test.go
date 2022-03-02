package buildbuddy_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/buildbuddy-io/buildbuddy/proto/acl"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	"github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	user1  = "USER1"
	group1 = "GROUP1"
	user2  = "USER2"
	group2 = "GROUP2"
)

func createInvocationForTesting(te environment.Env, user string) (string, error) {
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	started, err := anypb.New(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				OptionsDescription: "--remote_header='" + testauth.APIKeyHeader + "=" + user + "'",
			},
		},
	})
	request := &pepb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pepb.OrderedBuildEvent{
			SequenceNumber: 1,
			StreamId:       &bepb.StreamId{InvocationId: testInvocationID},
			Event: &bepb.BuildEvent{
				Event: &bepb.BuildEvent_BazelEvent{
					BazelEvent: started,
				},
			},
		},
	}
	err = channel.HandleEvent(request)
	if err != nil {
		return "", err
	}
	return testInvocationID, err
}

func TestGetInvocation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1, user2, group2))
	te.SetAuthenticator(auth)

	iid, err := createInvocationForTesting(te, user1)
	require.NoError(t, err)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	rsp, err := server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(rsp.Invocation))
	require.Equal(t, rsp.Invocation[0].InvocationId, iid)

	rsp, err = server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{Lookup: &inpb.InvocationLookup{InvocationId: ""}},
	)
	require.Error(t, err)

	rsp, err = server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user2),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user2, group2),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.Error(t, err)
}

func TestSearchInvocation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1))
	te.SetAuthenticator(auth)

	// Search Service is enterprise-only
	te.SetInvocationSearchService(nil)

	_, err := createInvocationForTesting(te, user1)
	require.NoError(t, err)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	_, err = server.SearchInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.SearchInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			Query:          &inpb.InvocationQuery{User: user1}},
	)
	require.Error(t, err)
}

func TestUpdateInvocation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1, user2, group2))
	te.SetAuthenticator(auth)
	te.GetDBHandle().DB(context.Background()).Create(&tables.Group{GroupID: group1, SharingEnabled: true})

	iid, err := createInvocationForTesting(te, user1)
	require.NoError(t, err)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	_, err = server.UpdateInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.UpdateInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			InvocationId:   iid,
			Acl: &acl.ACL{
				UserId:            &user_id.UserId{Id: user2},
				GroupId:           group2,
				OwnerPermissions:  &acl.ACL_Permissions{Read: true, Write: true},
				GroupPermissions:  &acl.ACL_Permissions{Read: true, Write: true},
				OthersPermissions: &acl.ACL_Permissions{Read: true, Write: false},
			}},
	)
	require.NoError(t, err)

	rsp, err := server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user2),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user2, group2),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(rsp.Invocation))
	require.Equal(t, rsp.Invocation[0].InvocationId, iid)
}

func TestDeleteInvocation(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1))
	te.SetAuthenticator(auth)

	iid, err := createInvocationForTesting(te, user1)
	require.NoError(t, err)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	rsp, err := server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(rsp.Invocation))
	require.Equal(t, rsp.Invocation[0].InvocationId, iid)

	_, err = server.DeleteInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.DeleteInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			InvocationId:   iid},
	)
	require.NoError(t, err)

	rsp, err = server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.Error(t, err)
}
