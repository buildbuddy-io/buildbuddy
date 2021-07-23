package build_event_handler_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	anypb "github.com/golang/protobuf/ptypes/any"
)

func streamRequest(anyEvent *anypb.Any, iid string, sequenceNumer int64) *pepb.PublishBuildToolEventStreamRequest {
	return &pepb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pepb.OrderedBuildEvent{
			SequenceNumber: sequenceNumer,
			StreamId:       &bepb.StreamId{InvocationId: iid},
			Event: &bepb.BuildEvent{
				Event: &bepb.BuildEvent_BazelEvent{
					BazelEvent: anyEvent,
				},
			},
		},
	}
}

func progressEvent() *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Progress{
			Progress: &build_event_stream.Progress{
				Stderr: "stderr",
				Stdout: "stdout",
			},
		},
	})
	return progressAny
}

func workspaceStatusEvent(key, value string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_WorkspaceStatus{
			WorkspaceStatus: &build_event_stream.WorkspaceStatus{
				Item: []*build_event_stream.WorkspaceStatus_Item{
					{Key: key, Value: value},
				},
			},
		},
	})
	return progressAny
}

func startedEvent(options string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				OptionsDescription: options,
			},
		},
	})
	return progressAny
}

func assertAPIKeyRedacted(t *testing.T, invocation *inpb.Invocation, apiKey string) {
	txt := proto.MarshalTextString(invocation)
	assert.NotContains(t, txt, apiKey, "API key %q should not appear in invocation", apiKey)
	assert.Contains(t, txt, "--remote_header='x-buildbuddy-api-key=<REDACTED>'", "All remote headers should be redacted")
	assert.Contains(t, txt, "--should_be_redacted=<REDACTED>", "BuildBuddy API key should be redacted, even if not in remote header")
}

func TestUnauthenticatedHandleEventWithStartedFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send unauthenticated started event without an api key
	request := streamRequest(startedEvent("--remote_upload_local_results"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's public
	invocation, err := build_event_handler.LookupInvocation(te, ctx, "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_PUBLIC, invocation.ReadPermission)
}

func TestAuthenticatedHandleEventWithStartedFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send authenticated started event with api key
	request := streamRequest(startedEvent("--remote_upload_local_results --remote_header='"+testauth.APIKeyHeader+"=USER1' --remote_instance_name=foo --should_be_redacted=USER1"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestAuthenticatedHandleEventWithProgressFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send progress event
	request := streamRequest(progressEvent(), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestAuthenticatedHandleEventWithStartedFirst_OptimizedAPIKeyRedaction(t *testing.T) {
	flags.Set(t, "enable_optimized_redaction", "true")
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send authenticated started event with api key
	request := streamRequest(startedEvent("--remote_upload_local_results --remote_header='"+testauth.APIKeyHeader+"=USER1' --remote_instance_name=foo --should_be_redacted=USER1"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestAuthenticatedHandleEventWithProgressFirst_OptimizedAPIKeyRedaction(t *testing.T) {
	flags.Set(t, "enable_optimized_redaction", "true")
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send progress event
	request := streamRequest(progressEvent(), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestUnAuthenticatedHandleEventWithProgressFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send progress event
	request := streamRequest(progressEvent(), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.Error(t, err)

	// Send started event with no api key
	request = streamRequest(startedEvent("--remote_upload_local_results"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's publicly visible
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_PUBLIC, invocation.ReadPermission)
}

func TestHandleEventOver100ProgressEventsBeforeStarted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send 104 progress events
	for i := 1; i < 105; i++ {
		request := streamRequest(progressEvent(), "test-invocation-id", int64(i))
		err := channel.HandleEvent(request)
		assert.NoError(t, err)
	}

	// Make sure invocation isn't written
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.Error(t, err)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 105)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
}

func TestHandleEventWithWorkspaceStatusBeforeStarted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send progress event
	request := streamRequest(progressEvent(), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make invocation sure isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation("test-invocation-id")
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}
