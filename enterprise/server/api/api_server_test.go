package api

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

var userMap = testauth.TestUsers("user1", "group1")

func TestGetInvocation(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()
	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetInvocation(ctx, &apipb.GetInvocationRequest{Selector: &apipb.InvocationSelector{InvocationId: testInvocationID}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, len(resp.Invocation))
}

func TestGetInvocationNotFound(t *testing.T) {
	env, ctx := getEnvAndCtx(t, "user1")
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()
	streamBuild(t, env, testInvocationID)
	testUUID, err = uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID2 := testUUID.String()
	s := NewAPIServer(env)
	resp, err := s.GetInvocation(ctx, &apipb.GetInvocationRequest{Selector: &apipb.InvocationSelector{InvocationId: testInvocationID2}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 0, len(resp.Invocation))
}

func TestGetInvocationAuth(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetInvocation(ctx, &apipb.GetInvocationRequest{Selector: &apipb.InvocationSelector{InvocationId: testInvocationID}})
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestGetTarget(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetTarget(ctx, &apipb.GetTargetRequest{Selector: &apipb.TargetSelector{InvocationId: testInvocationID}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 3, len(resp.Target))
}

func TestGetTargetAuth(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetTarget(ctx, &apipb.GetTargetRequest{Selector: &apipb.TargetSelector{InvocationId: testInvocationID}})
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestGetTargetByLabel(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetTarget(ctx, &apipb.GetTargetRequest{Selector: &apipb.TargetSelector{InvocationId: testInvocationID, Label: "//my/target:foo"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, len(resp.Target))
	assert.Equal(t, resp.Target[0].Language, "java")
}

func TestGetTargetByTag(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetTarget(ctx, &apipb.GetTargetRequest{Selector: &apipb.TargetSelector{InvocationId: testInvocationID, Tag: "tag-b"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 2, len(resp.Target))
}

func TestGetAction(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	env.GetInvocationDB().CreateInvocation(ctx, &tables.Invocation{InvocationID: testInvocationID})
	s := NewAPIServer(env)
	resp, err := s.GetAction(ctx, &apipb.GetActionRequest{Selector: &apipb.ActionSelector{InvocationId: testInvocationID}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 3, len(resp.Action))
	assert.Equal(t, resp.Action[0].File[0].Hash, "5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b")
	assert.Equal(t, resp.Action[0].File[0].SizeBytes, int64(152092))
}

func TestGetActionWithTargetID(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	testTargetID := "//my/target:foo"

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	env.GetInvocationDB().CreateInvocation(ctx, &tables.Invocation{InvocationID: testInvocationID})
	s := NewAPIServer(env)
	resp, err := s.GetAction(ctx, &apipb.GetActionRequest{Selector: &apipb.ActionSelector{InvocationId: testInvocationID, TargetId: testTargetID}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, len(resp.Action))
	assert.Equal(t, resp.Action[0].File[0].Hash, "5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b")
	assert.Equal(t, resp.Action[0].File[0].SizeBytes, int64(152092))
}

func TestGetActionWithTargetLabel(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	testTargetLabel := "//my/other/target:foo"

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	env.GetInvocationDB().CreateInvocation(ctx, &tables.Invocation{InvocationID: testInvocationID})
	s := NewAPIServer(env)
	resp, err := s.GetAction(ctx, &apipb.GetActionRequest{Selector: &apipb.ActionSelector{InvocationId: testInvocationID, TargetLabel: testTargetLabel}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, len(resp.Action))
	assert.Equal(t, resp.Action[0].TargetLabel, "//my/other/target:foo")
	assert.Equal(t, resp.Action[0].File[0].Hash, "5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b")
	assert.Equal(t, resp.Action[0].File[0].SizeBytes, int64(152092))
}

func TestGetActionAuth(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetAction(ctx, &apipb.GetActionRequest{Selector: &apipb.ActionSelector{InvocationId: testInvocationID}})
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestLog(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	env.GetInvocationDB().CreateInvocation(ctx, &tables.Invocation{InvocationID: testInvocationID})
	s := NewAPIServer(env)
	resp, err := s.GetLog(ctx, &apipb.GetLogRequest{Selector: &apipb.LogSelector{InvocationId: testInvocationID}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "hello world", resp.GetLog().GetContents())
}

func TestGetLogAuth(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetLog(ctx, &apipb.GetLogRequest{Selector: &apipb.LogSelector{InvocationId: testInvocationID}})
	require.Error(t, err)
	require.Nil(t, resp)
}

func getEnvAndCtx(t *testing.T, user string) (*testenv.TestEnv, context.Context) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(userMap)
	te.SetAuthenticator(ta)
	if user == "" {
		return te, context.Background()
	}
	ctx, err := ta.WithAuthenticatedUser(context.Background(), user)
	if err != nil {
		t.Fatal(err)
	}
	return te, ctx
}

func streamBuild(t *testing.T, te *testenv.TestEnv, iid string) {
	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(context.Background(), iid)

	err := channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=user1'"), iid, 1))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetConfiguredEvent("//my/target:foo", "java_binary rule", "tag-a"), iid, 2))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(progressEvent("hello world"), iid, 3))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetCompletedEvent("//my/target:foo"), iid, 4))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetConfiguredEvent("//my/other/target:foo", "go_binary rule", "tag-b"), iid, 5))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetCompletedEvent("//my/other/target:foo"), iid, 6))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetConfiguredEvent("//my/third/target:foo", "genrule rule", "tag-b"), iid, 7))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetCompletedEvent("//my/third/target:foo"), iid, 8))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(finishedEvent(), iid, 9))
	assert.NoError(t, err)

	err = channel.FinalizeInvocation(iid)
	assert.NoError(t, err)
}

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

func targetConfiguredEvent(label, kind, tag string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Id: &build_event_stream.BuildEventId{
			Id: &build_event_stream.BuildEventId_TargetConfigured{
				TargetConfigured: &build_event_stream.BuildEventId_TargetConfiguredId{
					Label: label,
				},
			},
		},
		Payload: &build_event_stream.BuildEvent_Configured{
			Configured: &build_event_stream.TargetConfigured{
				TargetKind: kind,
				Tag:        []string{tag},
			},
		},
	})
	return progressAny
}

func targetCompletedEvent(label string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Id: &build_event_stream.BuildEventId{
			Id: &build_event_stream.BuildEventId_TargetCompleted{
				TargetCompleted: &build_event_stream.BuildEventId_TargetCompletedId{
					Label: label,
					Configuration: &build_event_stream.BuildEventId_ConfigurationId{
						Id: "config1",
					},
				},
			},
		},
		Payload: &build_event_stream.BuildEvent_Completed{
			Completed: &build_event_stream.TargetComplete{
				ImportantOutput: []*build_event_stream.File{
					&build_event_stream.File{
						Name: "my-output.txt",
						File: &build_event_stream.File_Uri{
							Uri: "bytestream://localhost:8080/buildbuddy-io/buildbuddy/ci/blobs/5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b/152092",
						},
					},
				},
			},
		},
	})
	return progressAny
}

func progressEvent(stdout string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Progress{
			Progress: &build_event_stream.Progress{
				Stdout: stdout,
			},
		},
	})
	return progressAny
}

func finishedEvent() *anypb.Any {
	finishedAny := &anypb.Any{}
	finishedAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Finished{
			Finished: &build_event_stream.BuildFinished{
				ExitCode: &build_event_stream.BuildFinished_ExitCode{},
			},
		},
	})
	return finishedAny
}
