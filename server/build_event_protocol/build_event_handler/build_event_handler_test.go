package build_event_handler_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclock"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/anypb"

	bspb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
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
	progressAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_Progress{
			Progress: &bspb.Progress{
				Stderr: "stderr",
				Stdout: "stdout",
			},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_Progress{}},
	})
	return progressAny
}

func progressEventWithOutput(stdout, stderr string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_Progress{
			Progress: &bspb.Progress{
				Stderr: stderr,
				Stdout: stdout,
			},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_Progress{}},
	})
	return progressAny
}

func workspaceStatusEvent(key, value string) *anypb.Any {
	workspaceStatusAny := &anypb.Any{}
	workspaceStatusAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_WorkspaceStatus{
			WorkspaceStatus: &bspb.WorkspaceStatus{
				Item: []*bspb.WorkspaceStatus_Item{
					{Key: key, Value: value},
				},
			},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_WorkspaceStatus{}},
	})
	return workspaceStatusAny
}

func toType[T any](v any, t T) T {
	return v.(T)
}

func startedEvent(options string, children ...any) *anypb.Any {
	startedAny := &anypb.Any{}
	childIds := []*bspb.BuildEventId{}
	for _, c := range children {
		childIds = append(childIds, &bspb.BuildEventId{Id: toType(c, bspb.BuildEventId{}.Id)})
	}
	startedAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_Started{
			Started: &bspb.BuildStarted{
				OptionsDescription: options,
			},
		},
		Children: childIds,
		Id:       &bspb.BuildEventId{Id: &bspb.BuildEventId_Started{}},
	})
	return startedAny
}

func optionsParsedEvent(options string) *anypb.Any {
	optionsParsedAny := &anypb.Any{}
	optionsParsedAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_OptionsParsed{
			OptionsParsed: &bspb.OptionsParsed{
				CmdLine: strings.Split(options, " "),
			},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_OptionsParsed{}},
	})
	return optionsParsedAny
}

func buildMetadataEvent(metadata map[string]string) *anypb.Any {
	metadataAny := &anypb.Any{}
	metadataAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_BuildMetadata{
			BuildMetadata: &bspb.BuildMetadata{Metadata: metadata},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildMetadata{}},
	})
	return metadataAny
}

func structuredCommandLineEvent(env map[string]string) *anypb.Any {
	options := []*clpb.Option{}
	for k, v := range env {
		options = append(options, &clpb.Option{
			CombinedForm: fmt.Sprintf("--client_env=%s=%s", k, v),
			OptionName:   "client_env",
			OptionValue:  fmt.Sprintf("%s=%s", k, v),
		})
	}
	commandLine := &clpb.CommandLine{
		CommandLineLabel: "original command line",
		Sections: []*clpb.CommandLineSection{
			{
				SectionLabel: "command options",
				SectionType: &clpb.CommandLineSection_OptionList{
					OptionList: &clpb.OptionList{Option: options},
				},
			},
		},
	}
	commandLineAny := &anypb.Any{}
	commandLineAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_StructuredCommandLine{
			StructuredCommandLine: commandLine,
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_StructuredCommandLine{StructuredCommandLine: &bspb.BuildEventId_StructuredCommandLineId{CommandLineLabel: "original command line"}}},
	})
	return commandLineAny
}

func finishedEvent() *anypb.Any {
	finishedAny := &anypb.Any{}
	finishedAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_Finished{
			Finished: &bspb.BuildFinished{
				ExitCode: &bspb.BuildFinished_ExitCode{},
			},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildFinished{}},
	})
	return finishedAny
}

func assertAPIKeyRedacted(t *testing.T, invocation *inpb.Invocation, apiKey string) {
	txt, err := prototext.Marshal(invocation)
	require.NoError(t, err)
	assert.NotContains(t, string(txt), apiKey, "API key %q should not appear in invocation", apiKey)
	assert.NotContains(t, string(txt), "x-buildbuddy-api-key", "All remote headers should be redacted")
}

type FakeUsageTracker struct {
	invocations int64
}

func (t *FakeUsageTracker) Increment(ctx context.Context, usage *tables.UsageCounts) error {
	t.invocations += usage.Invocations
	return nil
}

func (t *FakeUsageTracker) StartDBFlush() {}
func (t *FakeUsageTracker) StopDBFlush()  {}

func TestUnauthenticatedHandleEventWithStartedFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send unauthenticated started event without an api key
	request := streamRequest(startedEvent("--remote_upload_local_results"), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's public
	invocation, err := build_event_handler.LookupInvocation(te, ctx, testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_PUBLIC, invocation.ReadPermission)
}

func TestAuthenticatedHandleEventWithStartedFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send authenticated started event with api key
	request := streamRequest(startedEvent("--remote_upload_local_results --remote_header='"+testauth.APIKeyHeader+"=USER1' --remote_instance_name=foo --should_be_redacted=USER1", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestAuthenticatedHandleEventWithOptionlessStartedEvent(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	request := streamRequest(startedEvent("", &bspb.BuildEventId_WorkspaceStatus{}, &bspb.BuildEventId_OptionsParsed{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	request = streamRequest(optionsParsedEvent("--remote_upload_local_results --remote_header='"+testauth.APIKeyHeader+"=USER1' --remote_instance_name=foo --should_be_redacted=USER1"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestAuthenticatedHandleEventWithProgressFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send progress event
	request := streamRequest(progressEvent(), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "USER1")
}

func TestUnAuthenticatedHandleEventWithProgressFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send progress event
	request := streamRequest(progressEvent(), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with no api key
	request = streamRequest(startedEvent("--remote_upload_local_results"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's publicly visible
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_PUBLIC, invocation.ReadPermission)
}

func TestHandleEventOver100ProgressEventsBeforeStarted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send 104 progress events
	for i := 1; i < 105; i++ {
		request := streamRequest(progressEvent(), testInvocationID, int64(i))
		err := channel.HandleEvent(request)
		assert.NoError(t, err)
	}

	// Make sure invocation isn't written
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), testInvocationID, 105)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
}

func TestHandleEventWithWorkspaceStatusBeforeStarted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send progress event
	request := streamRequest(progressEvent(), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make invocation sure isn't written yet
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 4)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestHandleEventWithEnvAndMetadataRedaction(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)

	testInvocationID := testUUID.String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send unauthenticated started event without an api key
	request := streamRequest(startedEvent(
		"--remote_upload_local_results "+
			"--build_metadata='ALLOW_ENV=FOO_ALLOWED' "+
			"--build_metadata='REPO_URL=https://username:githubToken@github.com/acme-inc/acme'",
		&bspb.BuildEventId_StructuredCommandLine{StructuredCommandLine: &bspb.BuildEventId_StructuredCommandLineId{CommandLineLabel: "original command line"}},
		&bspb.BuildEventId_BuildMetadata{},
		&bspb.BuildEventId_WorkspaceStatus{},
	), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send env and metadata with info that should be redacted
	request = streamRequest(structuredCommandLineEvent(map[string]string{
		"FOO_ALLOWED": "public_env_value",
		"FOO_SECRET":  "secret_env_value",
	}), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	request = streamRequest(buildMetadataEvent(map[string]string{
		// Note: ALLOW_ENV is also present in the build metadata event (not just the
		// started event). The build metadata event may come after the structured
		// command line event, which contains the env vars, but we should still
		// redact properly in this case.
		"ALLOW_ENV": "FOO_ALLOWED",
		"REPO_URL":  "https://username:githubToken@github.com/acme-inc/acme",
	}), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status so events get flushed. Include a secret here as well.
	request = streamRequest(workspaceStatusEvent(
		"REPO_URL", "https://username:githubToken@github.com/acme-inc/acme",
	), testInvocationID, 4)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure we redacted correctly
	invocation, err := build_event_handler.LookupInvocation(te, ctx, testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "https://github.com/acme-inc/acme", invocation.RepoUrl)
	txt, err := prototext.Marshal(invocation)
	require.NoError(t, err)
	assert.NotContains(t, string(txt), "secret_env_value", "Env secrets should not appear in invocation")
	assert.NotContains(t, string(txt), "githubToken", "URL secrets should not appear in invocation")
	assert.Contains(t, string(txt), "--client_env=FOO_ALLOWED=public_env_value", "Values of allowed env vars should not be redacted")
	assert.Contains(t, string(txt), "--client_env=FOO_SECRET=<REDACTED>", "Values of non-allowed env vars should be redacted")
}

func TestHandleEventWithUsageTracking(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ut := &FakeUsageTracker{}
	te.SetUsageTracker(ut)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	assert.Equal(t, int64(1), ut.invocations)

	// Send another started event for good measure; we should still only count 1
	// invocation since it's the same stream.
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	assert.Equal(t, int64(1), ut.invocations)
}

func TestFinishedFinalizeWithCanceledContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Cancel the context
	cancel()

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestFinishedFinalize(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)
	cancel()

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestUnfinishedFinalizeWithCanceledContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Cancel the context
	cancel()

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestUnfinishedFinalize(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)
	cancel()

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestRetryOnComplete(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it gets removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", chunkSize/2+1), ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, testInvocationID)
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure old files were not deleted
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

}

func TestRetryOnDisconnect(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it gets removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", chunkSize/2+1), ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, testInvocationID)
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure the old protofile was not removed
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Make sure old event log was not deleted
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "def456"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

	// Make sure the new protofile exists
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 2), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Make sure the new event log exists
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 2))
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRetryTwiceOnDisconnect(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it doesn't get removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", chunkSize/2+1), ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, testInvocationID)
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it doesn't get removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("b", chunkSize/2+1), ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure the old protofile was not removed
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Make sure old event log was not deleted
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "def456"), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 2), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 2))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, testInvocationID)
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it doesn't get removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("c", chunkSize/2+1), ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure the old protofile was not removed
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 2), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Make sure old event log was not deleted
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 2))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "000789"), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "000789", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "000789", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

	// Make sure all protofiles exist
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 2), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 3), 0))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Make sure all event logs exist
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 2))
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 3))
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRetryOnOldDisconnect(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, testInvocationID)

	// Say that it occurred 5 hours ago
	te.GetInvocationDB().SetNowFunc(testclock.StartingAt(time.Now().Add(-5 * time.Hour)).Now)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it gets removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", chunkSize/2+1), ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Reset the time for the database
	te.GetInvocationDB().SetNowFunc(time.Now)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, testInvocationID)
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure old files were not deleted
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)
}
