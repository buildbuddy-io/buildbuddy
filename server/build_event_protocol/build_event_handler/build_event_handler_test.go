package build_event_handler_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclock"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
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

func progressEventWithOutput(stdout, stderr string) *anypb.Any {
	progressAny := &anypb.Any{}
	progressAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Progress{
			Progress: &build_event_stream.Progress{
				Stderr: stderr,
				Stdout: stdout,
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

func buildMetadataEvent(metadata map[string]string) *anypb.Any {
	metadataAny := &anypb.Any{}
	metadataAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_BuildMetadata{
			BuildMetadata: &build_event_stream.BuildMetadata{Metadata: metadata},
		},
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
	commandLineAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_StructuredCommandLine{
			StructuredCommandLine: commandLine,
		},
	})
	return commandLineAny
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

func assertAPIKeyRedacted(t *testing.T, invocation *inpb.Invocation, apiKey string) {
	txt := proto.MarshalTextString(invocation)
	assert.NotContains(t, txt, apiKey, "API key %q should not appear in invocation", apiKey)
	assert.NotContains(t, txt, "x-buildbuddy-api-key", "All remote headers should be redacted")
}

type FakeUsageTracker struct {
	invocations int64
}

func (t *FakeUsageTracker) Increment(ctx context.Context, usage *tables.UsageCounts) error {
	t.invocations += usage.Invocations
	return nil
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
	assert.Equal(t, "", invocation.RepoUrl)

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
	assert.Equal(t, "", invocation.RepoUrl)

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

	// Send started event with api key
	request = streamRequest(finishedEvent(), "test-invocation-id", 4)
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

func TestHandleEventWithEnvAndMetadataRedaction(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send unauthenticated started event without an api key
	request := streamRequest(startedEvent(
		"--remote_upload_local_results "+
			"--build_metadata='ALLOW_ENV=FOO_ALLOWED' "+
			"--build_metadata='REPO_URL=https://username:githubToken@github.com/acme-inc/acme'",
	), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send env and metadata with info that should be redacted
	request = streamRequest(structuredCommandLineEvent(map[string]string{
		"FOO_ALLOWED": "public_env_value",
		"FOO_SECRET":  "secret_env_value",
	}), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	request = streamRequest(buildMetadataEvent(map[string]string{
		// Note: ALLOW_ENV is also present in the build metadata event (not just the
		// started event). The build metadata event may come after the structured
		// command line event, which contains the env vars, but we should still
		// redact properly in this case.
		"ALLOW_ENV": "FOO_ALLOWED",
		"REPO_URL":  "https://username:githubToken@github.com/acme-inc/acme",
	}), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status so events get flushed. Include a secret here as well.
	request = streamRequest(workspaceStatusEvent(
		"REPO_URL", "https://username:githubToken@github.com/acme-inc/acme",
	), "test-invocation-id", 4)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure we redacted correctly
	invocation, err := build_event_handler.LookupInvocation(te, ctx, "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "https://github.com/acme-inc/acme", invocation.RepoUrl)
	txt := proto.MarshalTextString(invocation)
	assert.NotContains(t, txt, "secret_env_value", "Env secrets should not appear in invocation")
	assert.NotContains(t, txt, "githubToken", "URL secrets should not appear in invocation")
	assert.Contains(t, txt, "--client_env=FOO_ALLOWED=public_env_value", "Values of allowed env vars should not be redacted")
	assert.Contains(t, txt, "--client_env=FOO_SECRET=<REDACTED>", "Values of non-allowed env vars should be redacted")
}

func TestHandleEventWithUsageTracking(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ut := &FakeUsageTracker{}
	te.SetUsageTracker(ut)
	flags.Set(t, "app.usage_tracking_enabled", "true")
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), "test-invocation-id", 2)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	assert.Equal(t, int64(1), ut.invocations)

	// Send another started event for good measure; we should still only count 1
	// invocation since it's the same stream.
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	assert.Equal(t, int64(1), ut.invocations)
}

func TestFinishedFinalizeWithCanceledContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Cancel the context
	cancel()

	// Finalize the invocation
	err = channel.FinalizeInvocation("test-invocation-id")
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestFinishedFinalize(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation("test-invocation-id")
	assert.NoError(t, err)
	cancel()

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestUnfinishedFinalizeWithCanceledContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Cancel the context
	cancel()

	// Finalize the invocation
	err = channel.FinalizeInvocation("test-invocation-id")
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestUnfinishedFinalize(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation("test-invocation-id")
	assert.NoError(t, err)
	cancel()

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestRetryOnComplete(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it gets removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", te.GetConfigurator().GetStorageChunkFileSizeBytes()/2+1), ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
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

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName("test-invocation-id", 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationId("test-invocation-id"))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, "test-invocation-id")
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure old files were not deleted
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName("test-invocation-id", 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationId("test-invocation-id"))
	assert.NoError(t, err)
	assert.True(t, exists)

}

func TestRetryOnDisconnect(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it gets removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", te.GetConfigurator().GetStorageChunkFileSizeBytes()/2+1), ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
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
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName("test-invocation-id", 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationId("test-invocation-id"))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, "test-invocation-id")
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure the old protofile was removed
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName("test-invocation-id", 0))
	assert.NoError(t, err)
	assert.False(t, exists)

	// Make sure old event log was deleted
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationId("test-invocation-id"))
	assert.NoError(t, err)
	assert.False(t, exists)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "def456"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), "test-invocation-id", 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation("test-invocation-id")
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
	assert.NoError(t, err)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestRetryOnOldDisconnect(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel := handler.OpenChannel(ctx, "test-invocation-id")

	// Say that it occurred 5 hours ago
	te.GetInvocationDB().SetNowFunc(testclock.StartingAt(time.Now().Add(-5 * time.Hour)).Now)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err := channel.HandleEvent(request)
	assert.NoError(t, err)

	// Write some stuff to disk so we can verify it gets removed on retry
	request = streamRequest(progressEventWithOutput(strings.Repeat("a", te.GetConfigurator().GetStorageChunkFileSizeBytes()/2+1), ""), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), "test-invocation-id", 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), "test-invocation-id")
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
	assert.Equal(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName("test-invocation-id", 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationId("test-invocation-id"))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Reset the time for the database
	te.GetInvocationDB().SetNowFunc(time.Now)

	// Attempt to start a new invocation with the same id
	channel = handler.OpenChannel(ctx, "test-invocation-id")
	request = streamRequest(startedEvent("--remote_header='"+testauth.APIKeyHeader+"=USER1'"), "test-invocation-id", 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure old files were not deleted
	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName("test-invocation-id", 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationId("test-invocation-id"))
	assert.NoError(t, err)
	assert.True(t, exists)
}
