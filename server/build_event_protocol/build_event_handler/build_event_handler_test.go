package build_event_handler_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/error_tracking"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testolapdb"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testusage"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bspb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	fdpb "github.com/buildbuddy-io/buildbuddy/proto/failure_details"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	zipb "github.com/buildbuddy-io/buildbuddy/proto/zip"
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

// Helper for building an OrderedBuildEvent sequence comprised of a BazelEvent
// stream.
type besSequence struct {
	t *testing.T
	n int64

	InvocationID string
}

func NewBESSequence(t *testing.T) *besSequence {
	iid, err := uuid.NewRandom()
	require.NoError(t, err)
	return &besSequence{
		t:            t,
		InvocationID: iid.String(),
	}
}

func (s *besSequence) NextRequest(event *bspb.BuildEvent) *pepb.PublishBuildToolEventStreamRequest {
	s.n++
	eventAny := &anypb.Any{}
	err := eventAny.MarshalFrom(event)
	require.NoError(s.t, err)
	return &pepb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pepb.OrderedBuildEvent{
			StreamId:       &bepb.StreamId{InvocationId: s.InvocationID},
			SequenceNumber: s.n,
			Event: &bepb.BuildEvent{Event: &bepb.BuildEvent_BazelEvent{
				BazelEvent: eventAny,
			}},
		},
	}
}

type FakeGitHubStatusService struct {
	Clients                []*FakeGitHubStatusClient
	StatusReportingEnabled bool
}

func (s *FakeGitHubStatusService) GetStatusClient() interfaces.GitHubStatusClient {
	client := &FakeGitHubStatusClient{StatusReportingEnabled: s.StatusReportingEnabled}
	s.Clients = append(s.Clients, client)
	return client
}

func (s *FakeGitHubStatusService) GetCreatedClient(t *testing.T) *FakeGitHubStatusClient {
	require.Equal(t, 1, len(s.Clients))
	return s.Clients[0]
}

func (c *FakeGitHubStatusService) HasNoStatuses() bool {
	if len(c.Clients) == 0 {
		return true
	}
	for _, c := range c.Clients {
		if len(c.Statuses) > 0 {
			return false
		}
	}
	return true
}

type FakeGitHubStatusClient struct {
	AccessToken            string
	Statuses               []*FakeGitHubStatus
	StatusReportingEnabled bool
}

type FakeGitHubStatus struct {
	OwnerRepo  string
	CommitSHA  string
	RepoStatus *github.GithubStatusPayload
}

func (c *FakeGitHubStatusClient) CreateStatus(ctx context.Context, groupID, ownerRepo, commitSHA string, p *github.GithubStatusPayload) error {
	s := &FakeGitHubStatus{
		OwnerRepo:  ownerRepo,
		CommitSHA:  commitSHA,
		RepoStatus: p,
	}
	c.Statuses = append(c.Statuses, s)
	return nil
}

func (c *FakeGitHubStatusClient) IsStatusReportingEnabled(ctx context.Context, groupID, repoURL string) (bool, error) {
	return c.StatusReportingEnabled, nil
}

func (c *FakeGitHubStatusClient) ConsumeStatuses() []*FakeGitHubStatus {
	s := c.Statuses
	c.Statuses = nil
	return s
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

func gitFetchCompletedEvent(totalBytes int64, duration time.Duration, retryCount int64) *anypb.Any {
	gitFetchAny := &anypb.Any{}
	gitFetchAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_GitFetchCompleted{
			GitFetchCompleted: &bspb.GitFetchCompleted{
				TotalBytes: totalBytes,
				Duration:   durationpb.New(duration),
				RetryCount: retryCount,
			},
		},
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_GitFetchCompleted{}},
	})
	return gitFetchAny
}

func assertAPIKeyRedacted(t *testing.T, invocation *inpb.Invocation, apiKey string) {
	txt, err := prototext.Marshal(invocation)
	require.NoError(t, err)
	assert.NotContains(t, string(txt), apiKey, "API key %q should not appear in invocation", apiKey)
	assert.NotContains(t, string(txt), "x-buildbuddy-api-key", "All remote headers should be redacted")
}

func TestUnauthenticatedHandleEventWithStartedFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

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
	testUsers := testauth.TestUsers("USER1", "GROUP1")
	// Map "APIKEY1" to User1.
	testUsers["APIKEY1"] = testUsers["USER1"]
	auth := testauth.NewTestAuthenticator(t, testUsers)
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send authenticated started event with api key
	request := streamRequest(startedEvent("--remote_upload_local_results --remote_header='"+authutil.APIKeyHeader+"=APIKEY1' --remote_instance_name=foo --should_be_redacted=APIKEY1", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "USER1", invocation.GetAcl().GetUserId().GetId())
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "USER1", invocation.GetAcl().GetUserId().GetId())
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "APIKEY1")
}

func TestAuthenticatedHandleEventWithOptionlessStartedEvent(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testUsers := testauth.TestUsers("USER1", "GROUP1")
	// Map "APIKEY1" to User1.
	testUsers["APIKEY1"] = testUsers["USER1"]
	auth := testauth.NewTestAuthenticator(t, testUsers)
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	request := streamRequest(startedEvent("", &bspb.BuildEventId_WorkspaceStatus{}, &bspb.BuildEventId_OptionsParsed{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	request = streamRequest(optionsParsedEvent("--remote_upload_local_results --remote_header='"+authutil.APIKeyHeader+"=APIKEY1' --remote_instance_name=foo --should_be_redacted=APIKEY1"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "USER1", invocation.GetAcl().GetUserId().GetId())
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "APIKEY1")
}

func TestAuthenticatedHandleEventWithRedactedStartedEvent(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testUsers := testauth.TestUsers("USER1", "GROUP1")
	// Map "APIKEY1" to User1.
	testUsers["APIKEY1"] = testUsers["USER1"]
	auth := testauth.NewTestAuthenticator(t, testUsers)
	te.SetAuthenticator(auth)
	ctx := testauth.WithAuthenticatedUserInfo(context.Background(), testUsers["USER1"])
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	request := streamRequest(startedEvent("", &bspb.BuildEventId_WorkspaceStatus{}, &bspb.BuildEventId_OptionsParsed{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	request = streamRequest(optionsParsedEvent("--remote_upload_local_results --remote_header='"+authutil.APIKeyHeader+"=' --remote_instance_name=foo"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "USER1", invocation.GetAcl().GetUserId().GetId())
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "APIKEY1")
}

func TestAuthenticatedHandleEventWithProgressFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	testUsers := testauth.TestUsers("USER1", "GROUP1")
	// Map "APIKEY1" to User1.
	testUsers["APIKEY1"] = testUsers["USER1"]
	auth := testauth.NewTestAuthenticator(t, testUsers)
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send progress event
	request := streamRequest(progressEvent(), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	_, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=APIKEY1' --should_be_redacted=APIKEY1", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's only visible to group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "USER1", invocation.GetAcl().GetUserId().GetId())
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)

	// Now write the workspace status event to ensure all events are written,
	// then make sure the API key is not visible in the returned invocation.
	request = streamRequest(workspaceStatusEvent("", ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "APIKEY1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "USER1", invocation.GetAcl().GetUserId().GetId())
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "", invocation.RepoUrl)

	assertAPIKeyRedacted(t, invocation, "APIKEY1")
}

func TestUnAuthenticatedHandleEventWithProgressFirst(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send progress event
	request := streamRequest(progressEvent(), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation isn't written yet
	_, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with no api key
	request = streamRequest(startedEvent("--remote_upload_local_results"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Look up the invocation and make sure it's publicly visible
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_PUBLIC, invocation.ReadPermission)
}

func TestHandleEventOver100ProgressEventsBeforeStarted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send 104 progress events
	for i := 1; i < 105; i++ {
		request := streamRequest(progressEvent(), testInvocationID, int64(i))
		err := channel.HandleEvent(request)
		assert.NoError(t, err)
	}

	// Make sure invocation isn't written
	_, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'"), testInvocationID, 105)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
}

func TestHandleEventWithWorkspaceStatusBeforeStarted(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send progress event
	request := streamRequest(progressEvent(), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send workspace status event with commit sha (which causes a flush)
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make invocation sure isn't written yet
	_, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.Error(t, err)

	// Send started event with api key
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 3)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event
	request = streamRequest(finishedEvent(), testInvocationID, 4)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make sure invocation is only readable by group and has commit sha
	invocation, err := build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, inpb.InvocationPermission_GROUP, invocation.ReadPermission)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestHandleEventWithEnvAndMetadataRedaction(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)

	testInvocationID := testUUID.String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

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

func TestHandleEventRedactsMultilineEnvVar(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	request := streamRequest(startedEvent("", &bspb.BuildEventId_OptionsParsed{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	const multiLineValue = `this value has spaces
and multiple
lines,
oddly.
it even has a
-----BEGIN OPENSSH PRIVATE KEY-----
PRIVATEKEYDATA
-----END OPENSSH PRIVATE KEY-----`
	flagValue := "--action_env=MULTILINE_VAR=" + multiLineValue
	optionsParsed := &bspb.OptionsParsed{
		CmdLine: []string{
			"bazel",
			"build",
			flagValue,
		},
		ExplicitCmdLine: []string{
			"bazel",
			"build",
			flagValue,
		},
	}
	optionsParsedAny := &anypb.Any{}
	err = optionsParsedAny.MarshalFrom(&bspb.BuildEvent{
		Payload: &bspb.BuildEvent_OptionsParsed{OptionsParsed: optionsParsed},
		Id:      &bspb.BuildEventId{Id: &bspb.BuildEventId_OptionsParsed{}},
	})
	require.NoError(t, err)

	request = &pepb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pepb.OrderedBuildEvent{
			StreamId:       &bepb.StreamId{InvocationId: testInvocationID},
			SequenceNumber: 2,
			Event: &bepb.BuildEvent{
				Event: &bepb.BuildEvent_BazelEvent{BazelEvent: optionsParsedAny},
			},
		},
	}
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	invocation, err := build_event_handler.LookupInvocation(te, ctx, testInvocationID)
	assert.NoError(t, err)

	const expected = "--action_env=MULTILINE_VAR=<REDACTED>"
	var actual *bspb.OptionsParsed
	for _, event := range invocation.Event {
		if optionsParsed := event.GetBuildEvent().GetOptionsParsed(); optionsParsed != nil {
			actual = optionsParsed
			break
		}
	}
	require.NotNil(t, actual, "expected an OptionsParsed event in invocation")

	require.Len(t, actual.CmdLine, 3)
	require.Len(t, actual.ExplicitCmdLine, 3)

	expectedOptions := &bspb.OptionsParsed{
		CmdLine: []string{"bazel", "build", expected},
		ExplicitCmdLine: []string{
			"bazel",
			"build",
			expected,
		},
	}
	require.Empty(t, cmp.Diff(expectedOptions, actual, protocmp.Transform()))

	txt, err := prototext.Marshal(invocation)
	require.NoError(t, err)
	assert.NotContains(t, string(txt), "OPENSSH PRIVATE KEY")
	assert.Contains(t, string(txt), expected)
}

func TestHandleEventWithUsageTracking(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ut := testusage.NewTracker()
	te.SetUsageTracker(ut)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []testusage.Total{
		{
			GroupID: "GROUP1",
			Labels:  tables.UsageLabels{},
			Counts: tables.UsageCounts{
				Invocations: 1,
			},
		},
	}, ut.Totals())
	assert.ElementsMatch(t, []testusage.OLAPTotal{
		{
			GroupID: "GROUP1",
			Labels:  sku.Labels{},
			Counts: map[sku.SKU]int64{
				sku.BuildEventsBESCount: 1,
			},
		},
	}, ut.OLAPTotals())

	// Send another started event for good measure; we should still only count 1
	// invocation since it's the same stream.
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1' --should_be_redacted=USER1"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Totals should remain the same (1 invocation total)
	assert.ElementsMatch(t, []testusage.Total{
		{
			GroupID: "GROUP1",
			Labels:  tables.UsageLabels{},
			Counts: tables.UsageCounts{
				Invocations: 1,
			},
		},
	}, ut.Totals())
	assert.ElementsMatch(t, []testusage.OLAPTotal{
		{
			GroupID: "GROUP1",
			Labels:  sku.Labels{},
			Counts: map[sku.SKU]int64{
				sku.BuildEventsBESCount: 1,
			},
		},
	}, ut.OLAPTotals())
}

func TestFinishedFinalizeWithCanceledContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Cancel the context
	cancel()

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestFinishedFinalize(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)
	cancel()

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestGitFetchStatsFlushedToOLAPDB(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send a started event announcing a workspace status event.
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	require.NoError(t, err)

	// Send the workspace status event to complete the metadata.
	request = streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)
	err = channel.HandleEvent(request)
	require.NoError(t, err)

	// Send a GitFetchCompleted event reporting git fetch stats, as published
	// by the remote runner after setting up the git repo.
	request = streamRequest(gitFetchCompletedEvent(9_000_000, 3*time.Second, 2), testInvocationID, 3)
	err = channel.HandleEvent(request)
	require.NoError(t, err)

	// Complete and finalize the invocation, which triggers the flush to the
	// OLAP DB.
	request = streamRequest(finishedEvent(), testInvocationID, 4)
	err = channel.HandleEvent(request)
	require.NoError(t, err)
	err = channel.FinalizeInvocation(testInvocationID)
	require.NoError(t, err)

	// The stats recorder flushes asynchronously; wait for the invocation to
	// show up in the OLAP DB and expect the git fetch stats to be set on it.
	var inv *tables.Invocation
	require.Eventually(t, func() bool {
		inv = olapDB.GetFlushedInvocation(testInvocationID)
		return inv != nil
	}, 30*time.Second, 50*time.Millisecond)
	assert.Equal(t, int64(9_000_000), inv.GitFetchTotalBytes)
	assert.Equal(t, (3 * time.Second).Microseconds(), inv.GitFetchDurationUsec)
	assert.Equal(t, int64(2), inv.GitFetchRetryCount)
}

func TestStatsFlushDoesNotHoldPrimaryDBLock(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	defer close(releaseFlush)
	olapDB.SetBeforeInvocationFlush(func() {
		close(flushStarted)
		<-releaseFlush
	})
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	require.NoError(t, channel.HandleEvent(streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 3)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	select {
	case <-flushStarted:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for OLAP invocation flush")
	}
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- te.GetInvocationDB().DeleteInvocation(context.Background(), testInvocationID)
	}()
	select {
	case err := <-deleteDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "primary DB delete blocked on OLAP invocation flush")
	}
}

func TestBESErrorOccurrencesDisabledByDefault(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	require.NoError(t, channel.HandleEvent(streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)))
	failedAction := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_ActionCompleted{ActionCompleted: &bspb.BuildEventId_ActionCompletedId{Label: "//pkg:target"}}},
		Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{
			Success: false,
			Type:    "GoCompilePkg",
			Stderr:  &bspb.File{Name: "stderr", File: &bspb.File_Contents{Contents: []byte("compile failed")}},
		}},
	}
	failedActionAny := &anypb.Any{}
	require.NoError(t, failedActionAny.MarshalFrom(failedAction))
	require.NoError(t, channel.HandleEvent(streamRequest(failedActionAny, testInvocationID, 3)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 4)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool {
		return olapDB.GetFlushedInvocation(testInvocationID) != nil
	}, 30*time.Second, 50*time.Millisecond)
	require.Empty(t, olapDB.GetErrorOccurrences())
}

func enableErrorTracking(t *testing.T) {
	flags.Set(t, "app.error_tracking_enabled", true)
}

func TestBESErrorOccurrencesFlushedToOLAPDB(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	require.NoError(t, channel.HandleEvent(streamRequest(workspaceStatusEvent("COMMIT_SHA", "abc123"), testInvocationID, 2)))
	failedAction := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_ActionCompleted{ActionCompleted: &bspb.BuildEventId_ActionCompletedId{Label: "//pkg:target"}}},
		Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{Success: false, Type: "GoCompilePkg", ExitCode: 1,
			Stderr:        &bspb.File{Name: "stderr", File: &bspb.File_Uri{Uri: "bytestream://attacker.invalid/blobs/deadbeef/123"}},
			Stdout:        &bspb.File{Name: "stdout", File: &bspb.File_Contents{Contents: []byte("server/pkg/file.go:42:7: undefined: compileThing\nfetch https://user:secret@example.com/source")}},
			FailureDetail: &fdpb.FailureDetail{Message: "compile process 123 failed", Category: &fdpb.FailureDetail_Spawn{Spawn: &fdpb.Spawn{Code: fdpb.Spawn_NON_ZERO_EXIT}}}}},
	}
	failedActionAny := &anypb.Any{}
	require.NoError(t, failedActionAny.MarshalFrom(failedAction))
	require.NoError(t, channel.HandleEvent(streamRequest(failedActionAny, testInvocationID, 3)))
	require.NoError(t, channel.HandleEvent(streamRequest(failedActionAny, testInvocationID, 4)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 5)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 1 }, 30*time.Second, 50*time.Millisecond)
	occurrence := olapDB.GetErrorOccurrences()[0]
	require.Equal(t, "GROUP1", occurrence.GroupID)
	require.Equal(t, "USER1", occurrence.UserID)
	require.Zero(t, occurrence.Perms)
	acl := olapDB.GetErrorInvocationACL(testInvocationID)
	require.NotNil(t, acl)
	require.Equal(t, int32(perms.GROUP_READ|perms.GROUP_WRITE), acl.Perms)
	require.Equal(t, testInvocationID, occurrence.InvocationID)
	require.Equal(t, strings.ToLower(strings.ReplaceAll(testInvocationID, "-", "")), occurrence.InvocationUUID)
	require.Equal(t, "//pkg:target", occurrence.TargetLabel)
	require.Equal(t, "server/pkg/file.go:42:7: undefined: compileThing\nfetch https://user:<REDACTED>@example.com/source", occurrence.Message)
	require.Equal(t, error_tracking.ActionFingerprintVersion, occurrence.FingerprintVersion)
	require.Equal(t, "action_output", occurrence.FingerprintSource)
	require.Equal(t, "high", occurrence.FingerprintConfidence)
	require.NotContains(t, occurrence.Message, "secret")
	require.Equal(t, "abc123", occurrence.CommitSHA)
}

func TestBESTestXMLProducesOneOccurrencePerFailedCase(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	xml := `<testsuite name="checkout"><testcase classname="CardTest" name="Checkout"><failure message="Failed">aggregate failure</failure></testcase>` +
		`<testcase classname="CardTest" name="Checkout/expired"><failure type="AssertionError" message="expected active">` +
		`checkout_test.py:42: expected active; fetch https://user:secret@example.com/result</failure></testcase>` +
		`<testcase classname="CardTest" name="Checkout/declined"><error type="RuntimeError" message="gateway unavailable">stack.py:81</error></testcase></testsuite>`
	testRequest := streamRequest(testResultEvent(t, "//checkout:test", "cfg-a", 2, 3, 4, bspb.TestStatus_FAILED,
		&bspb.File{Name: "bazel-testlogs/checkout/test.xml", File: &bspb.File_Contents{Contents: []byte(xml)}}), testInvocationID, 2)
	testRequest.OrderedBuildEvent.Event.EventTime = timestamppb.New(time.Now().Add(365 * 24 * time.Hour))
	require.NoError(t, channel.HandleEvent(testRequest))
	require.NoError(t, channel.HandleEvent(streamRequest(testSummaryEvent(t, "//checkout:test", "cfg-a", bspb.TestStatus_FAILED), testInvocationID, 3)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 4)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 2 }, 30*time.Second, 50*time.Millisecond)
	occurrences := olapDB.GetErrorOccurrences()
	require.ElementsMatch(t, []string{"Checkout/expired", "Checkout/declined"}, []string{occurrences[0].TestName, occurrences[1].TestName})
	for _, occurrence := range occurrences {
		require.Equal(t, error_tracking.TestFingerprintVersion, occurrence.FingerprintVersion)
		require.Equal(t, "test_xml", occurrence.FingerprintSource)
		require.Equal(t, "high", occurrence.FingerprintConfidence)
		require.Equal(t, "checkout", occurrence.TestSuite)
		require.Equal(t, "CardTest", occurrence.TestClass)
		require.Equal(t, int32(2), occurrence.TestRun)
		require.Equal(t, int32(3), occurrence.TestShard)
		require.Equal(t, int32(4), occurrence.TestAttempt)
		require.True(t, occurrence.TestCachedLocally)
		require.True(t, occurrence.TestCachedRemotely)
		require.Equal(t, "remote", occurrence.TestStrategy)
		require.NotContains(t, occurrence.Message, "secret")
		require.Less(t, occurrence.EventTimeUsec, time.Now().Add(time.Hour).UnixMicro())
	}
}

func TestBESTestAttemptBudgetPreservesLaterDistinctFailure(t *testing.T) {
	enableErrorTracking(t)
	const attemptBudget = 100
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	flakyXML := &bspb.File{Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(`<testsuite><testcase name="retry"><failure message="not ready"/></testcase></testsuite>`)}}
	for attempt := int32(1); attempt <= attemptBudget; attempt++ {
		require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, "//pkg:flaky_test", "cfg-flaky", 1, 0, attempt, bspb.TestStatus_FAILED, flakyXML), testInvocationID, int64(attempt)+1)))
	}
	terminalXML := &bspb.File{Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(`<testsuite><testcase name="terminal"><failure message="terminal root"/></testcase></testsuite>`)}}
	sequence := int64(attemptBudget) + 2
	require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, "//pkg:terminal_test", "cfg-terminal", 1, 0, 1, bspb.TestStatus_FAILED, terminalXML), testInvocationID, sequence)))
	require.NoError(t, channel.HandleEvent(streamRequest(testSummaryEvent(t, "//pkg:flaky_test", "cfg-flaky", bspb.TestStatus_FLAKY), testInvocationID, sequence+1)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, sequence+2)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 1 }, 30*time.Second, 50*time.Millisecond)
	require.Equal(t, "//pkg:terminal_test", olapDB.GetErrorOccurrences()[0].TargetLabel)
}

func TestBESTestFinalFlakySummarySuppressesFailedAttempts(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	xml := `<testsuite name="suite"><testcase classname="C" name="flaky"><failure message="first attempt failed"/></testcase></testsuite>`
	require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, "//pkg:flaky_test", "cfg-a", 1, 0, 1, bspb.TestStatus_FAILED,
		&bspb.File{Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(xml)}}), testInvocationID, 2)))
	terminalXML := `<testsuite name="suite"><testcase classname="C" name="terminal"><failure message="terminal failure"/></testcase></testsuite>`
	require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, "//pkg:flaky_test", "cfg-b", 1, 0, 1, bspb.TestStatus_FAILED,
		&bspb.File{Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(terminalXML)}}), testInvocationID, 3)))
	require.NoError(t, channel.HandleEvent(streamRequest(testSummaryEvent(t, "//pkg:flaky_test", "cfg-a", bspb.TestStatus_FLAKY), testInvocationID, 4)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 5)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 1 }, 30*time.Second, 50*time.Millisecond)
	require.Equal(t, "terminal", olapDB.GetErrorOccurrences()[0].TestName)
}

func TestBESTestArtifactFailuresUseTargetScopedFallback(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	artifacts := map[string]*bspb.File{
		"//pkg:malformed_test": {Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(`<testsuite><testcase>`)}},
		"//pkg:oversized_test": {Name: "test.xml", File: &bspb.File_Contents{Contents: []byte(strings.Repeat("x", (1<<20)+1))}},
		"//pkg:foreign_test":   {Name: "test.xml", File: &bspb.File_Uri{Uri: "bytestream://attacker.invalid/blobs/deadbeef/123"}},
	}
	sequenceNumber := int64(2)
	for target, testXML := range artifacts {
		testLog := &bspb.File{Name: "test.log", File: &bspb.File_Contents{Contents: []byte("runner failed; https://user:secret@example.com/log")}}
		require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, target, "cfg-a", 1, 0, 1, bspb.TestStatus_FAILED, testXML, testLog), testInvocationID, sequenceNumber)))
		sequenceNumber++
	}
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, sequenceNumber)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 3 }, 30*time.Second, 50*time.Millisecond)
	fingerprints := make(map[string]struct{})
	for _, occurrence := range olapDB.GetErrorOccurrences() {
		require.Equal(t, error_tracking.TestFallbackFingerprintVersion, occurrence.FingerprintVersion)
		require.Equal(t, "test_result_fallback", occurrence.FingerprintSource)
		require.Equal(t, "low", occurrence.FingerprintConfidence)
		require.NotContains(t, occurrence.Message, "secret")
		fingerprints[occurrence.Fingerprint] = struct{}{}
	}
	require.Len(t, fingerprints, 3)
}

type blockingPooledByteStreamClient struct{}

func (*blockingPooledByteStreamClient) StreamBytestreamFile(ctx context.Context, _ *url.URL, _ io.Writer) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*blockingPooledByteStreamClient) StreamBytestreamFileChunk(ctx context.Context, _ *url.URL, _, _ int64, _ io.Writer) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*blockingPooledByteStreamClient) FetchBytestreamZipManifest(context.Context, *url.URL) (*zipb.Manifest, error) {
	return nil, nil
}

func (*blockingPooledByteStreamClient) StreamSingleFileFromBytestreamZip(context.Context, *url.URL, *zipb.ManifestEntry, io.Writer) error {
	return nil
}

type controlledPooledByteStreamClient struct {
	started  chan struct{}
	release  chan struct{}
	contents []byte
	once     sync.Once
}

func (c *controlledPooledByteStreamClient) StreamBytestreamFile(ctx context.Context, _ *url.URL, w io.Writer) error {
	return c.StreamBytestreamFileChunk(ctx, nil, 0, 0, w)
}

func (c *controlledPooledByteStreamClient) StreamBytestreamFileChunk(ctx context.Context, _ *url.URL, _, _ int64, w io.Writer) error {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		_, err := w.Write(c.contents)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*controlledPooledByteStreamClient) FetchBytestreamZipManifest(context.Context, *url.URL) (*zipb.Manifest, error) {
	return nil, nil
}

func (*controlledPooledByteStreamClient) StreamSingleFileFromBytestreamZip(context.Context, *url.URL, *zipb.ManifestEntry, io.Writer) error {
	return nil
}

func TestBESTestArtifactFetchTimeoutFallsBack(t *testing.T) {
	enableErrorTracking(t)
	cacheURL, err := url.Parse("grpc://test.invalid:1985")
	require.NoError(t, err)
	flags.Set(t, "app.cache_api_url", *cacheURL)
	te := testenv.GetTestEnv(t)
	te.SetPooledByteStreamClient(&blockingPooledByteStreamClient{})
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	testXML := &bspb.File{Name: "test.xml", File: &bspb.File_Uri{Uri: "bytestream://test.invalid:1985/blobs/deadbeef/123"}}
	testLog := &bspb.File{Name: "test.log", File: &bspb.File_Contents{Contents: []byte("runner timed out")}}
	require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, "//pkg:timeout_test", "cfg-a", 1, 0, 1, bspb.TestStatus_TIMEOUT, testXML, testLog), testInvocationID, 2)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 3)))

	started := time.Now()
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))
	require.Less(t, time.Since(started), time.Second, "artifact fetches must not delay BES finalization acknowledgements")
	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 1 }, 30*time.Second, 50*time.Millisecond)
	occurrence := olapDB.GetErrorOccurrences()[0]
	require.Equal(t, error_tracking.TestFallbackFingerprintVersion, occurrence.FingerprintVersion)
	require.Equal(t, "runner timed out", occurrence.Message)
}

func testResultEvent(t *testing.T, target, configurationID string, run, shard, attempt int32, status bspb.TestStatus, outputs ...*bspb.File) *anypb.Any {
	t.Helper()
	event := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_TestResult{TestResult: &bspb.BuildEventId_TestResultId{
			Label: target, Configuration: &bspb.BuildEventId_ConfigurationId{Id: configurationID}, Run: run, Shard: shard, Attempt: attempt,
		}}},
		Payload: &bspb.BuildEvent_TestResult{TestResult: &bspb.TestResult{
			Status: status, StatusDetails: "test runner failed", TestActionOutput: outputs, CachedLocally: true,
			ExecutionInfo: &bspb.TestResult_ExecutionInfo{ExitCode: 1, CachedRemotely: true, Strategy: "remote"},
		}},
	}
	result := &anypb.Any{}
	require.NoError(t, result.MarshalFrom(event))
	return result
}

func testSummaryEvent(t *testing.T, target, configurationID string, status bspb.TestStatus) *anypb.Any {
	t.Helper()
	event := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_TestSummary{TestSummary: &bspb.BuildEventId_TestSummaryId{
			Label: target, Configuration: &bspb.BuildEventId_ConfigurationId{Id: configurationID},
		}}},
		Payload: &bspb.BuildEvent_TestSummary{TestSummary: &bspb.TestSummary{OverallStatus: status}},
	}
	result := &anypb.Any{}
	require.NoError(t, result.MarshalFrom(event))
	return result
}

func TestBESErrorCandidateBufferKeepsLaterDistinctFailure(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)
	testInvocationID := strings.ToUpper(uuid.New().String())
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	makeFailedAction := func(label, message string) *anypb.Any {
		event := &bspb.BuildEvent{
			Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_ActionCompleted{ActionCompleted: &bspb.BuildEventId_ActionCompletedId{Label: label}}},
			Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{
				Success: false, Type: "GoCompilePkg", ExitCode: 1,
				Stderr:        &bspb.File{Name: "stderr", File: &bspb.File_Contents{Contents: []byte(message)}},
				FailureDetail: &fdpb.FailureDetail{Message: message, Category: &fdpb.FailureDetail_Spawn{Spawn: &fdpb.Spawn{Code: fdpb.Spawn_NON_ZERO_EXIT}}},
			}},
		}
		result := &anypb.Any{}
		require.NoError(t, result.MarshalFrom(event))
		return result
	}
	duplicate := makeFailedAction("//pkg:duplicate", "pkg/duplicate.go:10:2: undefined: repeated")
	for sequenceNumber := int64(2); sequenceNumber <= int64(error_tracking.MaxOccurrencesPerInvocation)+1; sequenceNumber++ {
		require.NoError(t, channel.HandleEvent(streamRequest(duplicate, testInvocationID, sequenceNumber)))
	}
	distinctSequenceNumber := int64(error_tracking.MaxOccurrencesPerInvocation) + 2
	require.NoError(t, channel.HandleEvent(streamRequest(makeFailedAction("//pkg:distinct", "pkg/distinct.go:20:4: undefined: laterRoot"), testInvocationID, distinctSequenceNumber)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, distinctSequenceNumber+1)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 2 }, 30*time.Second, 50*time.Millisecond)
	messages := []string{olapDB.GetErrorOccurrences()[0].Message, olapDB.GetErrorOccurrences()[1].Message}
	require.ElementsMatch(t, []string{"pkg/duplicate.go:10:2: undefined: repeated", "pkg/distinct.go:20:4: undefined: laterRoot"}, messages)
}

func TestBESErrorFlushWaitsForACLSync(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)
	require.NoError(t, te.GetDBHandle().GORM(ctx, "insert_error_tracking_group").Create(&tables.Group{
		GroupID: "GROUP1", UserID: "USER1", SharingEnabled: true,
	}).Error)

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var syncCalls atomic.Int32
	olapDB := testolapdb.NewHandle()
	olapDB.SetBeforeErrorACLUpdate(func() {
		if syncCalls.Add(1) == 1 {
			close(syncStarted)
			<-releaseSync
		}
	})
	te.SetOLAPDBHandle(olapDB)

	testInvocationID := uuid.New().String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	failedAction := &bspb.BuildEvent{
		Id:      &bspb.BuildEventId{Id: &bspb.BuildEventId_ActionCompleted{ActionCompleted: &bspb.BuildEventId_ActionCompletedId{Label: "//pkg:target"}}},
		Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{Success: false, Type: "GoCompilePkg", ExitCode: 1}},
	}
	failedActionAny := &anypb.Any{}
	require.NoError(t, failedActionAny.MarshalFrom(failedAction))
	require.NoError(t, channel.HandleEvent(streamRequest(failedActionAny, testInvocationID, 2)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 3)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))
	select {
	case <-syncStarted:
	case <-time.After(30 * time.Second):
		close(releaseSync)
		require.FailNow(t, "timed out waiting for error ACL synchronization")
	}

	require.Empty(t, olapDB.GetErrorOccurrences())

	user, err := auth.AuthenticatedUser(ctx)
	require.NoError(t, err)
	updateDone := make(chan error, 1)
	go func() {
		ownerOnly := perms.ToACLProto(&uidpb.UserId{Id: "USER1"}, "GROUP1", perms.OWNER_READ|perms.OWNER_WRITE)
		updateDone <- te.GetInvocationDB().UpdateInvocationACL(ctx, &user, testInvocationID, ownerOnly)
	}()
	select {
	case err := <-updateDone:
		require.Failf(t, "ACL update raced past the serialized visibility grant", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseSync)
	require.NoError(t, <-updateDone)
	require.Eventually(t, func() bool {
		acl := olapDB.GetErrorInvocationACL(testInvocationID)
		return len(olapDB.GetErrorOccurrences()) == 1 && acl != nil && acl.Perms == perms.OWNER_READ|perms.OWNER_WRITE
	}, 30*time.Second, 50*time.Millisecond)
}

func TestBESErrorFlushSerializesConcurrentACLUpdateOnSQLite(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)
	require.NoError(t, te.GetDBHandle().GORM(ctx, "insert_error_tracking_group").Create(&tables.Group{
		GroupID: "GROUP1", UserID: "USER1", SharingEnabled: true,
	}).Error)

	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	olapDB := testolapdb.NewHandle()
	olapDB.SetBeforeErrorFlush(func() {
		close(flushStarted)
		<-releaseFlush
	})
	te.SetOLAPDBHandle(olapDB)

	testInvocationID := uuid.New().String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	failedAction := &bspb.BuildEvent{
		Id:      &bspb.BuildEventId{Id: &bspb.BuildEventId_ActionCompleted{ActionCompleted: &bspb.BuildEventId_ActionCompletedId{Label: "//pkg:target"}}},
		Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{Success: false, Type: "GoCompilePkg", ExitCode: 1}},
	}
	failedActionAny := &anypb.Any{}
	require.NoError(t, failedActionAny.MarshalFrom(failedAction))
	require.NoError(t, channel.HandleEvent(streamRequest(failedActionAny, testInvocationID, 2)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 3)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))
	select {
	case <-flushStarted:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for error flush")
	}

	user, err := auth.AuthenticatedUser(ctx)
	require.NoError(t, err)
	updateDone := make(chan error, 1)
	updateStarted := make(chan struct{})
	go func() {
		close(updateStarted)
		ownerOnly := perms.ToACLProto(&uidpb.UserId{Id: "USER1"}, "GROUP1", perms.OWNER_READ|perms.OWNER_WRITE)
		updateDone <- te.GetInvocationDB().UpdateInvocationACL(ctx, &user, testInvocationID, ownerOnly)
	}()
	<-updateStarted
	select {
	case err := <-updateDone:
		require.Failf(t, "ACL update raced past the serialized occurrence insert", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFlush)
	require.NoError(t, <-updateDone)
	require.Eventually(t, func() bool {
		acl := olapDB.GetErrorInvocationACL(testInvocationID)
		return len(olapDB.GetErrorOccurrences()) == 1 && acl != nil && acl.Perms == perms.OWNER_READ|perms.OWNER_WRITE
	}, 30*time.Second, 50*time.Millisecond)
}

func TestBESErrorFlushRejectsStaleTaskAfterInvocationIDReuse(t *testing.T) {
	enableErrorTracking(t)
	cacheURL, err := url.Parse("grpc://test.invalid:1985")
	require.NoError(t, err)
	flags.Set(t, "app.cache_api_url", *cacheURL)

	te := testenv.GetTestEnv(t)
	fixedNow := time.Unix(1_800_000_000, 123_000)
	te.GetDBHandle().SetNowFunc(func() time.Time { return fixedNow })
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1", "USER2", "GROUP2"))
	te.SetAuthenticator(auth)
	artifactClient := &controlledPooledByteStreamClient{
		started: make(chan struct{}), release: make(chan struct{}),
		contents: []byte(`<testsuite><testcase name="old failure"><failure message="stale diagnostic"/></testcase></testsuite>`),
	}
	te.SetPooledByteStreamClient(artifactClient)
	olapDB := testolapdb.NewHandle()
	te.SetOLAPDBHandle(olapDB)

	testInvocationID := uuid.New().String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	testXML := &bspb.File{Name: "test.xml", File: &bspb.File_Uri{Uri: "bytestream://test.invalid:1985/blobs/deadbeef/123"}}
	require.NoError(t, channel.HandleEvent(streamRequest(testResultEvent(t, "//pkg:old_test", "cfg", 1, 0, 1, bspb.TestStatus_FAILED, testXML), testInvocationID, 2)))
	require.NoError(t, channel.HandleEvent(streamRequest(testSummaryEvent(t, "//pkg:old_test", "cfg", bspb.TestStatus_FAILED), testInvocationID, 3)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 4)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))

	select {
	case <-artifactClient.started:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for the queued artifact loader")
	}
	oldCtx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)
	oldInvocation, err := te.GetInvocationDB().LookupInvocation(oldCtx, testInvocationID)
	require.NoError(t, err)
	require.NoError(t, te.GetInvocationDB().DeleteInvocation(oldCtx, testInvocationID))
	newCtx, err := auth.WithAuthenticatedUser(context.Background(), "USER2")
	require.NoError(t, err)
	replacement := &tables.Invocation{InvocationID: testInvocationID}
	created, err := te.GetInvocationDB().CreateInvocation(newCtx, replacement)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, oldInvocation.CreatedAtUsec, replacement.CreatedAtUsec)
	require.NotEqual(t, oldInvocation.ErrorTrackingIncarnation, replacement.ErrorTrackingIncarnation)
	close(artifactClient.release)

	require.Never(t, func() bool { return olapDB.GetFlushedInvocation(testInvocationID) != nil }, 500*time.Millisecond, 50*time.Millisecond)
	require.Empty(t, olapDB.GetErrorOccurrences())
	require.Nil(t, olapDB.GetErrorInvocationACL(testInvocationID))
}

func TestBESLiveChannelRejectsUpdatesAfterInvocationIDReuse(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1", "USER2", "GROUP2"))
	te.SetAuthenticator(auth)
	testInvocationID := uuid.New().String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_BuildMetadata{}), testInvocationID, 1)))
	oldCtx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)
	require.NoError(t, te.GetInvocationDB().DeleteInvocation(oldCtx, testInvocationID))
	newCtx, err := auth.WithAuthenticatedUser(context.Background(), "USER2")
	require.NoError(t, err)
	replacement := &tables.Invocation{InvocationID: testInvocationID, Pattern: "//replacement"}
	created, err := te.GetInvocationDB().CreateInvocation(newCtx, replacement)
	require.NoError(t, err)
	require.True(t, created)

	err = channel.HandleEvent(streamRequest(buildMetadataEvent(map[string]string{
		"PATTERN":    "//stale",
		"REPO_URL":   "https://example.com/old/private.git",
		"VISIBILITY": "PUBLIC",
	}), testInvocationID, 2))
	require.Error(t, err)
	require.True(t, status.IsCanceledError(err))

	var got tables.Invocation
	require.NoError(t, te.GetDBHandle().NewQuery(context.Background(), "get_replacement_after_stale_live_update").Raw(
		`SELECT pattern, repo_url, perms, error_tracking_incarnation FROM "Invocations" WHERE invocation_id = ?`, testInvocationID,
	).Take(&got))
	require.Equal(t, "//replacement", got.Pattern)
	require.Empty(t, got.RepoURL)
	require.Zero(t, got.Perms&perms.OTHERS_READ)
	require.Equal(t, replacement.ErrorTrackingIncarnation, got.ErrorTrackingIncarnation)
}

func TestBESErrorAmbiguousOLAPWriteStillTombstonesOnDelete(t *testing.T) {
	enableErrorTracking(t)
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	olapDB := testolapdb.NewHandle()
	olapDB.SetErrorFlushError(errors.New("ambiguous insert timeout"))
	te.SetOLAPDBHandle(olapDB)

	testInvocationID := uuid.New().String()
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	require.NoError(t, channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)))
	failedAction := &bspb.BuildEvent{
		Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_ActionCompleted{ActionCompleted: &bspb.BuildEventId_ActionCompletedId{Label: "//pkg:target"}}},
		Payload: &bspb.BuildEvent_Action{Action: &bspb.ActionExecuted{
			Success: false, Type: "GoCompilePkg", ExitCode: 1,
			Stderr: &bspb.File{Name: "stderr", File: &bspb.File_Contents{Contents: []byte("pkg/file.go:12:3: undefined: missing")}},
		}},
	}
	failedActionAny := &anypb.Any{}
	require.NoError(t, failedActionAny.MarshalFrom(failedAction))
	require.NoError(t, channel.HandleEvent(streamRequest(failedActionAny, testInvocationID, 2)))
	require.NoError(t, channel.HandleEvent(streamRequest(finishedEvent(), testInvocationID, 3)))
	require.NoError(t, channel.FinalizeInvocation(testInvocationID))
	require.Eventually(t, func() bool { return len(olapDB.GetErrorOccurrences()) == 1 }, 30*time.Second, 50*time.Millisecond)

	ctx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)
	invocation, err := te.GetInvocationDB().LookupInvocation(ctx, testInvocationID)
	require.NoError(t, err)
	require.Equal(t, error_tracking.ErrorOccurrencesPresent, invocation.ErrorOccurrencesState)
	require.NoError(t, te.GetInvocationDB().DeleteInvocation(ctx, testInvocationID))
	acl := olapDB.GetErrorInvocationACL(testInvocationID)
	require.NotNil(t, acl)
	require.True(t, acl.Deleted)
	require.Zero(t, acl.Perms)
}

func TestUnfinishedFinalizeWithCanceledContext(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Cancel the context
	cancel()

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)
}

// failingBlobstore wraps a Blobstore and fails all blob writes when
// failWrites is set.
type failingBlobstore struct {
	interfaces.Blobstore
	failWrites atomic.Bool
}

func (b *failingBlobstore) WriteBlob(ctx context.Context, blobName string, data []byte) (int, error) {
	if b.failWrites.Load() {
		return 0, fmt.Errorf("blobstore write failed")
	}
	return b.Blobstore.WriteBlob(ctx, blobName, data)
}

func TestUnfinishedFinalizeWithBlobstoreWriteFailure(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	bs := &failingBlobstore{Blobstore: te.GetBlobstore()}
	te.SetBlobstore(bs)
	ctx := t.Context()
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key. The event is buffered in the
	// invocation event stream and not yet flushed to blobstore.
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make blobstore writes fail, then finalize the invocation without a
	// finished event, as happens when the client disconnects. Flushing the
	// buffered events fails, but there is no connected client left to receive
	// an error and retry, so finalization should proceed anyway.
	bs.failWrites.Store(true)
	err = channel.FinalizeInvocation(testInvocationID)
	require.NoError(t, err)

	// Make sure the invocation was marked disconnected in the DB so that
	// bazel may retry it.
	authCtx := auth.AuthContextFromAPIKey(t.Context(), "USER1")
	ti, err := te.GetInvocationDB().LookupInvocation(authCtx, testInvocationID)
	require.NoError(t, err)
	assert.Equal(t, int64(inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS), ti.InvocationStatus)
}

func TestFinishedFinalizeWithBlobstoreWriteFailure(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	bs := &failingBlobstore{Blobstore: te.GetBlobstore()}
	te.SetBlobstore(bs)
	ctx := t.Context()
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key. The event is buffered in the
	// invocation event stream and not yet flushed to blobstore.
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Send finished event, so that the invocation finalizes as complete
	// rather than disconnected.
	request = streamRequest(finishedEvent(), testInvocationID, 2)
	err = channel.HandleEvent(request)
	assert.NoError(t, err)

	// Make blobstore writes fail, then finalize the invocation. The client is
	// still connected, so the error from flushing the buffered events should
	// be returned, which lets the client retry sending the events.
	bs.failWrites.Store(true)
	err = channel.FinalizeInvocation(testInvocationID)
	require.Error(t, err)

	// Make sure the invocation was not finalized in the DB.
	authCtx := auth.AuthContextFromAPIKey(t.Context(), "USER1")
	ti, err := te.GetInvocationDB().LookupInvocation(authCtx, testInvocationID)
	require.NoError(t, err)
	assert.Equal(t, int64(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS), ti.InvocationStatus)
}

func TestUnfinishedFinalize(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx, cancel := context.WithCancel(context.Background())
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)
	cancel()

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(context.Background(), "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)
}

func TestPeriodicInvocationRowUpdateWhileStreaming(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	clock := clockwork.NewFakeClock()
	te.SetClock(clock)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	// Make DB writes stamp a fixed time so that we can tell when the
	// invocation row gets written.
	t0 := time.Unix(1000, 0)
	te.GetInvocationDB().SetNowFunc(func() time.Time { return t0 })

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key, which creates the invocation row.
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
	err = channel.HandleEvent(request)
	require.NoError(t, err)

	authCtx := auth.AuthContextFromAPIKey(context.Background(), "USER1")
	ti, err := te.GetInvocationDB().LookupInvocation(authCtx, testInvocationID)
	require.NoError(t, err)
	require.Equal(t, t0.UnixMicro(), ti.UpdatedAtUsec)

	// Advance the DB clock. An event that arrives before the periodic update
	// period has elapsed should not update the invocation row.
	t1 := time.Unix(2000, 0)
	te.GetInvocationDB().SetNowFunc(func() time.Time { return t1 })
	request = streamRequest(progressEventWithOutput("hello", ""), testInvocationID, 2)
	err = channel.HandleEvent(request)
	require.NoError(t, err)

	ti, err = te.GetInvocationDB().LookupInvocation(authCtx, testInvocationID)
	require.NoError(t, err)
	assert.Equal(t, t0.UnixMicro(), ti.UpdatedAtUsec)

	// Advance the stream's clock past half the reconnect window. The next
	// event should update the invocation row, keeping the invocation
	// retryable in case it gets disconnected later.
	clock.Advance(te.GetInvocationDB().GetInvocationReconnectWindow()/2 + time.Second)
	request = streamRequest(progressEventWithOutput("world", ""), testInvocationID, 3)
	err = channel.HandleEvent(request)
	require.NoError(t, err)

	ti, err = te.GetInvocationDB().LookupInvocation(authCtx, testInvocationID)
	require.NoError(t, err)
	assert.Equal(t, t1.UnixMicro(), ti.UpdatedAtUsec)
}

func TestRetryOnComplete(t *testing.T) {
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel, err = handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'"), testInvocationID, 1)
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
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel, err = handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

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
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel, err = handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "def456", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err = te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 2), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 2))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Attempt to start a new invocation with the same id
	channel, err = handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "000789", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

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
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(auth)
	ctx := context.Background()
	testUUID, err := uuid.NewRandom()
	assert.NoError(t, err)
	testInvocationID := testUUID.String()
	chunkSize := 128
	flags.Set(t, "storage.chunk_file_size_bytes", chunkSize)

	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()

	// Say that it occurred 5 hours ago
	te.GetInvocationDB().SetNowFunc(func() time.Time {
		return time.Now().Add(-5 * time.Hour)
	})

	// Send started event with api key
	request := streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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
	assert.Equal(t, inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS, invocation.InvocationStatus)

	// Finalize the invocation
	err = channel.FinalizeInvocation(testInvocationID)
	assert.NoError(t, err)

	// Make sure it gets finalized properly
	invocation, err = build_event_handler.LookupInvocation(te, auth.AuthContextFromAPIKey(ctx, "USER1"), testInvocationID)
	assert.NoError(t, err)
	assert.Equal(t, "abc123", invocation.CommitSha)
	assert.Equal(t, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS, invocation.InvocationStatus)

	exists, err := te.GetBlobstore().BlobExists(ctx, protofile.ChunkName(build_event_handler.GetStreamIdFromInvocationIdAndAttempt(testInvocationID, 1), 0))
	assert.NoError(t, err)
	assert.True(t, exists)
	exists, err = chunkstore.New(te.GetBlobstore(), &chunkstore.ChunkstoreOptions{}).BlobExists(ctx, eventlog.GetEventLogPathFromInvocationIdAndAttempt(testInvocationID, 1))
	assert.NoError(t, err)
	assert.True(t, exists)

	// Reset the time for the database
	te.GetInvocationDB().SetNowFunc(time.Now)

	// Attempt to start a new invocation with the same id
	channel, err = handler.OpenChannel(ctx, testInvocationID)
	require.NoError(t, err)
	defer channel.Close()
	request = streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=USER1'", &bspb.BuildEventId_WorkspaceStatus{}), testInvocationID, 1)
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

func TestBuildStatusReporting(t *testing.T) {
	for _, test := range []struct {
		name           string
		metadataEvents []*bspb.BuildEvent
		statusContext  string
	}{
		{
			name:          "BuildMetadataThenWorkspaceStatus",
			statusContext: "bazel build //...",
			metadataEvents: []*bspb.BuildEvent{
				&bspb.BuildEvent{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_Pattern{Pattern: &bspb.BuildEventId_PatternExpandedId{
						Pattern: []string{"//..."},
					}}},
				},
				&bspb.BuildEvent{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildMetadata{}},
					Payload: &bspb.BuildEvent_BuildMetadata{BuildMetadata: &bspb.BuildMetadata{
						// Status reporting is only enabled for CI builds.
						Metadata: map[string]string{"ROLE": "CI"},
					}},
				},
				&bspb.BuildEvent{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_WorkspaceStatus{}},
					Payload: &bspb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bspb.WorkspaceStatus{
						Item: []*bspb.WorkspaceStatus_Item{
							{Key: "REPO_URL", Value: "https://github.com/testowner/testrepo.git"},
							{Key: "COMMIT_SHA", Value: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2"},
						},
					}},
				},
			},
		},
		{
			name:          "WorkspaceStatusThenBuildMetadataWithCommitStatusLabel",
			statusContext: "Build and test",
			metadataEvents: []*bspb.BuildEvent{
				&bspb.BuildEvent{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_Pattern{Pattern: &bspb.BuildEventId_PatternExpandedId{
						Pattern: []string{"//..."},
					}}},
				},
				&bspb.BuildEvent{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_WorkspaceStatus{}},
					Payload: &bspb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bspb.WorkspaceStatus{
						Item: []*bspb.WorkspaceStatus_Item{
							{Key: "REPO_URL", Value: "https://github.com/testowner/testrepo.git"},
							{Key: "COMMIT_SHA", Value: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2"},
						},
					}},
				},
				&bspb.BuildEvent{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildMetadata{}},
					Payload: &bspb.BuildEvent_BuildMetadata{BuildMetadata: &bspb.BuildMetadata{
						// Status reporting is only enabled for CI builds.
						Metadata: map[string]string{
							"ROLE":                "CI",
							"COMMIT_STATUS_LABEL": "Build and test",
						},
					}},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			fakeGH := &FakeGitHubStatusService{StatusReportingEnabled: true}
			te.SetGitHubStatusService(fakeGH)
			auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
			te.SetAuthenticator(auth)
			ctx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
			require.NoError(t, err)
			handler := build_event_handler.NewBuildEventHandler(te)

			// Initialize a github app installation to report statuses for.
			dbh := te.GetDBHandle()
			require.NotNil(t, dbh)
			gh := &tables.GitHubAppInstallation{
				GroupID:                         "GROUP1",
				Owner:                           "testowner",
				ReportCommitStatusesForCIBuilds: true,
			}
			err = dbh.NewQuery(context.Background(), "create_github_app_installation_for_test").Create(gh)
			require.NoError(t, err)

			// Start an invocation
			seq := NewBESSequence(t)
			channel, err := handler.OpenChannel(ctx, seq.InvocationID)
			require.NoError(t, err)
			defer channel.Close()

			// Handle Started event referencing the metadata events as children.
			var metadataEventIDs []*bspb.BuildEventId
			for _, e := range test.metadataEvents {
				metadataEventIDs = append(metadataEventIDs, e.GetId())
			}
			started := &bspb.BuildEvent{
				Id:       &bspb.BuildEventId{Id: &bspb.BuildEventId_Started{}},
				Children: metadataEventIDs,
				Payload: &bspb.BuildEvent_Started{Started: &bspb.BuildStarted{
					Command: "build",
					// TODO: the test fails unless OptionsDescription is set,
					// which seems error-prone.
					OptionsDescription: "--some_build_options",
				}},
			}
			err = channel.HandleEvent(seq.NextRequest(started))
			require.NoError(t, err)

			// Should not have reported any statuses yet, since we haven't
			// handled any metadata events.
			require.True(t, fakeGH.HasNoStatuses())

			// Handle *all but the last* metadata event - no statuses should be
			// reported yet. We should only report a status once *all* of the
			// metadata events declared in the Started event have been handled.
			md := test.metadataEvents
			for len(md) > 1 {
				event := md[0]
				md = md[1:]
				err := channel.HandleEvent(seq.NextRequest(event))
				require.NoError(t, err)
				require.True(t, fakeGH.HasNoStatuses())
			}

			// Now handle the last metadata event - should report a status,
			// since all metadata events have been handled.
			err = channel.HandleEvent(seq.NextRequest(md[0]))
			require.NoError(t, err)
			require.Equal(t, 1, len(fakeGH.Clients))
			client := fakeGH.GetCreatedClient(t)
			require.Equal(t, []*FakeGitHubStatus{
				{
					OwnerRepo: "testowner/testrepo",
					CommitSHA: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2",
					RepoStatus: &github.GithubStatusPayload{
						TargetURL:   pointer("http://localhost:8080/invocation/" + seq.InvocationID),
						State:       pointer("pending"),
						Description: pointer("Running..."),
						Context:     pointer(test.statusContext),
					},
				},
			}, client.ConsumeStatuses())

			// Handle the Finished event - should report another status.
			fin := &bspb.BuildEvent{
				Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildFinished{}},
				Payload: &bspb.BuildEvent_Finished{Finished: &bspb.BuildFinished{
					ExitCode: &bspb.BuildFinished_ExitCode{
						Name: "SUCCESS",
						Code: 0,
					},
				}},
			}
			err = channel.HandleEvent(seq.NextRequest(fin))
			require.NoError(t, err)
			require.Equal(t, []*FakeGitHubStatus{
				{
					OwnerRepo: "testowner/testrepo",
					CommitSHA: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2",
					RepoStatus: &github.GithubStatusPayload{
						TargetURL:   pointer("http://localhost:8080/invocation/" + seq.InvocationID),
						State:       pointer("success"),
						Description: pointer("Success"),
						Context:     pointer(test.statusContext),
					},
				},
			}, client.ConsumeStatuses())
		})
	}
}

func TestBuildStatusReportingDisabled(t *testing.T) {
	for _, test := range []struct {
		name                     string
		enableReportingForRepo   bool
		role                     string
		disableReportingForBuild string
	}{
		{
			name:                     "status reporting disabled for the repo",
			enableReportingForRepo:   false,
			role:                     "CI",
			disableReportingForBuild: "false",
		},
		{
			name:                     "status reporting disabled for the build",
			enableReportingForRepo:   true,
			role:                     "CI",
			disableReportingForBuild: "true",
		},
		{
			name:                     "not CI build",
			enableReportingForRepo:   true,
			role:                     "default",
			disableReportingForBuild: "false",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			fakeGH := &FakeGitHubStatusService{StatusReportingEnabled: test.enableReportingForRepo}
			te.SetGitHubStatusService(fakeGH)
			auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
			te.SetAuthenticator(auth)
			ctx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
			require.NoError(t, err)
			handler := build_event_handler.NewBuildEventHandler(te)

			// Initialize a git repo to report statuses for.
			dbh := te.GetDBHandle()
			require.NotNil(t, dbh)
			gh := &tables.GitHubAppInstallation{
				GroupID: "GROUP1",
				Owner:   "testowner",
			}
			err = dbh.NewQuery(context.Background(), "create_github_app_installation_for_test").Create(gh)
			require.NoError(t, err)
			// Gorm `Create` will ignore the value of `report_commit_statuses_for_ci_builds`
			// if it is set to false in the struct. To override its default value of true,
			// you have to explicitly update the value of the field.
			rsp := dbh.NewQuery(context.Background(), "create_github_app_installation_for_test").Raw(`UPDATE "GitHubAppInstallations" SET report_commit_statuses_for_ci_builds = ?`, test.enableReportingForRepo).Exec()
			require.NoError(t, rsp.Error)

			buildEvents := []*bspb.BuildEvent{
				{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_Pattern{Pattern: &bspb.BuildEventId_PatternExpandedId{
						Pattern: []string{"//..."},
					}}},
				},
				{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildMetadata{}},
					Payload: &bspb.BuildEvent_BuildMetadata{BuildMetadata: &bspb.BuildMetadata{
						Metadata: map[string]string{
							"ROLE":                            test.role,
							"DISABLE_COMMIT_STATUS_REPORTING": test.disableReportingForBuild,
						},
					}},
				},
				{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_WorkspaceStatus{}},
					Payload: &bspb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bspb.WorkspaceStatus{
						Item: []*bspb.WorkspaceStatus_Item{
							{Key: "REPO_URL", Value: "https://github.com/testowner/testrepo.git"},
							{Key: "COMMIT_SHA", Value: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2"},
						},
					}},
				},
			}

			// Start an invocation
			seq := NewBESSequence(t)
			channel, err := handler.OpenChannel(ctx, seq.InvocationID)
			require.NoError(t, err)
			defer channel.Close()

			// Handle Started event referencing the metadata events as children.
			var metadataEventIDs []*bspb.BuildEventId
			for _, e := range buildEvents {
				metadataEventIDs = append(metadataEventIDs, e.GetId())
			}
			started := &bspb.BuildEvent{
				Id:       &bspb.BuildEventId{Id: &bspb.BuildEventId_Started{}},
				Children: metadataEventIDs,
				Payload: &bspb.BuildEvent_Started{Started: &bspb.BuildStarted{
					Command: "build",
					// TODO: the test fails unless OptionsDescription is set,
					// which seems error-prone.
					OptionsDescription: "--some_build_options",
				}},
			}
			err = channel.HandleEvent(seq.NextRequest(started))
			require.NoError(t, err)

			// Handle metadata events.
			for _, event := range buildEvents {
				err := channel.HandleEvent(seq.NextRequest(event))
				require.NoError(t, err)
				require.True(t, fakeGH.HasNoStatuses())
			}
			// No statuses should've been reported.
			require.True(t, fakeGH.HasNoStatuses())

			// Handle the Finished event - should not report a status.
			fin := &bspb.BuildEvent{
				Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildFinished{}},
				Payload: &bspb.BuildEvent_Finished{Finished: &bspb.BuildFinished{
					ExitCode: &bspb.BuildFinished_ExitCode{
						Name: "SUCCESS",
						Code: 0,
					},
				}},
			}
			err = channel.HandleEvent(seq.NextRequest(fin))
			require.NoError(t, err)
			require.True(t, fakeGH.HasNoStatuses())
		})
	}
}

func TestBuildStatusReporting_LegacyMethods(t *testing.T) {
	for _, test := range []struct {
		name                       string
		legacyWorkflow             bool
		legacyGroupLevelOauthToken bool
	}{
		{
			name:                       "Legacy workflow",
			legacyWorkflow:             true,
			legacyGroupLevelOauthToken: false,
		},
		{
			name:                       "Legacy group level oauth token",
			legacyWorkflow:             false,
			legacyGroupLevelOauthToken: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			fakeGH := &FakeGitHubStatusService{StatusReportingEnabled: true}
			te.SetGitHubStatusService(fakeGH)
			auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1"))
			te.SetAuthenticator(auth)
			ctx, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
			require.NoError(t, err)
			handler := build_event_handler.NewBuildEventHandler(te)

			dbh := te.GetDBHandle()
			require.NotNil(t, dbh)
			if test.legacyWorkflow {
				wf := &tables.Workflow{
					RepoURL: "https://github.com/testowner/testrepo",
				}
				err := dbh.NewQuery(context.Background(), "create_workflow_for_test").Create(wf)
				require.NoError(t, err)
			}
			if test.legacyGroupLevelOauthToken {
				token := "token"
				g := &tables.Group{
					GroupID:     "GROUP1",
					GithubToken: &token,
				}
				err := dbh.NewQuery(context.Background(), "create_group_for_test").Create(g)
				require.NoError(t, err)
			}

			buildEvents := []*bspb.BuildEvent{
				{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_Pattern{Pattern: &bspb.BuildEventId_PatternExpandedId{
						Pattern: []string{"//..."},
					}}},
				},
				{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildMetadata{}},
					Payload: &bspb.BuildEvent_BuildMetadata{BuildMetadata: &bspb.BuildMetadata{
						Metadata: map[string]string{"ROLE": "CI"},
					}},
				},
				{
					Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_WorkspaceStatus{}},
					Payload: &bspb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bspb.WorkspaceStatus{
						Item: []*bspb.WorkspaceStatus_Item{
							{Key: "REPO_URL", Value: "https://github.com/testowner/testrepo.git"},
							{Key: "COMMIT_SHA", Value: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2"},
						},
					}},
				},
			}

			// Start an invocation
			seq := NewBESSequence(t)
			channel, err := handler.OpenChannel(ctx, seq.InvocationID)
			require.NoError(t, err)
			defer channel.Close()

			// Handle Started event referencing the metadata events as children.
			var metadataEventIDs []*bspb.BuildEventId
			for _, e := range buildEvents {
				metadataEventIDs = append(metadataEventIDs, e.GetId())
			}
			started := &bspb.BuildEvent{
				Id:       &bspb.BuildEventId{Id: &bspb.BuildEventId_Started{}},
				Children: metadataEventIDs,
				Payload: &bspb.BuildEvent_Started{Started: &bspb.BuildStarted{
					Command: "build",
					// TODO: the test fails unless OptionsDescription is set,
					// which seems error-prone.
					OptionsDescription: "--some_build_options",
				}},
			}
			err = channel.HandleEvent(seq.NextRequest(started))
			require.NoError(t, err)

			// Should not have reported any statuses yet, since we haven't
			// handled any metadata events.
			require.True(t, fakeGH.HasNoStatuses())

			// Handle *all but the last* metadata event - no statuses should be
			// reported yet. We should only report a status once *all* of the
			// metadata events declared in the Started event have been handled.
			md := buildEvents
			for len(md) > 1 {
				event := md[0]
				md = md[1:]
				err := channel.HandleEvent(seq.NextRequest(event))
				require.NoError(t, err)
				require.True(t, fakeGH.HasNoStatuses())
			}

			// Now handle the last metadata event - should report a status,
			// since all metadata events have been handled.
			err = channel.HandleEvent(seq.NextRequest(md[0]))
			require.NoError(t, err)
			client := fakeGH.GetCreatedClient(t)
			require.Equal(t, []*FakeGitHubStatus{
				{
					OwnerRepo: "testowner/testrepo",
					CommitSHA: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2",
					RepoStatus: &github.GithubStatusPayload{
						TargetURL:   pointer("http://localhost:8080/invocation/" + seq.InvocationID),
						State:       pointer("pending"),
						Description: pointer("Running..."),
						Context:     pointer("bazel build //..."),
					},
				},
			}, client.ConsumeStatuses())

			// Handle the Finished event - should report another status.
			fin := &bspb.BuildEvent{
				Id: &bspb.BuildEventId{Id: &bspb.BuildEventId_BuildFinished{}},
				Payload: &bspb.BuildEvent_Finished{Finished: &bspb.BuildFinished{
					ExitCode: &bspb.BuildFinished_ExitCode{
						Name: "SUCCESS",
						Code: 0,
					},
				}},
			}
			err = channel.HandleEvent(seq.NextRequest(fin))
			require.NoError(t, err)
			require.Equal(t, []*FakeGitHubStatus{
				{
					OwnerRepo: "testowner/testrepo",
					CommitSHA: "0c894fe31c2e91d59cb1a59bb25aaa78089919c2",
					RepoStatus: &github.GithubStatusPayload{
						TargetURL:   pointer("http://localhost:8080/invocation/" + seq.InvocationID),
						State:       pointer("success"),
						Description: pointer("Success"),
						Context:     pointer("bazel build //..."),
					},
				},
			}, client.ConsumeStatuses())
		})
	}
}

func TestTruncateStringSlice(t *testing.T) {
	for _, test := range []struct {
		Strings   []string
		Limit     int
		Expected  []string
		Truncated bool
	}{
		{
			Strings:   nil,
			Limit:     0,
			Expected:  nil,
			Truncated: false,
		},
		{
			Strings:   []string{""},
			Limit:     0,
			Expected:  []string{""},
			Truncated: false,
		},
		{
			Strings:   []string{"a"},
			Limit:     0,
			Expected:  nil,
			Truncated: true,
		},
		{
			Strings:   []string{"ツ"}, // note: len("ツ") is 3
			Limit:     1,
			Expected:  nil,
			Truncated: true,
		},
		{
			Strings:   []string{"a"},
			Limit:     1,
			Expected:  []string{"a"},
			Truncated: false,
		},
		{
			Strings:   []string{"ab"},
			Limit:     1,
			Expected:  nil,
			Truncated: true,
		},
		{
			Strings:   []string{"a", "b"},
			Limit:     1,
			Expected:  []string{"a"},
			Truncated: true,
		},
		{
			Strings:   []string{"a", "b"},
			Limit:     2,
			Expected:  []string{"a"},
			Truncated: true,
		},
		{
			Strings:   []string{"a", "b"},
			Limit:     3,
			Expected:  []string{"a", "b"},
			Truncated: false,
		},
		{
			Strings:   []string{"a", "bc"},
			Limit:     3,
			Expected:  []string{"a"},
			Truncated: true,
		},
	} {
		t.Run(fmt.Sprintf("%s/%d", test.Strings, test.Limit), func(t *testing.T) {
			out, truncated := build_event_handler.TruncateStringSlice(test.Strings, test.Limit)

			if len(out) == 0 {
				out = nil
			}

			assert.Equal(t, test.Expected, out)
			assert.Equal(t, test.Truncated, truncated, "truncated should be %t", test.Truncated)
		})
	}
}

func pointer[T any](value T) *T {
	return &value
}
