package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/failure_details"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	commonpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	dto "github.com/prometheus/client_model/go"
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
	assert.Equal(t, 0, len(resp.Invocation[0].BuildMetadata))
	assert.Equal(t, 0, len(resp.Invocation[0].WorkspaceStatus))
}

func TestGetInvocationWithMetadata(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()
	env, ctx := getEnvAndCtx(t, "user1")
	streamBuild(t, env, testInvocationID)
	s := NewAPIServer(env)
	resp, err := s.GetInvocation(ctx, &apipb.GetInvocationRequest{Selector: &apipb.InvocationSelector{InvocationId: testInvocationID}, IncludeMetadata: true})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, len(resp.Invocation))
	assert.Equal(t, 3, len(resp.Invocation[0].BuildMetadata))
	assert.Equal(t, 2, len(resp.Invocation[0].WorkspaceStatus))
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

func TestGetTargetFailedToBuild(t *testing.T) {
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	env, ctx := getEnvAndCtx(t, "user1")
	streamFailedBuild(t, env, testInvocationID)

	s := NewAPIServer(env)
	resp, err := s.GetTarget(ctx, &apipb.GetTargetRequest{Selector: &apipb.TargetSelector{InvocationId: testInvocationID}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, len(resp.Target))
	target := resp.Target[0]
	assert.Equal(t, "//failed/target:bar", target.GetLabel())
	assert.Equal(t, commonpb.Status_FAILED_TO_BUILD, target.GetStatus())
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
	require.Equal(t, 4, len(resp.GetAction()))
	assert.Equal(t, resp.Action[0].File[0].Hash, "5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b")
	assert.Equal(t, resp.Action[0].File[0].SizeBytes, int64(152092))
	assert.Equal(t, resp.Action[3].File[0].Name, "stderr")
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
	require.Equal(t, 1, len(resp.Action))
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

func TestDeleteFile_CAS(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	var err error
	env, ctx := getEnvAndCtx(t, "user1")
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator()); err != nil {
		t.Fatal(err)
	}

	s := NewAPIServer(env)

	// Save file
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	if err := s.env.GetCache().Set(ctx, r, buf); err != nil {
		t.Fatal(err)
	}
	data, err := s.env.GetCache().Get(ctx, r)
	require.NoError(t, err)
	require.NotNil(t, data)

	casURI := fmt.Sprintf("blobs/%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: casURI})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify file was deleted
	data, err = s.env.GetCache().Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, data)
}

func TestDeleteFile_AC(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	var err error
	env, ctx := getEnvAndCtx(t, "user1")
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator()); err != nil {
		t.Fatal(err)
	}

	s := NewAPIServer(env)

	// Save file
	r, buf := testdigest.RandomACResourceBuf(t, 100)
	if err = env.GetCache().Set(ctx, r, buf); err != nil {
		t.Fatal(err)
	}
	data, err := env.GetCache().Get(ctx, r)
	require.NoError(t, err)
	require.NotNil(t, data)

	acURI := fmt.Sprintf("blobs/ac/%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: acURI})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify file was deleted
	data, err = env.GetCache().Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, data)
}

func TestDeleteFile_AC_RemoteInstanceName(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	var err error
	env, ctx := getEnvAndCtx(t, "user1")
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator()); err != nil {
		t.Fatal(err)
	}

	s := NewAPIServer(env)

	// Save file
	remoteInstanceName := "remote/instance"
	r, buf := testdigest.NewRandomResourceAndBuf(t, 100, rspb.CacheType_AC, remoteInstanceName)
	if err = env.GetCache().Set(ctx, r, buf); err != nil {
		t.Fatal(err)
	}
	data, err := env.GetCache().Get(ctx, r)
	require.NoError(t, err)
	require.NotNil(t, data)

	acURI := fmt.Sprintf("%s/blobs/ac/%s/%d", remoteInstanceName, r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: acURI})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify file was deleted
	data, err = env.GetCache().Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, data)
}

func TestDeleteFile_NonExistentFile(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	var err error
	env, ctx := getEnvAndCtx(t, "user1")
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator()); err != nil {
		t.Fatal(err)
	}
	s := NewAPIServer(env)

	// Do not write data to the cache
	r, _ := testdigest.RandomCASResourceBuf(t, 100)
	casURI := fmt.Sprintf("blobs/%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: casURI})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify file still does not exist - no side effects
	data, err := s.env.GetCache().Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, data)
}

func TestDeleteFile_LeadingSlash(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	var err error
	env, ctx := getEnvAndCtx(t, "user1")
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator()); err != nil {
		t.Fatal(err)
	}

	s := NewAPIServer(env)

	// Save file
	r, buf := testdigest.RandomCASResourceBuf(t, 100)
	if err = s.env.GetCache().Set(ctx, r, buf); err != nil {
		t.Fatal(err)
	}
	data, err := s.env.GetCache().Get(ctx, r)
	require.NoError(t, err)
	require.NotNil(t, data)

	acURI := fmt.Sprintf("/blobs/%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: acURI})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify file was deleted
	data, err = s.env.GetCache().Get(ctx, r)
	require.True(t, status.IsNotFoundError(err))
	require.Nil(t, data)
}

func TestDeleteFile_InvalidAuth(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	userID := "user"
	userWithoutWriteAuth := testauth.TestUser{
		UserID:       userID,
		GroupID:      "group",
		Capabilities: []cappb.Capability{},
	}

	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{userID: &userWithoutWriteAuth})
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)

	s := NewAPIServer(env)
	r, _ := testdigest.RandomCASResourceBuf(t, 100)
	casURI := fmt.Sprintf("blobs/%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: casURI})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
	require.Nil(t, resp)
}

func TestDeleteFile_InvalidURI(t *testing.T) {
	flags.Set(t, "enable_cache_delete_api", true)
	var err error
	env, ctx := getEnvAndCtx(t, "user1")
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator()); err != nil {
		t.Fatal(err)
	}

	s := NewAPIServer(env)
	r, _ := testdigest.RandomCASResourceBuf(t, 100)
	uriNonParsableFormat := fmt.Sprintf("non-valid-blob-type/%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes())
	resp, err := s.DeleteFile(ctx, &apipb.DeleteFileRequest{Uri: uriNonParsableFormat})
	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err))
	require.Nil(t, resp)
}

func TestGetActionWithRealData(t *testing.T) {
	env, ctx := getEnvAndCtx(t, "user1")
	iid := streamBuildFromTestData(t, env, ctx, "bes.json")

	s := NewAPIServer(env)
	actionResp, err := s.GetAction(ctx, &apipb.GetActionRequest{Selector: &apipb.ActionSelector{InvocationId: iid}})
	require.NoError(t, err)
	require.NotNil(t, actionResp)
	require.Equal(t, 1, len(actionResp.GetAction()))
	require.Equal(t, 1, len(actionResp.GetAction()[0].GetFile()))
	require.Equal(t, "stderr", actionResp.GetAction()[0].GetFile()[0].GetName())
}

func TestGetInvocationIncludeChildren(t *testing.T) {
	env, ctx := getEnvAndCtx(t, "user1")
	child1InvocationId := streamBuildFromTestData(t, env, ctx, "child1-workflow-bes.json")
	child2InvocationId := streamBuildFromTestData(t, env, ctx, "child2-workflow-bes.json")
	parentInvocationId := streamBuildFromTestData(t, env, ctx, "parent-workflow-bes.json")

	s := NewAPIServer(env)
	rsp, err := s.GetInvocation(ctx, &apipb.GetInvocationRequest{
		Selector:                &apipb.InvocationSelector{InvocationId: parentInvocationId},
		IncludeChildInvocations: true,
	})
	require.NoError(t, err)
	require.NotNil(t, rsp)
	require.Equal(t, 1, len(rsp.GetInvocation()))

	invocationRsp := rsp.GetInvocation()[0]
	require.Equal(t, 2, len(invocationRsp.GetChildInvocations()))
	require.Equal(t, child1InvocationId, invocationRsp.GetChildInvocations()[0].GetInvocationId())
	require.Equal(t, child2InvocationId, invocationRsp.GetChildInvocations()[1].GetInvocationId())
}

func TestGetTargetWithLowFilterThreshold(t *testing.T) {
	// Avoid trying to replicate the blobs to local test server
	flags.Set(t, "storage.disable_persist_cache_artifacts", true)
	// Avoid extracting inline cache score card
	flags.Set(t, "cache.detailed_stats_enabled", true)
	env, ctx := getEnvAndCtx(t, "user1")
	for _, tc := range []struct {
		eventFilterThreshold int
	}{
		{
			eventFilterThreshold: 1,
		},
		{
			eventFilterThreshold: 30,
		},
	} {
		t.Run(fmt.Sprintf("%d", tc.eventFilterThreshold), func(t *testing.T) {
			flags.Set(t, "app.build_event_filter_start_threshold", tc.eventFilterThreshold)

			testUUID, err := uuid.NewRandom()
			assert.NoError(t, err)
			testInvocationID := testUUID.String()
			streamBuild(t, env, testInvocationID)

			s := NewAPIServer(env)
			targetResp, err := s.GetTarget(ctx, &apipb.GetTargetRequest{Selector: &apipb.TargetSelector{InvocationId: testInvocationID}})
			require.NoError(t, err)
			require.NotNil(t, targetResp)
			require.Equal(t, 3, len(targetResp.GetTarget()))
			for _, target := range targetResp.GetTarget() {
				if target.GetTiming() == nil {
					require.Equal(
						t,
						commonpb.Status_BUILT,
						target.GetStatus(),
						fmt.Sprintf(
							"expect target %q to have status %q, actual %q",
							target.GetLabel(),
							commonpb.Status_name[int32(commonpb.Status_BUILT)],
							commonpb.Status_name[int32(target.GetStatus())],
						),
					)
				} else {
					require.Equal(
						t,
						commonpb.Status_PASSED,
						target.GetStatus(),
						fmt.Sprintf(
							"expect target %q to have status %q, actual %q",
							target.GetLabel(),
							commonpb.Status_name[int32(commonpb.Status_PASSED)],
							commonpb.Status_name[int32(target.GetStatus())],
						),
					)
				}
			}
		})
	}
}

func TestCreateUserApiKey(t *testing.T) {
	flags.Set(t, "app.user_owned_keys_enabled", true)
	ctx := context.Background()
	env := enterprise_testenv.New(t)
	auth := enterprise_testauth.Configure(t, env)
	users := enterprise_testauth.CreateRandomGroups(t, env)

	// Get an admin user.
	var admin *tables.User
	var adminGroup *tables.Group
	for _, u := range users {
		if u.Groups[0].HasCapability(cappb.Capability_ORG_ADMIN) {
			admin = u
			adminGroup = &u.Groups[0].Group
			break
		}
	}
	adminUserCtx, err := auth.WithAuthenticatedUser(ctx, admin.UserID)
	require.NoError(t, err)
	// As the admin user, enable user-owned keys for their group.
	enterprise_testauth.SetUserOwnedKeysEnabled(t, adminUserCtx, env, adminGroup.GroupID, true)
	// As the admin user, create an org admin key.
	orgAdminKey, err := env.GetAuthDB().CreateAPIKey(
		adminUserCtx, adminGroup.GroupID, "test-admin-key",
		[]cappb.Capability{cappb.Capability_ORG_ADMIN},
		0,     /*=expiresIn*/
		false, /*=visibleToDevelopers*/
	)
	require.NoError(t, err)
	orgAdminKeyInfo, err := env.GetAuthDB().GetAPIKeyGroupFromAPIKey(ctx, orgAdminKey.Value)
	require.NoError(t, err)
	orgAdminKeyClaims, err := claims.APIKeyGroupClaims(ctx, orgAdminKeyInfo)
	require.NoError(t, err)
	orgAdminKeyCtx := testauth.WithAuthenticatedUserInfo(ctx, orgAdminKeyClaims)

	s := NewAPIServer(env)

	// Send an unauthenticated request to create a user API key; this should
	// fail.
	rsp, err := s.CreateUserApiKey(ctx, &apipb.CreateUserApiKeyRequest{
		UserId: admin.UserID,
	})
	require.True(t, status.IsUnauthenticatedError(err))
	require.Nil(t, rsp)

	// Send an authenticated request to create a user API key for the admin
	// user; this should succeed.
	rsp, err = s.CreateUserApiKey(orgAdminKeyCtx, &apipb.CreateUserApiKeyRequest{
		UserId:    admin.UserID,
		Label:     "test-api-key",
		ExpiresIn: durationpb.New(time.Hour),
	})
	require.NoError(t, err)
	require.NotEmpty(t, rsp.GetApiKey().GetApiKeyId())
	require.NotEmpty(t, rsp.GetApiKey().GetValue())
	require.Greater(
		t,
		rsp.GetApiKey().GetExpirationTimestamp().AsTime().Unix(),
		time.Now().Add(30*time.Minute).Unix(),
	)
	require.Less(
		t,
		rsp.GetApiKey().GetExpirationTimestamp().AsTime().Unix(),
		time.Now().Add(90*time.Minute).Unix(),
	)
	require.Equal(t, "test-api-key", rsp.GetApiKey().GetLabel())

	// Find another user who is not in the admin group.
	// Should not be able to create a user API key for that user.
	var nonGroupMember *tables.User
	for _, u := range users {
		isAdminGroupMember := false
		for _, g := range u.Groups {
			if g.GroupID == adminGroup.GroupID {
				isAdminGroupMember = true
				break
			}
		}
		if !isAdminGroupMember {
			nonGroupMember = u
		}
	}
	_, err = s.CreateUserApiKey(orgAdminKeyCtx, &apipb.CreateUserApiKeyRequest{
		UserId: nonGroupMember.UserID,
		Label:  "test-api-key",
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "want PermissionDenied, got %v", err)

	// Add the non-member to the group.
	err = env.GetUserDB().UpdateGroupUsers(orgAdminKeyCtx, adminGroup.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{
			UserId:           &uidpb.UserId{Id: nonGroupMember.UserID},
			MembershipAction: grpb.UpdateGroupUsersRequest_Update_ADD,
		},
	})
	require.NoError(t, err)
	newlyAddedUserID := nonGroupMember.UserID

	// Try creating a user API key again for the user who is now a member of the
	// group; this should succeed.
	rsp, err = s.CreateUserApiKey(orgAdminKeyCtx, &apipb.CreateUserApiKeyRequest{
		UserId: newlyAddedUserID,
		Label:  "test-api-key",
	})
	require.NoError(t, err)
	require.NotEmpty(t, rsp.GetApiKey().GetApiKeyId())
	require.NotEmpty(t, rsp.GetApiKey().GetValue())
	require.Equal(t, "test-api-key", rsp.GetApiKey().GetLabel())
	require.Nil(t, rsp.GetApiKey().GetExpirationTimestamp())
}

func TestMetrics(t *testing.T) {
	flags.Set(t, "api.enable_metrics_api", true)
	const testFlags = `{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "api.metrics_federation.enabled": {
      "state": "ENABLED",
      "defaultVariant": "false",
      "variants": {
        "true": true,
        "false": false
      },
      "targeting": {
        "if": [
			{"==": [ {"var": "group_id"}, "GR1" ]},
			"true",
			"false"
		]
      }
    },
	"api.metrics_federation.match_parameters": {
      "state": "ENABLED",
      "defaultVariant": "mac_metrics",
      "variants": {
        "mac_metrics": {
			"job": "(mac-executor|mac-node)"
		}
      }
    }
  }
}
`
	env := enterprise_testenv.New(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)
	tmp := testfs.MakeTempDir(t)
	offlineFlagPath := testfs.WriteFile(t, tmp, "config.flagd.json", testFlags)
	provider := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
	openfeature.SetProviderAndWait(provider)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
	fakeProm := &fakePromQuerier{}
	env.SetPromQuerier(fakeProm)
	s := NewAPIServer(env)

	for _, tc := range []struct {
		name                string
		authenticatedUserID string
		expectedStatus      int
		expectedResponse    string
	}{
		{
			name:                "fetch federated metrics as configured group",
			authenticatedUserID: "US1",
			expectedStatus:      http.StatusOK,
			expectedResponse:    `buildbuddy_remote_execution_tasks_executing{job="mac-executor",group_id="GR1"} 1`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.authenticatedUserID != "" {
				var err error
				ctx, err = ta.WithAuthenticatedUser(ctx, tc.authenticatedUserID)
				require.NoError(t, err)
			}
			req := &http.Request{}
			req = req.WithContext(ctx)
			tw := &testResponseWriter{}
			s.GetMetricsHandler().ServeHTTP(tw, req)
			assert.Equal(t, tc.expectedStatus, tw.Status)
			assert.Contains(t, tw.String(), tc.expectedResponse)
		})
	}
}

func getEnvAndCtx(t testing.TB, user string) (*testenv.TestEnv, context.Context) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, userMap)
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

func streamBuildFromTestData(t testing.TB, te *testenv.TestEnv, ctx context.Context, testDataFile string) string {
	handler := build_event_handler.NewBuildEventHandler(te)

	f, err := os.Open(path.Join("testdata", testDataFile))
	require.NoError(t, err)
	defer f.Close()

	// Assume the test data follows the --build_event_json_file format
	decoder := json.NewDecoder(f)
	// skip the first token, which is the square bracket `[` of the json array
	_, err = decoder.Token()
	require.NoError(t, err)

	var channel interfaces.BuildEventChannel
	var iid string
	eventId := int64(1)
	for decoder.More() {
		var raw json.RawMessage
		err := decoder.Decode(&raw)
		require.NoError(t, err)

		var event build_event_stream.BuildEvent
		err = protojson.Unmarshal(raw, &event)
		require.NoError(t, err)

		if eventId == 1 {
			iid = event.GetStarted().GetUuid()
			require.NotEmpty(t, iid, event.String())

			channel, err = handler.OpenChannel(ctx, iid)
			require.NoError(t, err)
			defer channel.Close()
		}

		anyEvent := &anypb.Any{}
		anyEvent.MarshalFrom(&event)

		err = channel.HandleEvent(streamRequest(anyEvent, iid, int64(eventId)))
		require.NoError(t, err)

		eventId++
	}

	err = channel.FinalizeInvocation(iid)
	assert.NoError(t, err)

	return iid
}

func streamFailedBuild(t *testing.T, te *testenv.TestEnv, iid string) {
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), iid)
	require.NoError(t, err)
	defer channel.Close()

	events := []*anypb.Any{
		startedEvent("--remote_header='" + authutil.APIKeyHeader + "=user1'"),
		targetConfiguredEvent("//failed/target:bar", "java_binary rule", "tag-failed"),
		targetFailedEvent("//failed/target:bar"),
		finishedEvent(),
	}

	for idx, evt := range events {
		err := channel.HandleEvent(streamRequest(evt, iid, int64(idx+1)))
		assert.NoError(t, err)
	}

	err = channel.FinalizeInvocation(iid)
	assert.NoError(t, err)
}

func streamBuild(t *testing.T, te *testenv.TestEnv, iid string) {
	handler := build_event_handler.NewBuildEventHandler(te)
	channel, err := handler.OpenChannel(context.Background(), iid)
	require.NoError(t, err)
	defer channel.Close()

	err = channel.HandleEvent(streamRequest(startedEvent("--remote_header='"+authutil.APIKeyHeader+"=user1'"), iid, 1))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetConfiguredEvent("//my/target:foo", "java_binary rule", "tag-a"), iid, 2))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(buildMetadataEvent(), iid, 3))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(workspaceStatusEvent(), iid, 4))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(progressEvent("hello world"), iid, 5))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetCompletedEvent("//my/target:foo"), iid, 6))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetConfiguredEvent("//my/other/target:foo", "go_binary rule", "tag-b"), iid, 7))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetCompletedEvent("//my/other/target:foo"), iid, 8))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetConfiguredEvent("//my/third/target:foo", "genrule rule", "tag-b"), iid, 9))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(targetCompletedEvent("//my/third/target:foo"), iid, 10))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(actionCompleteEvent("//failed:target"), iid, 11))
	assert.NoError(t, err)

	err = channel.HandleEvent(streamRequest(finishedEvent(), iid, 12))
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
	startedAny := &anypb.Any{}
	startedAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				OptionsDescription: options,
			},
		},
	})
	return startedAny
}

func targetConfiguredEvent(label, kind, tag string) *anypb.Any {
	targetConfiguredAny := &anypb.Any{}
	targetConfiguredAny.MarshalFrom(&build_event_stream.BuildEvent{
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
	return targetConfiguredAny
}

func targetCompletedEvent(label string) *anypb.Any {
	targetCompletedAny := &anypb.Any{}
	targetCompletedAny.MarshalFrom(&build_event_stream.BuildEvent{
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
				Success: true,
				DirectoryOutput: []*build_event_stream.File{
					{
						Name: "my-output.txt",
						File: &build_event_stream.File_Uri{
							Uri: "bytestream://localhost:8080/buildbuddy-io/buildbuddy/ci/blobs/5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b/152092",
						},
					},
				},
			},
		},
	})
	return targetCompletedAny
}

func targetFailedEvent(label string) *anypb.Any {
	targetFailedAny := &anypb.Any{}
	targetFailedAny.MarshalFrom(&build_event_stream.BuildEvent{
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
				Success: false,
				FailureDetail: &failure_details.FailureDetail{
					Message: "worker spawn failed",
					Category: &failure_details.FailureDetail_Spawn{
						Spawn: &failure_details.Spawn{
							Code: *failure_details.Spawn_NON_ZERO_EXIT.Enum(),
						},
					},
				},
			},
		},
	})
	return targetFailedAny
}

func actionCompleteEvent(label string) *anypb.Any {
	actionCompleteEvent := &anypb.Any{}
	actionCompleteEvent.MarshalFrom(&build_event_stream.BuildEvent{
		Id: &build_event_stream.BuildEventId{
			Id: &build_event_stream.BuildEventId_ActionCompleted{
				ActionCompleted: &build_event_stream.BuildEventId_ActionCompletedId{
					Label: label,
					Configuration: &build_event_stream.BuildEventId_ConfigurationId{
						Id: "config1",
					},
				},
			},
		},
		Payload: &build_event_stream.BuildEvent_Action{
			Action: &build_event_stream.ActionExecuted{
				Type:     "actionMnemonic",
				ExitCode: 1,
				Stderr: &build_event_stream.File{
					Name: "stderr",
					File: &build_event_stream.File_Uri{
						Uri: "bytestream://localhost:8080/buildbuddy-io/buildbuddy/ci/blobs/5dee5f7b2ecaf0365ae2811ab98cb5ba306e72fb088787e176e3b4afd926a55b/152092",
					},
				},
				Label: label,
				Configuration: &build_event_stream.BuildEventId_ConfigurationId{
					Id: "config1",
				},
				CommandLine: []string{"exit", "1"},
				FailureDetail: &failure_details.FailureDetail{
					Message: "action failed",
					Category: &failure_details.FailureDetail_Spawn{
						Spawn: &failure_details.Spawn{
							Code: *failure_details.Spawn_NON_ZERO_EXIT.Enum(),
						},
					},
				},
			},
		},
	})
	return actionCompleteEvent
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

func buildMetadataEvent() *anypb.Any {
	buildMetadataAny := &anypb.Any{}
	buildMetadataAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_BuildMetadata{
			BuildMetadata: &build_event_stream.BuildMetadata{
				Metadata: map[string]string{
					"ALLOW_ENV": "SHELL",
					"ROLE":      "METADATA_CI",
					"REPO_URL":  "git@github.com:/buildbuddy-io/metadata_repo_url",
				},
			},
		},
	})
	return buildMetadataAny
}

func workspaceStatusEvent() *anypb.Any {
	workspaceStatusAny := &anypb.Any{}
	workspaceStatusAny.MarshalFrom(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_WorkspaceStatus{
			WorkspaceStatus: &build_event_stream.WorkspaceStatus{
				Item: []*build_event_stream.WorkspaceStatus_Item{
					{
						Key:   "BUILD_USER",
						Value: "WORKSPACE_STATUS_BUILD_USER",
					},
					{
						Key:   "BUILD_HOST",
						Value: "WORKSPACE_STATUS_BUILD_HOST",
					},
				},
			},
		},
	})
	return workspaceStatusAny
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

type testResponseWriter struct {
	bytes.Buffer
	Status int
	header http.Header
}

func (w *testResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *testResponseWriter) WriteHeader(status int) {
	w.Status = status
}

func (w *testResponseWriter) Write(p []byte) (int, error) {
	if w.Status == 0 {
		w.WriteHeader(200)
	}
	return w.Buffer.Write(p)
}

type fakePromQuerier struct{}

func (p *fakePromQuerier) FetchMetrics(ctx context.Context, groupID string) ([]*dto.MetricFamily, error) {
	return nil, nil
}
func (p *fakePromQuerier) FetchFederatedMetrics(ctx context.Context, w io.Writer, match string) error {
	if !strings.Contains(match, `group_id="GR1"`) {
		return fmt.Errorf("match param must contain group_id filter")
	}
	if !strings.Contains(match, "job=~") {
		return fmt.Errorf("match param must contain job filter")
	}
	_, err := w.Write([]byte(`buildbuddy_remote_execution_tasks_executing{job="mac-executor",group_id="GR1"} 1`))
	return err
}
