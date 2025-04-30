package buildbuddy_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/acl"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/user_id"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	channel, err := handler.OpenChannel(ctx, testInvocationID)
	if err != nil {
		return "", err
	}

	// Send started event with api key
	options := ""
	if user != "" {
		options = "--remote_header='" + authutil.APIKeyHeader + "=" + user + "'"
	}
	started, err := anypb.New(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				OptionsDescription: options,
			},
		},
	})
	if err != nil {
		return "", err
	}
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

	_, err = server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{Lookup: &inpb.InvocationLookup{InvocationId: ""}},
	)
	require.Error(t, err)

	_, err = server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user2),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user2, group2),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.Error(t, err)
}

func TestGetInvocation_FetchChildren(t *testing.T) {
	te := testenv.GetTestEnv(t)

	auth := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1, user2, group2))
	te.SetAuthenticator(auth)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	ctx, err = auth.WithAuthenticatedUser(ctx, user1)
	require.NoError(t, err)

	dbh := te.GetDBHandle()
	idb := invocationdb.NewInvocationDB(te, dbh)

	// Create parent invocation
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{
		InvocationID: "parent",
		RunID:        "parent-id",
	})
	require.NoError(t, err)
	require.True(t, created)

	// Create child invocations
	for i := 0; i < 10; i++ {
		created, err = idb.CreateInvocation(ctx, &tables.Invocation{
			InvocationID: fmt.Sprintf("child-%d", i),
			ParentRunID:  "parent-id",
		})
		require.NoError(t, err)
		require.True(t, created)
	}

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	rsp, err := server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			Lookup: &inpb.InvocationLookup{
				InvocationId:          "parent",
				FetchChildInvocations: true,
			}},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(rsp.Invocation))
	inv := rsp.Invocation[0]
	require.Equal(t, inv.InvocationId, "parent")

	// Ensure child invocations are sorted by increasing creation time
	require.Equal(t, 10, len(inv.ChildInvocations))
	for i := 0; i < 10; i++ {
		require.Equal(t, fmt.Sprintf("child-%d", i), inv.ChildInvocations[i].InvocationId)
	}
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
	te.GetDBHandle().NewQuery(context.Background(), "create_invocation").Create(&tables.Group{GroupID: group1, SharingEnabled: true})

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

	_, err = server.GetInvocation(
		te.GetAuthenticator().AuthContextFromAPIKey(context.Background(), user1),
		&inpb.GetInvocationRequest{
			RequestContext: testauth.RequestContext(user1, group1),
			Lookup:         &inpb.InvocationLookup{InvocationId: iid}},
	)
	require.Error(t, err)
}

func TestFileDownloadEndpoint(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1))
	te.SetAuthenticator(auth)
	err := buildbuddy_server.Register(te)
	require.NoError(t, err)
	// Start gRPC server (for cache API)
	grpcPort := testport.FindFree(t)
	gs, err := grpc_server.New(te, grpcPort, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	te.SetGRPCServer(gs.GetServer())
	testcache.RegisterServers(t, te)
	err = gs.Start()
	require.NoError(t, err)
	// Register gRPC clients
	conn, err := grpc_client.DialSimpleWithoutPooling(fmt.Sprintf("grpc://localhost:%d", grpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	testcache.RegisterClients(te, conn)
	// Start HTTP server (for /file/download endpoint)
	mux := http.NewServeMux()
	mux.Handle("/file/download", interceptors.WrapAuthenticatedExternalHandler(te, te.GetBuildBuddyServer()))
	baseURL := testhttp.StartServer(t, mux)

	iid, err := createInvocationForTesting(te, "" /*=user*/)
	require.NoError(t, err)

	{
		// Upload CAS resource
		rn, b := testdigest.RandomCASResourceBuf(t, 100)
		casrn, err := digest.CASResourceNameFromProto(rn)
		require.NoError(t, err)
		_, _, err = cachetools.UploadFromReader(ctx, te.GetByteStreamClient(), casrn, bytes.NewReader(b))
		require.NoError(t, err)

		// Fetch it from /file/download endpoint
		bsURL := fmt.Sprintf("bytestream://localhost:%d/blobs/%s", grpcPort, digest.String(rn.GetDigest()))
		rsp, err := http.Get(fmt.Sprintf(
			"%s/file/download?invocation_id=%s&bytestream_url=%s",
			baseURL, iid, url.QueryEscape(bsURL)))
		require.NoError(t, err)
		defer rsp.Body.Close()
		body, err := io.ReadAll(rsp.Body)
		require.NoError(t, err)
		require.Equal(t, b, body)
	}

	{
		// Upload AC resource
		key := &repb.Digest{
			// Note: hash here can be arbitrary.
			Hash:      "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
			SizeBytes: 111,
		}
		rn := digest.NewACResourceName(key, "", repb.DigestFunction_SHA256)
		ar := &repb.ActionResult{
			StdoutRaw: []byte("test-stdout"),
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				// Set worker explicitly, otherwise AC server sets it to a uuid.
				Worker: "test-worker",
			},
		}
		err = cachetools.UploadActionResult(ctx, te.GetActionCacheClient(), rn, ar)
		require.NoError(t, err)

		// Fetch it with /file/download endpoint
		acURL := fmt.Sprintf("actioncache://localhost:%d/blobs/ac/%s", grpcPort, digest.String(key))
		rsp, err := http.Get(fmt.Sprintf(
			"%s/file/download?invocation_id=%s&bytestream_url=%s",
			baseURL, iid, url.QueryEscape(acURL)))
		require.NoError(t, err)
		defer rsp.Body.Close()
		body, err := io.ReadAll(rsp.Body)
		require.NoError(t, err)
		arb, err := proto.Marshal(ar)
		require.NoError(t, err)
		require.Equal(t, arb, body)
	}
}
