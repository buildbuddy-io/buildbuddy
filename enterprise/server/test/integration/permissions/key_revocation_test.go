package permissions_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/permissionstest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func TestPersonalAPIKeyDowngradeAndRemovalWithUncachedAuthentication(t *testing.T) {
	// This variant verifies enforcement at the DB lookup boundary. Production
	// caches API-key group claims (default TTL: 5 minutes), so it does NOT claim
	// immediate revocation for deployments using the default cache settings.
	// The existing browser-session test keeps the production cache defaults.
	f := permissionstest.New(t, "--auth.api_key_group_cache_ttl=0", "--auth.enable_anonymous_usage=false")
	admin := f.Login(t, f.Users[permissionstest.AdminName])
	developer := f.Login(t, f.Users[permissionstest.DeveloperName])
	created := &akpb.CreateApiKeyResponse{}
	require.NoError(t, developer.RPC("CreateUserApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: requestContext(developer, f.OrgA),
		UserId:         f.Users[permissionstest.DeveloperName].ID,
		Label:          "key-to-downgrade-and-revoke",
		Capability:     []cappb.Capability{cappb.Capability_CAS_WRITE},
	}, created))
	require.NotEmpty(t, created.GetApiKey().GetValue())

	conn, err := grpc_client.DialSimpleWithoutPooling(f.App.GRPCAddress())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cas := repb.NewContentAddressableStorageClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// This connection has only the API key, no user session cookies or trusted JWT.
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", created.GetApiKey().GetValue())
	upload := func(data string) *repb.Digest {
		t.Helper()
		digest := &repb.Digest{Hash: fmt.Sprintf("%x", sha256.Sum256([]byte(data))), SizeBytes: int64(len(data))}
		rsp, err := cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
			DigestFunction: repb.DigestFunction_SHA256,
			Requests:       []*repb.BatchUpdateBlobsRequest_Request{{Digest: digest, Data: []byte(data)}},
		})
		require.NoError(t, err)
		require.Len(t, rsp.GetResponses(), 1)
		require.EqualValues(t, codes.OK, rsp.GetResponses()[0].GetStatus().GetCode())
		return digest
	}
	before := upload("persisted-before-role-downgrade")
	existing, err := cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{before}})
	require.NoError(t, err)
	require.Empty(t, existing.GetMissingBlobDigests(), "positive control: Developer key actually stored the blob")

	require.NoError(t, admin.RPC("UpdateGroupUsers", &grpb.UpdateGroupUsersRequest{
		RequestContext: requestContext(admin, f.OrgA), GroupId: f.OrgA,
		Update: []*grpb.UpdateGroupUsersRequest_Update{{UserId: &uidpb.UserId{Id: f.Users[permissionstest.DeveloperName].ID}, Role: grpb.Group_READER_ROLE}},
	}, &grpb.UpdateGroupUsersResponse{}))
	existing, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{before}})
	require.NoError(t, err)
	require.Empty(t, existing.GetMissingBlobDigests(), "Reader key remains authenticated and can read existing data")

	// Read-only CAS uploads deliberately acknowledge success without persisting
	// data. Checking only the RPC status would falsely claim writes still work.
	after := upload("must-not-be-persisted-after-role-downgrade")
	missing, err := cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{after}})
	require.NoError(t, err)
	require.Len(t, missing.GetMissingBlobDigests(), 1)
	require.Equal(t, after.GetHash(), missing.GetMissingBlobDigests()[0].GetHash())

	require.NoError(t, admin.RPC("UpdateGroupUsers", &grpb.UpdateGroupUsersRequest{
		RequestContext: requestContext(admin, f.OrgA), GroupId: f.OrgA,
		Update: []*grpb.UpdateGroupUsersRequest_Update{{
			UserId:           &uidpb.UserId{Id: f.Users[permissionstest.DeveloperName].ID},
			MembershipAction: grpb.UpdateGroupUsersRequest_Update_REMOVE,
		}},
	}, &grpb.UpdateGroupUsersResponse{}))
	_, err = cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{before}})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "the same key must stop authenticating after removal: %v", err)
}
