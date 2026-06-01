// This is a test for server/buildbuddy_server that exercises enterprise
// features that cannot be referenced in the non-enterprise test.
package buildbuddy_server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/testencryption"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func authUserCtx(ctx context.Context, env environment.Env, t *testing.T, userID string) context.Context {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func getGroup(t *testing.T, ctx context.Context, env environment.Env) *tables.GroupRole {
	tu, err := env.GetUserDB().GetUser(ctx)
	require.NoError(t, err, "failed to get self-owned group")
	require.Len(t, tu.Groups, 1, "getGroup: user must be part of exactly one group")
	return tu.Groups[0]
}

func TestCreateGroup(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	auth := te.GetAuthenticator()
	te.SetAuthenticator(auth)
	ctx := context.Background()

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	flags.Set(t, "app.restrict_multi_group_to_enterprise", true)

	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	parentGroup := getGroup(t, userCtx, te).Group
	parentGroup.SamlIdpMetadataUrl = "https://some/saml/url"
	parentGroup.URLIdentifier = "foo"
	_, err = te.GetUserDB().UpdateGroup(userCtx, &parentGroup)
	require.NoError(t, err)

	// Set up server admin and update group status to enterprise
	flags.Set(t, "auth.admin_group_id", parentGroup.GroupID)
	adminRole, err := role.ToProto(role.Admin)
	require.NoError(t, err)
	err = te.GetUserDB().UpdateGroupUsers(userCtx, parentGroup.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{
			UserId: &uidpb.UserId{Id: "US1"},
			Role:   adminRole,
		},
	})
	require.NoError(t, err)
	userCtx = authUserCtx(ctx, te, t, "US1")
	err = te.GetUserDB().UpdateGroupStatus(userCtx, parentGroup.GroupID, grpb.Group_ENTERPRISE_GROUP_STATUS)
	require.NoError(t, err)

	adminKey, err := te.GetAuthDB().CreateAPIKey(
		userCtx, parentGroup.GroupID, "admin",
		[]cappb.Capability{cappb.Capability_ORG_ADMIN},
		0, /*=expiresIn*/
		false /*=visibleToDevelopers*/)
	require.NoError(t, err)
	adminKeyCtx := te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	// Create a new group. The SAML IDP Metadata URL should not be set as the
	// first group is not marked as a "parent".
	rsp, err := server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
		Name:          "test",
		UrlIdentifier: "test",
	})
	require.NoError(t, err)
	g, err := te.GetUserDB().GetGroupByID(ctx, rsp.GetId())
	require.NoError(t, err)
	require.Empty(t, g.SamlIdpMetadataUrl)

	// Make the first group a parent and try again.
	// The SAML IDP Metadata URL should match that of the original group.
	parentGroup.IsParent = true
	_, err = te.GetUserDB().UpdateGroup(userCtx, &parentGroup)
	require.NoError(t, err)
	rsp, err = server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
		Name:          "test2",
		UrlIdentifier: "test2",
	})
	require.NoError(t, err)
	g, err = te.GetUserDB().GetGroupByID(ctx, rsp.GetId())
	require.NoError(t, err)
	require.Equal(t, parentGroup.SamlIdpMetadataUrl, g.SamlIdpMetadataUrl)
	require.False(t, g.IsParent)
}

func TestCreateGroup_StatusRestrictions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      grpb.Group_GroupStatus
		shouldAllow bool
	}{
		{"FreeTier", grpb.Group_FREE_TIER_GROUP_STATUS, false},
		{"Blocked", grpb.Group_BLOCKED_GROUP_STATUS, false},
		{"Unknown", grpb.Group_UNKNOWN_GROUP_STATUS, true},
		{"EnterpriseTrial", grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS, true},
		{"Enterprise", grpb.Group_ENTERPRISE_GROUP_STATUS, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := enterprise_testenv.New(t)
			enterprise_testauth.Configure(t, te)
			auth := te.GetAuthenticator()
			te.SetAuthenticator(auth)
			ctx := context.Background()

			flags.Set(t, "app.create_group_per_user", true)
			flags.Set(t, "app.no_default_user_group", true)
			flags.Set(t, "app.restrict_multi_group_to_enterprise", true)

			err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
			require.NoError(t, err)
			userCtx := authUserCtx(ctx, te, t, "US1")
			group := getGroup(t, userCtx, te).Group
			group.URLIdentifier = "test-group"
			_, err = te.GetUserDB().UpdateGroup(userCtx, &group)
			require.NoError(t, err)

			flags.Set(t, "auth.admin_group_id", group.GroupID)
			adminRole, err := role.ToProto(role.Admin)
			require.NoError(t, err)
			err = te.GetUserDB().UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
				{
					UserId: &uidpb.UserId{Id: "US1"},
					Role:   adminRole,
				},
			})
			require.NoError(t, err)
			userCtx = authUserCtx(ctx, te, t, "US1")
			err = te.GetUserDB().UpdateGroupStatus(userCtx, group.GroupID, tc.status)
			require.NoError(t, err)

			adminKey, err := te.GetAuthDB().CreateAPIKey(
				userCtx, group.GroupID, "admin",
				[]cappb.Capability{cappb.Capability_ORG_ADMIN},
				0, /*=expiresIn*/
				false /*=visibleToDevelopers*/)
			require.NoError(t, err)
			adminKeyCtx := te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)

			server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
			require.NoError(t, err)

			_, err = server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
				Name:          "test",
				UrlIdentifier: "test",
			})
			if tc.shouldAllow {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "enterprise account is required")
			}
		})
	}
}

func TestSetGroupStatus(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	ctx := context.Background()
	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group

	flags.Set(t, "auth.admin_group_id", group.GroupID)
	adminRole, err := role.ToProto(role.Admin)
	require.NoError(t, err)
	err = te.GetUserDB().UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{
			UserId: &uidpb.UserId{Id: "US1"},
			Role:   adminRole,
		},
	})
	require.NoError(t, err)
	userCtx = authUserCtx(ctx, te, t, "US1")

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	req := &grpb.SetGroupStatusRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Status:         grpb.Group_UNKNOWN_GROUP_STATUS,
	}
	rsp, err := server.SetGroupStatus(userCtx, req)
	require.NoError(t, err)
	require.NotNil(t, rsp)

	updatedGroup, err := te.GetUserDB().GetGroupByID(ctx, group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, grpb.Group_UNKNOWN_GROUP_STATUS, updatedGroup.Status)

	req.Status = grpb.Group_BLOCKED_GROUP_STATUS
	rsp, err = server.SetGroupStatus(userCtx, req)
	require.NoError(t, err)
	require.NotNil(t, rsp)

	updatedGroup, err = te.GetUserDB().GetGroupByID(ctx, group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, grpb.Group_BLOCKED_GROUP_STATUS, updatedGroup.Status)
}

const validSamlMetadata = `<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/saml">
  <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>
  </md:IDPSSODescriptor>
</md:EntityDescriptor>`

func setUpSSOConfigTest(t *testing.T) (context.Context, *buildbuddy_server.BuildBuddyServer, environment.Env, *tables.Group) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	ctx := context.Background()
	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group

	flags.Set(t, "auth.admin_group_id", group.GroupID)
	adminRole, err := role.ToProto(role.Admin)
	require.NoError(t, err)
	err = te.GetUserDB().UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{UserId: &uidpb.UserId{Id: "US1"}, Role: adminRole},
	})
	require.NoError(t, err)
	userCtx = authUserCtx(ctx, te, t, "US1")

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)
	return userCtx, server, te, &group
}

func TestGetSSOConfig(t *testing.T) {
	userCtx, server, te, group := setUpSSOConfigTest(t)

	// Initially unset.
	rsp, err := server.GetSSOConfig(userCtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.NoError(t, err)
	assert.Equal(t, "", rsp.GetConfig().GetSamlIdpMetadataUrl())

	// Persist a value directly and read it back.
	require.NoError(t, te.GetUserDB().UpdateGroupSamlIdpMetadataUrl(userCtx, group.GroupID, "https://idp.example.com/meta"))

	rsp, err = server.GetSSOConfig(userCtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.NoError(t, err)
	assert.Equal(t, "https://idp.example.com/meta", rsp.GetConfig().GetSamlIdpMetadataUrl())
}

func TestSetSSOConfig_ValidMetadata(t *testing.T) {
	userCtx, server, te, group := setUpSSOConfigTest(t)

	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		_, _ = w.Write([]byte(validSamlMetadata))
	}))
	defer idp.Close()

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: idp.URL},
	})
	require.NoError(t, err)

	updated, err := te.GetUserDB().GetGroupByID(context.Background(), group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, idp.URL, updated.SamlIdpMetadataUrl)
}

func TestSetSSOConfig_ClearsURL(t *testing.T) {
	userCtx, server, te, group := setUpSSOConfigTest(t)

	// Seed a value so we can verify it gets cleared.
	require.NoError(t, te.GetUserDB().UpdateGroupSamlIdpMetadataUrl(userCtx, group.GroupID, "https://idp.example.com/meta"))

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: ""},
	})
	require.NoError(t, err)

	updated, err := te.GetUserDB().GetGroupByID(context.Background(), group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, "", updated.SamlIdpMetadataUrl)
}

func TestSetSSOConfig_RejectsInvalidMetadata(t *testing.T) {
	userCtx, server, _, group := setUpSSOConfigTest(t)

	notSaml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><body>hello</body></html>`))
	}))
	defer notSaml.Close()

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: notSaml.URL},
	})
	require.Error(t, err)
}

func TestSetSSOConfig_RejectsNonHTTPScheme(t *testing.T) {
	userCtx, server, _, group := setUpSSOConfigTest(t)

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: "file:///etc/passwd"},
	})
	require.Error(t, err)
}

func TestSetSSOConfig_RequiresServerAdmin(t *testing.T) {
	// Create a group that is NOT a server admin and verify the RPC is rejected.
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	// Point admin_group_id at a different group ID so US1's group isn't admin.
	flags.Set(t, "auth.admin_group_id", "GR-NOT-MY-GROUP")

	ctx := context.Background()
	require.NoError(t, te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"}))
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	_, err = server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: "https://idp.example.com/meta"},
	})
	require.Error(t, err)

	_, err = server.GetSSOConfig(userCtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.Error(t, err)
}

func TestInvocationArtifactDownloads(t *testing.T) {
	// This test does the following:
	//
	// Set up pebble cache in testenv with multiple partitions:
	// - anon (for unauthorized users)
	// - default (for authorized users)
	// - nondefault-part1 (org-specific partition; encryption is enabled)
	// - nondefault-part2 (org-specific partition; encryption is not enabled)
	// Use local key provider / testonly key provider. Do not try to use real
	// GCS / AWS.
	//
	// Create orgs with different cache setups:
	// - GR1 (slug: "default-org"): default partition; cache encryption disabled
	// - GR2 (slug: "default-encrypted-org"): default partition; cache encryption enabled
	// - GR3 (slug: "nondefault-part1-encrypted-org"): nondefault-part1 partition; cache encryption enabled
	// - GR4 (slug: "nondefault-part2-unencrypted-org"): nondefault-part2 partition; cache encryption disabled
	//
	// Create fake invocations using build_event_publisher:
	// - One ANON invocation
	// - Two invocations for each org:
	//   - One private invocation
	//   - One public invocation
	//
	// Each invocation should attach three File artifacts, referenced via the
	// BES stream:
	// - One File artifact that successfully gets uploaded to cache and blobstore
	//   File contents: "${invocation_id}_cache_and_blobstore"
	// - One File artifact that gets uploaded to cache, but not blobstore
	//   (simulate this by deleting it from blobstore after publishing the invocation):
	//   File contents: "${invocation_id}_cache_only"
	// - One File artifact that was uploaded to blobstore but expired from blobstore
	//   (simulate this by calling DeleteFile on the cache, assuming this works for pebble.
	//   if not, just don't ever upload it, and write it to blobstore manually)
	// Just use SHA256 digest function and instance_name="". Do not compress
	// artifacts.
	//
	// Then, test /file/download access. Test ALL combinations of the following
	// dimensions:
	//
	// 1. Artifact owner group ID
	// 2. Invocation artifact digest (should test ALL artifacts as described above).
	// 3. Authenticated user's group ID:
	//   For group-owned (authenticated) invocations:
	//     a. Authenticated group ID matches artifact group ID
	//     b. Authenticated group ID does not match artifact group ID but user's
	//        allowed groups list contains the artifact group ID
	//     c. Authenticated user does not have the artifact group ID in their allowed groups
	//     d. User is unauthenticated (anonymous)
	//   For ANON invocations:
	//     a. User is authenticated
	//     b. User is unauthenticated

	ctx := context.Background()
	flags.Set(t, "cache_stats_finalization_delay", time.Millisecond)

	const (
		cacheAndBlobstoreStorage = "cache_and_blobstore"
		cacheOnlyStorage         = "cache_only"
		gcsOnlyStorage           = "gcs_only"
	)

	type testInvocationOwner struct {
		name      string
		groupID   string
		apiKey    string
		userID    string
		encrypted bool
		partition string
		isAnon    bool
	}
	type testInvocationArtifact struct {
		name          string
		storage       string
		contents      []byte
		bytestreamURL string
		cacheResource *rspb.ResourceName
		blobPath      string
	}
	type testInvocation struct {
		name      string
		id        string
		owner     testInvocationOwner
		public    bool
		artifacts []*testInvocationArtifact
	}
	type testInvocationViewer struct {
		name                     string
		apiKey                   string
		groupID                  string
		shouldAllow              bool
		hasInvocationGroupAccess bool
		partition                string
		cacheEncryptionEnabled   bool
	}
	type skippedDownloadTestCase struct {
		invocationOwner string
		visibility      string
		accessVia       string
		artifactStorage string
	}

	te := enterprise_testenv.New(t)
	groupOwners := []testInvocationOwner{
		{name: "default-org", groupID: "GR1", apiKey: "APIKEY_GR1", userID: "US1"},
		{name: "default-encrypted-org", groupID: "GR2", apiKey: "APIKEY_GR2", userID: "US2", encrypted: true},
		{name: "nondefault-part1-encrypted-org", groupID: "GR3", apiKey: "APIKEY_GR3", userID: "US3", encrypted: true, partition: "nondefault-part1"},
		{name: "nondefault-part2-unencrypted-org", groupID: "GR4", apiKey: "APIKEY_GR4", userID: "US4", partition: "nondefault-part2"},
	}
	anonOwner := testInvocationOwner{name: "ANON", groupID: interfaces.AuthAnonymousUser, partition: "anon", isAnon: true}
	partitionID := func(owner testInvocationOwner) string {
		if owner.partition != "" {
			return owner.partition
		}
		return pebble_cache.DefaultPartitionID
	}

	testUsers := map[string]interfaces.UserInfo{}
	allowedAPIKeys := map[string]string{}
	for _, owner := range groupOwners {
		ownerUser := testauth.User(owner.userID, owner.groupID)
		ownerUser.CacheEncryptionEnabled = owner.encrypted
		testUsers[owner.apiKey] = ownerUser

		allowedAPIKey := "APIKEY_ALLOWED_" + owner.groupID
		allowedUser := testauth.User("US_ALLOWED_"+owner.groupID, "GR_ALLOWED_"+owner.groupID)
		allowedUser.AllowedGroups = append(allowedUser.AllowedGroups, owner.groupID)
		testUsers[allowedAPIKey] = allowedUser
		allowedAPIKeys[owner.groupID] = allowedAPIKey
	}
	outsiderAPIKey := "APIKEY_OUTSIDER"
	testUsers[outsiderAPIKey] = testauth.User("US_OUTSIDER", "GR_OUTSIDER")

	auth := testauth.NewTestAuthenticator(t, testUsers)
	te.SetAuthenticator(auth)

	for _, owner := range groupOwners {
		require.NoError(t, te.GetDBHandle().NewQuery(ctx, "create_test_group").Create(&tables.Group{
			GroupID:       owner.groupID,
			URLIdentifier: owner.name,
		}))
	}

	testencryption.Setup(t, te)
	for _, owner := range groupOwners {
		if !owner.encrypted {
			continue
		}
		authCtx := auth.AuthContextFromAPIKey(t.Context(), owner.apiKey)
		testencryption.EnableForAuthenticatedGroup(t, authCtx, te)
	}

	partitionSizeBytes := int64(1_000_000_000)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		Partitions: []disk.Partition{
			{ID: "anon", MaxSizeBytes: partitionSizeBytes},
			{ID: pebble_cache.DefaultPartitionID, MaxSizeBytes: partitionSizeBytes},
			{ID: "nondefault-part1", MaxSizeBytes: partitionSizeBytes},
			{ID: "nondefault-part2", MaxSizeBytes: partitionSizeBytes},
		},
		PartitionMappings: []disk.PartitionMapping{
			{GroupID: interfaces.AuthAnonymousUser, PartitionID: "anon"},
			{GroupID: "GR3", PartitionID: "nondefault-part1"},
			{GroupID: "GR4", PartitionID: "nondefault-part2"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, pc.Start())
	t.Cleanup(func() { require.NoError(t, pc.Stop()) })
	te.SetCache(pc)

	grpcPort := testport.FindFree(t)
	flags.Set(t, "grpc_port", grpcPort)
	gs, err := grpc_server.New(te, grpcPort, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	te.SetGRPCServer(gs.GetServer())
	testcache.RegisterServers(t, te)

	handler := build_event_handler.NewBuildEventHandler(te)
	te.SetBuildEventHandler(handler)
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(te, false /*=synchronous*/)
	require.NoError(t, err)
	pepb.RegisterPublishBuildEventServer(te.GetGRPCServer(), buildEventServer)

	require.NoError(t, gs.Start())
	conn, err := grpc_client.DialSimpleWithoutPooling(fmt.Sprintf("grpc://localhost:%d", grpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	testcache.RegisterClients(te, conn)

	mux := http.NewServeMux()
	mux.Handle("/file/download", interceptors.WrapAuthenticatedExternalHandler(te, te.GetBuildBuddyServer()))
	baseURL := testhttp.StartServer(t, mux)

	digestFunction := repb.DigestFunction_SHA256
	artifactStorages := []string{cacheAndBlobstoreStorage, cacheOnlyStorage, gcsOnlyStorage}
	newArtifact := func(invocationID, storage string) *testInvocationArtifact {
		contents := []byte(invocationID + "_" + storage)
		d, err := digest.Compute(bytes.NewReader(contents), digestFunction)
		require.NoError(t, err)
		rn := digest.NewCASResourceName(d, "", digestFunction)
		bytestreamURL := fmt.Sprintf("bytestream://localhost:%d/%s", grpcPort, rn.DownloadString())
		u, err := url.Parse(bytestreamURL)
		require.NoError(t, err)
		return &testInvocationArtifact{
			name:          storage + ".txt",
			storage:       storage,
			contents:      contents,
			bytestreamURL: bytestreamURL,
			cacheResource: rn.ToProto(),
			blobPath:      path.Join(invocationID, "artifacts", "cache", u.Path),
		}
	}
	ownerContext := func(owner testInvocationOwner) context.Context {
		if owner.isAnon {
			return ctx
		}
		return auth.AuthContextFromAPIKey(ctx, owner.apiKey)
	}
	publishInvocation := func(owner testInvocationOwner, public bool) *testInvocation {
		visibility := "private"
		if public {
			visibility = "public"
		}
		invocationID := uuid.NewString()
		invocation := &testInvocation{
			name:   owner.name + "-" + visibility,
			id:     invocationID,
			owner:  owner,
			public: public || owner.isAnon,
		}
		for _, storage := range artifactStorages {
			invocation.artifacts = append(invocation.artifacts, newArtifact(invocationID, storage))
		}
		for _, artifact := range invocation.artifacts {
			require.NoError(t, te.GetCache().Set(ownerContext(owner), artifact.cacheResource, artifact.contents))
		}

		publisher, err := build_event_publisher.New(fmt.Sprintf("grpc://localhost:%d", grpcPort), owner.apiKey, invocationID)
		require.NoError(t, err)
		publisher.Start(ctx)
		require.NoError(t, publisher.Publish(buildStartedEvent(owner.apiKey)))
		if public {
			require.NoError(t, publisher.Publish(buildMetadataEvent()))
		}
		logs := make([]*bespb.File, 0, len(invocation.artifacts))
		for _, artifact := range invocation.artifacts {
			logs = append(logs, &bespb.File{
				Name:   artifact.name,
				File:   &bespb.File_Uri{Uri: artifact.bytestreamURL},
				Length: int64(len(artifact.contents)),
			})
		}
		require.NoError(t, publisher.Publish(buildToolLogsEvent(logs)))
		require.NoError(t, publisher.Publish(buildFinishedEvent()))
		require.NoError(t, publisher.Finish())
		return invocation
	}

	invocations := []*testInvocation{publishInvocation(anonOwner, true)}
	for _, owner := range groupOwners {
		invocations = append(invocations, publishInvocation(owner, false))
		invocations = append(invocations, publishInvocation(owner, true))
	}

	var allArtifacts []*testInvocationArtifact
	for _, invocation := range invocations {
		allArtifacts = append(allArtifacts, invocation.artifacts...)
	}
	require.Eventually(t, func() bool {
		for _, artifact := range allArtifacts {
			exists, err := te.GetBlobstore().BlobExists(ctx, artifact.blobPath)
			if !exists || err != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)

	for _, invocation := range invocations {
		for _, artifact := range invocation.artifacts {
			switch artifact.storage {
			case cacheOnlyStorage:
				require.NoError(t, te.GetBlobstore().DeleteBlob(ctx, artifact.blobPath))
			case gcsOnlyStorage:
				require.NoError(t, te.GetCache().Delete(ownerContext(invocation.owner), artifact.cacheResource))
			}
		}
	}

	viewers := func(invocation *testInvocation) []testInvocationViewer {
		if invocation.owner.isAnon {
			return []testInvocationViewer{
				{name: "authenticated", apiKey: outsiderAPIKey, groupID: "GR_OUTSIDER", shouldAllow: true, partition: pebble_cache.DefaultPartitionID},
				{name: "unauthenticated", shouldAllow: true},
			}
		}
		return []testInvocationViewer{
			{
				name:                     "same_group",
				apiKey:                   invocation.owner.apiKey,
				groupID:                  invocation.owner.groupID,
				shouldAllow:              true,
				hasInvocationGroupAccess: true,
				partition:                partitionID(invocation.owner),
				cacheEncryptionEnabled:   invocation.owner.encrypted,
			},
			{
				name:                     "allowed_group",
				apiKey:                   allowedAPIKeys[invocation.owner.groupID],
				groupID:                  "GR_ALLOWED_" + invocation.owner.groupID,
				shouldAllow:              true,
				hasInvocationGroupAccess: true,
				partition:                pebble_cache.DefaultPartitionID,
			},
			{name: "outside_group", apiKey: outsiderAPIKey, groupID: "GR_OUTSIDER", shouldAllow: invocation.public, partition: pebble_cache.DefaultPartitionID},
			{name: "anonymous", shouldAllow: invocation.public},
		}
	}
	download := func(apiKey string, invocation *testInvocation, artifact *testInvocationArtifact) (int, []byte) {
		q := url.Values{}
		q.Set("invocation_id", invocation.id)
		q.Set("bytestream_url", artifact.bytestreamURL)
		q.Set("filename", artifact.name)
		req, err := http.NewRequest(http.MethodGet, baseURL.String()+"/file/download?"+q.Encode(), nil)
		require.NoError(t, err)
		if apiKey != "" {
			req.Header.Set(authutil.APIKeyHeader, apiKey)
		}
		rsp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer rsp.Body.Close()
		body, err := io.ReadAll(rsp.Body)
		require.NoError(t, err)
		return rsp.StatusCode, body
	}
	skippedTestCases := []skippedDownloadTestCase{
		// TODO: ANON invocation artifacts should be accessible by authenticated
		// users. Currently, these have to be viewed in incognito, which is kind
		// of inconvenient.
		{invocationOwner: "ANON", visibility: "public", accessVia: "authenticated", artifactStorage: cacheOnlyStorage},

		// TODO: if the authenticated user doesn't have the invocation owner
		// group ID as their selected group ID, but they have access to the owner
		// group via allowed_groups, they should still be able to view CAS artifacts
		// from the invocation. Currently this only works if the authenticated group
		// ID and invocation owner group ID resolve to the same cache partition and
		// compatible cache encryption config. We should make it work even when the
		// selected group uses a different cache partition or encryption config.
		{invocationOwner: "default-encrypted-org", visibility: "private", accessVia: "allowed_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "default-encrypted-org", visibility: "public", accessVia: "allowed_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part1-encrypted-org", visibility: "private", accessVia: "allowed_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part1-encrypted-org", visibility: "public", accessVia: "allowed_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part2-unencrypted-org", visibility: "private", accessVia: "allowed_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part2-unencrypted-org", visibility: "public", accessVia: "allowed_group", artifactStorage: cacheOnlyStorage},

		// TODO: for public invocations, if an invocation artifact is only
		// present in the CAS (i.e. we failed to store it to blobstore), check
		// whether it is a "persistable" artifact (i.e. one that *would* have
		// been written to blobstore), and if so, grant access to it.
		{invocationOwner: "default-org", visibility: "public", accessVia: "anonymous", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "default-encrypted-org", visibility: "public", accessVia: "anonymous", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part1-encrypted-org", visibility: "public", accessVia: "anonymous", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part2-unencrypted-org", visibility: "public", accessVia: "anonymous", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "default-encrypted-org", visibility: "public", accessVia: "outside_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part1-encrypted-org", visibility: "public", accessVia: "outside_group", artifactStorage: cacheOnlyStorage},
		{invocationOwner: "nondefault-part2-unencrypted-org", visibility: "public", accessVia: "outside_group", artifactStorage: cacheOnlyStorage},
	}
	shouldSkipDownload := func(download skippedDownloadTestCase) bool {
		return slices.Contains(skippedTestCases, download)
	}
	isCacheBackedArtifact := func(artifact *testInvocationArtifact) bool {
		return artifact.storage == cacheAndBlobstoreStorage || artifact.storage == cacheOnlyStorage
	}
	includeDownloadCase := func(invocation *testInvocation, artifact *testInvocationArtifact, viewer testInvocationViewer) bool {
		// Include every public invocation case in the matrix.
		if invocation.public {
			return true
		}

		// Include non cache backed artifacts because their behavior does not depend
		// on cache partition sharing.
		if !isCacheBackedArtifact(artifact) {
			return true
		}

		// Include anonymous private access because it exercises invocation ACLs,
		// not shared cache partition behavior.
		if viewer.apiKey == "" {
			return true
		}

		// Include direct and allowed_groups access because those cases should be
		// authorized through the invocation owner group.
		if viewer.hasInvocationGroupAccess {
			return true
		}

		// Omit private cache backed cases where the viewer does not have invocation
		// group access but shares the invocation owner's unencrypted cache partition;
		// those exercise the "knowledge of hash" cache model rather than invocation
		// artifact access.
		if partitionID(invocation.owner) == viewer.partition &&
			!invocation.owner.encrypted &&
			!viewer.cacheEncryptionEnabled {
			return false
		}

		return true
	}

	for _, invocation := range invocations {
		visibility := "private"
		if invocation.public {
			visibility = "public"
		}
		for _, artifact := range invocation.artifacts {
			for _, viewer := range viewers(invocation) {
				if !includeDownloadCase(invocation, artifact, viewer) {
					continue
				}
				testName := fmt.Sprintf(
					"Invocation=%s,%s/ArtifactStorage=%s/ArtifactAccessedVia=%s",
					invocation.owner.name,
					visibility,
					artifact.storage,
					viewer.name,
				)
				t.Run(testName, func(t *testing.T) {
					downloadSpec := skippedDownloadTestCase{
						invocationOwner: invocation.owner.name,
						visibility:      visibility,
						accessVia:       viewer.name,
						artifactStorage: artifact.storage,
					}
					if shouldSkipDownload(downloadSpec) {
						t.Skipf("skipping currently failing characterization case: %+v", downloadSpec)
					}
					statusCode, body := download(viewer.apiKey, invocation, artifact)
					if !viewer.shouldAllow {
						if viewer.groupID == "" && !invocation.public {
							// TODO: return 403 instead of 500 for anonymous access to
							// private invocation artifacts.
							require.Contains(
								t,
								[]int{http.StatusForbidden, http.StatusInternalServerError},
								statusCode,
							)
							return
						}
						require.Equal(
							t,
							http.StatusForbidden,
							statusCode,
						)
						return
					}
					require.Equal(
						t,
						http.StatusOK,
						statusCode,
					)
					require.Equal(
						t,
						artifact.contents,
						body,
					)
				})
			}
		}
	}
}

func buildStartedEvent(apiKey string) *bespb.BuildEvent {
	options := "--remote_upload_local_results"
	if apiKey != "" {
		options += fmt.Sprintf(" --remote_header='%s=%s'", authutil.APIKeyHeader, apiKey)
	}
	return &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{OptionsDescription: options},
		},
	}
}

func buildMetadataEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_BuildMetadata{
			BuildMetadata: &bespb.BuildMetadata{
				Metadata: map[string]string{"VISIBILITY": "PUBLIC"},
			},
		},
	}
}

func buildToolLogsEvent(logs []*bespb.File) *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_BuildToolLogs{
			BuildToolLogs: &bespb.BuildToolLogs{Log: logs},
		},
	}
}

func buildFinishedEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{}},
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{ExitCode: &bespb.BuildFinished_ExitCode{}},
		},
	}
}
