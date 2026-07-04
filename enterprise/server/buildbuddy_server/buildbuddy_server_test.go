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
		name    string
		status  grpb.Group_GroupStatus
		allowed bool
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
			if tc.allowed {
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
	// - part1 (org-specific partition; encryption is enabled)
	// - part2 (org-specific partition; encryption is not enabled)
	// Use local key provider / testonly key provider. Do not try to use real
	// GCS / AWS.
	//
	// Create orgs with different cache setups:
	// - Group GR1: default cache partition; cache encryption disabled
	// - Group GR2: default cache partition; cache encryption enabled
	// - Group GR3: part1 cache partition; cache encryption enabled
	// - Group GR4: part2 cache partition; cache encryption disabled
	//
	// Create fake invocations using build_event_publisher:
	// - One ANON invocation
	// - Two invocations for each org:
	//   - One private invocation
	//   - One public invocation
	//
	// Each invocation should attach three File artifacts, referenced via the
	// BES stream (using SHA256 digest function / empty instance name):
	// - One File artifact that successfully gets uploaded to cache and blobstore
	//   File contents: "${invocation_id}_persisted_true_cached_true.txt"
	// - One File artifact that gets uploaded to cache, but not blobstore
	//   (simulate this by deleting it from blobstore after publishing the invocation):
	//   File contents: "${invocation_id}_persisted_false_cached_true.txt"
	// - One File artifact that was uploaded to blobstore but expired from cache
	//   (simulate this by calling DeleteFile on the cache, assuming this works for pebble.
	//   if not, just don't ever upload it, and write it to blobstore manually)
	//   File contents: "${invocation_id}_persisted_true_cached_false.txt"
	//
	// Then, we test /file/download access. Baseline auth paths exercise
	// cache-only and blobstore-only artifacts (GCS in production). Cases that
	// specifically vary cache partition and encryption settings only use the
	// cache-only artifact. A small separate section covers artifacts that are
	// present in both blobstore and cache.
	//
	// 1. Artifact owner group ID
	// 2. Invocation artifact digest.
	// 3. Reader group and reader auxiliary groups:
	//   For group-owned (authenticated) invocations:
	//     a. Reader group matches the invocation owner group
	//     b. Reader group is a representative non-owner group, but reader
	//        auxiliary groups contain the invocation owner group
	//     c. Reader group is a representative non-owner group, and the user
	//        does not have access to the invocation owner group
	//     d. User is unauthenticated (anonymous)
	//   For ANON invocations:
	//     a. User is authenticated in each representative reader group
	//     b. User is unauthenticated

	ctx := context.Background()
	flags.Set(t, "cache_stats_finalization_delay", time.Millisecond)

	type testInvocationOwner struct {
		slug      string
		groupID   string
		apiKey    string
		userID    string
		encrypted bool
		isAnon    bool
	}
	type testInvocationArtifact struct {
		name          string
		persisted     bool
		cached        bool
		contents      []byte
		bytestreamURL string
		cacheResource *rspb.ResourceName
		blobPath      string
	}
	type invocation struct {
		id        string
		owner     testInvocationOwner
		public    bool
		artifacts []*testInvocationArtifact
	}
	type artifactPersistence struct {
		persisted bool
		cached    bool
	}
	type testDownload struct {
		owner     string
		public    bool
		reader    string
		readerAux string
		persisted bool
		cached    bool
		allowed   bool
	}
	type testDownloadKey struct {
		owner     string
		public    bool
		reader    string
		readerAux string
		persisted bool
		cached    bool
	}

	te := enterprise_testenv.New(t)

	// Set up invocation owner groups with different cache partition and cache
	// encryption settings.
	groupOwners := []testInvocationOwner{
		{slug: "d", groupID: "GR1", apiKey: "APIKEY_DEFAULT_ORG", userID: "US_DEFAULT_ORG"},
		{slug: "d-enc", groupID: "GR2", apiKey: "APIKEY_DEFAULT_ENCRYPTED_ORG", userID: "US_DEFAULT_ENCRYPTED_ORG", encrypted: true},
		{slug: "p1-enc", groupID: "GR3", apiKey: "APIKEY_PART1_ENCRYPTED_ORG", userID: "US_PART1_ENCRYPTED_ORG", encrypted: true},
		{slug: "p2-noenc", groupID: "GR4", apiKey: "APIKEY_PART2_UNENCRYPTED_ORG", userID: "US_PART2_UNENCRYPTED_ORG"},
	}
	anonOwner := testInvocationOwner{slug: "ANON", groupID: interfaces.AuthAnonymousUser, isAnon: true}
	outsider := testInvocationOwner{slug: "d-outsider", groupID: "GR5", apiKey: "APIKEY_OUTSIDE_DEFAULT_ORG", userID: "US_OUTSIDE_DEFAULT_ORG"}
	readers := append([]testInvocationOwner{}, groupOwners...)
	readers = append(readers, outsider)
	readersBySlug := map[string]testInvocationOwner{}
	for _, reader := range readers {
		readersBySlug[reader.slug] = reader
	}

	testUsers := map[string]interfaces.UserInfo{}
	// Create an owner user whose authenticated group is the owner group.
	for _, owner := range groupOwners {
		ownerUser := testauth.User(owner.userID, owner.groupID)
		ownerUser.CacheEncryptionEnabled = owner.encrypted
		testUsers[owner.apiKey] = ownerUser
	}
	// Create an authenticated user with default cache settings that has no
	// access to any owner group.
	testUsers[outsider.apiKey] = testauth.User(outsider.userID, outsider.groupID)

	// Map reader auxiliary group ID -> reader group ID -> API key for a user
	// whose reader group differs from the owner group but whose auxiliary
	// groups contain the owner group.
	readerAuxAPIKeys := map[string]map[string]string{}
	for _, owner := range groupOwners {
		readerAuxAPIKeys[owner.groupID] = map[string]string{}
		for _, reader := range readers {
			if reader.groupID == owner.groupID {
				continue
			}
			apiKey := fmt.Sprintf("APIKEY_READER_%s_AUX_%s", reader.groupID, owner.groupID)
			user := testauth.User("US_READER_"+reader.groupID+"_AUX_"+owner.groupID, reader.groupID)
			user.CacheEncryptionEnabled = reader.encrypted
			user.AllowedGroups = append(user.AllowedGroups, owner.groupID)
			testUsers[apiKey] = user
			readerAuxAPIKeys[owner.groupID][reader.groupID] = apiKey
		}
	}

	auth := testauth.NewTestAuthenticator(t, testUsers)
	te.SetAuthenticator(auth)

	for _, owner := range groupOwners {
		require.NoError(t, te.GetDBHandle().NewQuery(ctx, "create_test_group").Create(&tables.Group{
			GroupID:       owner.groupID,
			URLIdentifier: owner.slug,
		}))
	}

	// Set up encryption + enable for orgs that should have it enabled.
	testencryption.Setup(t, te)
	for _, owner := range groupOwners {
		if !owner.encrypted {
			continue
		}
		authCtx := auth.AuthContextFromAPIKey(t.Context(), owner.apiKey)
		testencryption.EnableForAuthenticatedGroup(t, authCtx, te)
	}

	// Set up partitioned pebble cache
	partitionSizeBytes := int64(1_000_000_000)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		Partitions: []disk.Partition{
			{ID: "anon", MaxSizeBytes: partitionSizeBytes},
			{ID: pebble_cache.DefaultPartitionID, MaxSizeBytes: partitionSizeBytes},
			{ID: "part1", MaxSizeBytes: partitionSizeBytes},
			{ID: "part2", MaxSizeBytes: partitionSizeBytes},
		},
		PartitionMappings: []disk.PartitionMapping{
			{GroupID: interfaces.AuthAnonymousUser, PartitionID: "anon"},
			{GroupID: "GR3", PartitionID: "part1"},
			{GroupID: "GR4", PartitionID: "part2"},
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

	// Set up BES service
	handler := build_event_handler.NewBuildEventHandler(te)
	te.SetBuildEventHandler(handler)
	buildEventServer, err := build_event_server.NewBuildEventProtocolServer(te, false /*=synchronous*/)
	require.NoError(t, err)
	pepb.RegisterPublishBuildEventServer(te.GetGRPCServer(), buildEventServer)

	// Set up cache services
	require.NoError(t, gs.Start())
	conn, err := grpc_client.DialSimpleWithoutPooling(fmt.Sprintf("grpc://localhost:%d", grpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	testcache.RegisterClients(te, conn)

	mux := http.NewServeMux()
	mux.Handle("/file/download", interceptors.WrapAuthenticatedExternalHandler(te, te.GetBuildBuddyServer()))
	baseURL := testhttp.StartServer(t, mux)

	digestFunction := repb.DigestFunction_SHA256
	artifactLocations := []artifactPersistence{
		{persisted: true, cached: true},
		{persisted: false, cached: true},
		{persisted: true, cached: false},
	}
	newArtifact := func(invocationID string, location artifactPersistence) *testInvocationArtifact {
		name := fmt.Sprintf("persisted_%t_cached_%t.txt", location.persisted, location.cached)
		contents := []byte(invocationID + "_" + name)
		d, err := digest.Compute(bytes.NewReader(contents), digestFunction)
		require.NoError(t, err)
		rn := digest.NewCASResourceName(d, "", digestFunction)
		bytestreamURL := fmt.Sprintf("bytestream://localhost:%d/%s", grpcPort, rn.DownloadString())
		u, err := url.Parse(bytestreamURL)
		require.NoError(t, err)
		return &testInvocationArtifact{
			name:          name,
			persisted:     location.persisted,
			cached:        location.cached,
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
	publishInvocation := func(owner testInvocationOwner, public bool) *invocation {
		invocationID := uuid.NewString()
		invocation := &invocation{
			id:     invocationID,
			owner:  owner,
			public: public || owner.isAnon,
		}
		for _, location := range artifactLocations {
			invocation.artifacts = append(invocation.artifacts, newArtifact(invocationID, location))
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

	invocations := []*invocation{publishInvocation(anonOwner, true)}
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
	}, 15*time.Second, 10*time.Millisecond)

	for _, invocation := range invocations {
		for _, artifact := range invocation.artifacts {
			if !artifact.persisted {
				require.NoError(t, te.GetBlobstore().DeleteBlob(ctx, artifact.blobPath))
			}
			if !artifact.cached {
				require.NoError(t, te.GetCache().Delete(ownerContext(invocation.owner), artifact.cacheResource))
			}
		}
	}

	download := func(apiKey string, invocation *invocation, artifact *testInvocationArtifact) (int, []byte) {
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

	// Test matrix. Rows that vary cache partition or encryption settings use
	// the cache-only artifact; a smaller number of representative cases cover
	// blobstore auth behavior.
	testDownloads := []testDownload{
		// ANON invocations are public, so authenticated readers in any reader
		// group, as well as unauthenticated readers, should be allowed.
		{owner: "ANON", reader: "d", cached: true, allowed: true},
		{owner: "ANON", reader: "d-enc", cached: true, allowed: true},
		{owner: "ANON", reader: "p1-enc", cached: true, allowed: true},
		{owner: "ANON", reader: "p2-noenc", cached: true, allowed: true},
		{owner: "ANON", reader: "d-outsider", cached: true, allowed: true},
		{owner: "ANON", reader: "", cached: true, allowed: true},
		{owner: "ANON", reader: "", persisted: true, allowed: true},

		// Private invocations owned by the org in the default cache partition
		// with encryption disabled should allow the owner reader group and
		// readers with the owner group as a reader auxiliary group, but deny
		// unrelated reader groups and unauthenticated readers.
		{owner: "d", reader: "d", cached: true, allowed: true},
		{owner: "d", reader: "d", persisted: true, allowed: true},
		{owner: "d", reader: "d-enc", readerAux: "d", cached: true, allowed: true},
		{owner: "d", reader: "p1-enc", readerAux: "d", cached: true, allowed: true},
		{owner: "d", reader: "p2-noenc", readerAux: "d", cached: true, allowed: true},
		{owner: "d", reader: "d-outsider", readerAux: "d", cached: true, allowed: true},
		{owner: "d", reader: "d-outsider", readerAux: "d", persisted: true, allowed: true},
		{owner: "d", reader: "d-enc", cached: true, allowed: false},
		{owner: "d", reader: "p1-enc", cached: true, allowed: false},
		{owner: "d", reader: "p2-noenc", cached: true, allowed: false},
		{owner: "d", reader: "d-outsider", cached: true, allowed: false},
		{owner: "d", reader: "d-outsider", persisted: true, allowed: false},
		{owner: "d", reader: "", cached: true, allowed: false},
		{owner: "d", reader: "", persisted: true, allowed: false},

		// Public invocations owned by the org in the default cache partition
		// with encryption disabled should allow every reader, including
		// unrelated reader groups and unauthenticated readers.
		{owner: "d", public: true, reader: "d", cached: true, allowed: true},
		{owner: "d", public: true, reader: "d", persisted: true, allowed: true},
		{owner: "d", public: true, reader: "d-enc", readerAux: "d", cached: true, allowed: true},
		{owner: "d", public: true, reader: "p1-enc", readerAux: "d", cached: true, allowed: true},
		{owner: "d", public: true, reader: "p2-noenc", readerAux: "d", cached: true, allowed: true},
		{owner: "d", public: true, reader: "d-enc", cached: true, allowed: true},
		{owner: "d", public: true, reader: "p1-enc", cached: true, allowed: true},
		{owner: "d", public: true, reader: "p2-noenc", cached: true, allowed: true},
		{owner: "d", public: true, reader: "", cached: true, allowed: true},
		{owner: "d", public: true, reader: "", persisted: true, allowed: true},

		// Private invocations owned by the org in the default cache partition
		// with encryption enabled should enforce the same owner group access
		// rule.
		{owner: "d-enc", reader: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", reader: "d", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", reader: "p1-enc", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", reader: "p2-noenc", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", reader: "d-outsider", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", reader: "d", cached: true, allowed: false},
		{owner: "d-enc", reader: "p1-enc", cached: true, allowed: false},
		{owner: "d-enc", reader: "p2-noenc", cached: true, allowed: false},
		{owner: "d-enc", reader: "d-outsider", cached: true, allowed: false},
		{owner: "d-enc", reader: "", cached: true, allowed: false},

		// Public invocations owned by the org in the default cache partition
		// with encryption enabled should allow every reader.
		// These rows isolate public cache-only access with default partition
		// encryption enabled.
		{owner: "d-enc", public: true, reader: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "d", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "p1-enc", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "p2-noenc", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "d-outsider", readerAux: "d-enc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "d", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "p1-enc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "p2-noenc", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "d-outsider", cached: true, allowed: true},
		{owner: "d-enc", public: true, reader: "", cached: true, allowed: true},

		// Private invocations owned by the org in the part1 cache partition
		// with encryption enabled should enforce the owner group access rule.
		{owner: "p1-enc", reader: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", reader: "d", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", reader: "d-enc", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", reader: "p2-noenc", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", reader: "d-outsider", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", reader: "d", cached: true, allowed: false},
		{owner: "p1-enc", reader: "d-enc", cached: true, allowed: false},
		{owner: "p1-enc", reader: "p2-noenc", cached: true, allowed: false},
		{owner: "p1-enc", reader: "d-outsider", cached: true, allowed: false},
		{owner: "p1-enc", reader: "", cached: true, allowed: false},

		// Public invocations owned by the org in the part1 cache partition
		// with encryption enabled should allow every reader. These rows isolate
		// public cache-only access with encrypted part1 cache storage.
		{owner: "p1-enc", public: true, reader: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "d", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "d-enc", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "p2-noenc", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "d-outsider", readerAux: "p1-enc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "d", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "d-enc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "p2-noenc", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "d-outsider", cached: true, allowed: true},
		{owner: "p1-enc", public: true, reader: "", cached: true, allowed: true},

		// Private invocations owned by the org in the part2 cache partition
		// with encryption disabled should enforce the owner group access rule.
		{owner: "p2-noenc", reader: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", reader: "d", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", reader: "d-enc", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", reader: "p1-enc", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", reader: "d-outsider", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", reader: "d", cached: true, allowed: false},
		{owner: "p2-noenc", reader: "d-enc", cached: true, allowed: false},
		{owner: "p2-noenc", reader: "p1-enc", cached: true, allowed: false},
		{owner: "p2-noenc", reader: "d-outsider", cached: true, allowed: false},
		{owner: "p2-noenc", reader: "", cached: true, allowed: false},

		// Public invocations owned by the org in the part2 cache partition with
		// encryption disabled should allow every reader. These rows isolate
		// public cache-only access with unencrypted part2 cache storage.
		{owner: "p2-noenc", public: true, reader: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "d", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "d-enc", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "p1-enc", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "d-outsider", readerAux: "p2-noenc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "d", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "d-enc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "p1-enc", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "d-outsider", cached: true, allowed: true},
		{owner: "p2-noenc", public: true, reader: "", cached: true, allowed: true},

		// Artifacts present in both blobstore and cache should be downloadable
		// through representative private owner and public anonymous access.
		{owner: "d", reader: "d", persisted: true, cached: true, allowed: true},
		{owner: "d", public: true, reader: "", persisted: true, cached: true, allowed: true},
	}

	// Define skipped test cases that currently fail their expected behavior
	// assertions. Remove entries as the implementation catches up to the table.
	skippedTestCases := []testDownloadKey{
		// TODO: ANON invocation artifacts should be accessible by authenticated
		// users. Currently, these have to be viewed in incognito, which is kind
		// of inconvenient.
		{owner: "ANON", reader: "d", cached: true},
		{owner: "ANON", reader: "d-enc", cached: true},
		{owner: "ANON", reader: "p1-enc", cached: true},
		{owner: "ANON", reader: "p2-noenc", cached: true},
		{owner: "ANON", reader: "d-outsider", cached: true},

		// TODO: if the reader doesn't have the invocation owner group ID as
		// their reader group ID, but they have access to the owner group via
		// reader auxiliary groups, they should still be able to view CAS
		// artifacts from the invocation. Currently this only works if the reader
		// group ID and invocation owner group ID have the same cache partition
		// and cache encryption status (enabled/disabled). We should make it work
		// even when they use different cache partitions or
		// different cache encryption settings.
		{owner: "d", reader: "d-enc", readerAux: "d", cached: true},
		{owner: "d", reader: "p1-enc", readerAux: "d", cached: true},
		{owner: "d", reader: "p2-noenc", readerAux: "d", cached: true},
		{owner: "d", public: true, reader: "d-enc", readerAux: "d", cached: true},
		{owner: "d", public: true, reader: "p1-enc", readerAux: "d", cached: true},
		{owner: "d", public: true, reader: "p2-noenc", readerAux: "d", cached: true},
		{owner: "d-enc", reader: "d", readerAux: "d-enc", cached: true},
		{owner: "d-enc", reader: "p1-enc", readerAux: "d-enc", cached: true},
		{owner: "d-enc", reader: "p2-noenc", readerAux: "d-enc", cached: true},
		{owner: "d-enc", reader: "d-outsider", readerAux: "d-enc", cached: true},
		{owner: "d-enc", public: true, reader: "d", readerAux: "d-enc", cached: true},
		{owner: "d-enc", public: true, reader: "p1-enc", readerAux: "d-enc", cached: true},
		{owner: "d-enc", public: true, reader: "p2-noenc", readerAux: "d-enc", cached: true},
		{owner: "d-enc", public: true, reader: "d-outsider", readerAux: "d-enc", cached: true},
		{owner: "p1-enc", reader: "d", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", reader: "d-enc", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", reader: "p2-noenc", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", reader: "d-outsider", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", public: true, reader: "d", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", public: true, reader: "d-enc", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", public: true, reader: "p2-noenc", readerAux: "p1-enc", cached: true},
		{owner: "p1-enc", public: true, reader: "d-outsider", readerAux: "p1-enc", cached: true},
		{owner: "p2-noenc", reader: "d", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", reader: "d-enc", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", reader: "p1-enc", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", reader: "d-outsider", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", public: true, reader: "d", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", public: true, reader: "d-enc", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", public: true, reader: "p1-enc", readerAux: "p2-noenc", cached: true},
		{owner: "p2-noenc", public: true, reader: "d-outsider", readerAux: "p2-noenc", cached: true},

		// TODO: private invocation artifacts should return 403 when the
		// authenticated user does not have access to the invocation owner group.
		// Same partition, unencrypted cache cases currently succeed if the
		// authenticated user knows the artifact digest.
		{owner: "d", reader: "d-outsider", cached: true},

		// TODO: for public invocations, if an invocation artifact is only
		// present in the CAS (i.e. we failed to store it to blobstore), check
		// whether it is a persistable artifact (i.e. one that would have been
		// written to blobstore), and if so, grant access to it.
		{owner: "d", public: true, reader: "", cached: true},
		{owner: "d-enc", public: true, reader: "", cached: true},
		{owner: "p1-enc", public: true, reader: "", cached: true},
		{owner: "p2-noenc", public: true, reader: "", cached: true},
		{owner: "d", public: true, reader: "d-enc", cached: true},
		{owner: "d", public: true, reader: "p1-enc", cached: true},
		{owner: "d", public: true, reader: "p2-noenc", cached: true},
		{owner: "d-enc", public: true, reader: "d", cached: true},
		{owner: "d-enc", public: true, reader: "p1-enc", cached: true},
		{owner: "d-enc", public: true, reader: "p2-noenc", cached: true},
		{owner: "d-enc", public: true, reader: "d-outsider", cached: true},
		{owner: "p1-enc", public: true, reader: "d", cached: true},
		{owner: "p1-enc", public: true, reader: "d-enc", cached: true},
		{owner: "p1-enc", public: true, reader: "p2-noenc", cached: true},
		{owner: "p1-enc", public: true, reader: "d-outsider", cached: true},
		{owner: "p2-noenc", public: true, reader: "d", cached: true},
		{owner: "p2-noenc", public: true, reader: "d-enc", cached: true},
		{owner: "p2-noenc", public: true, reader: "p1-enc", cached: true},
		{owner: "p2-noenc", public: true, reader: "d-outsider", cached: true},
	}

	publicForDownload := func(testDownload testDownload) bool {
		return testDownload.owner == anonOwner.slug || testDownload.public
	}
	groupIDForSlug := func(slug string) string {
		if slug == "" {
			return ""
		}
		group, ok := readersBySlug[slug]
		require.True(t, ok, "download test references unknown group slug")
		return group.groupID
	}
	keyForTestDownload := func(testDownload testDownload) testDownloadKey {
		return testDownloadKey{
			owner:     testDownload.owner,
			public:    publicForDownload(testDownload),
			reader:    testDownload.reader,
			readerAux: testDownload.readerAux,
			persisted: testDownload.persisted,
			cached:    testDownload.cached,
		}
	}
	normalizeTestDownloadKey := func(key testDownloadKey) testDownloadKey {
		if key.owner == anonOwner.slug {
			key.public = true
		}
		return key
	}
	for i := range skippedTestCases {
		skippedTestCases[i] = normalizeTestDownloadKey(skippedTestCases[i])
	}
	shouldSkip := func(testDownload testDownload) bool {
		key := keyForTestDownload(testDownload)
		return slices.Contains(skippedTestCases, key)
	}
	getInvocationForDownload := func(testDownload testDownload) *invocation {
		public := publicForDownload(testDownload)
		for _, invocation := range invocations {
			if invocation.owner.slug == testDownload.owner && invocation.public == public {
				return invocation
			}
		}
		return nil
	}
	getArtifactForDownload := func(invocation *invocation, testDownload testDownload) *testInvocationArtifact {
		for _, artifact := range invocation.artifacts {
			if artifact.persisted == testDownload.persisted && artifact.cached == testDownload.cached {
				return artifact
			}
		}
		return nil
	}
	apiKeyForDownload := func(testDownload testDownload, invocation *invocation) string {
		if testDownload.reader == "" {
			return ""
		}
		readerGID := groupIDForSlug(testDownload.reader)
		if testDownload.readerAux != "" {
			readerAuxGID := groupIDForSlug(testDownload.readerAux)
			require.Equal(t, invocation.owner.groupID, readerAuxGID, "download test reader auxiliary group must be invocation owner")
			return readerAuxAPIKeys[readerAuxGID][readerGID]
		}
		if readerGID == invocation.owner.groupID {
			return invocation.owner.apiKey
		}
		reader, ok := readersBySlug[testDownload.reader]
		require.True(t, ok, "download test references unknown reader")
		return reader.apiKey
	}

	for _, testDownload := range testDownloads {
		public := publicForDownload(testDownload)
		name := fmt.Sprintf(
			"Invocation:Owner=%s,Public=%t/Artifact:Persisted=%t,Cached=%t/Reader=%s,ReaderAux=%s",
			testDownload.owner,
			public,
			testDownload.persisted,
			testDownload.cached,
			testDownload.reader,
			testDownload.readerAux,
		)
		t.Run(name, func(t *testing.T) {
			if shouldSkip(testDownload) {
				t.Skipf("skipping currently failing characterization case: %+v", keyForTestDownload(testDownload))
			}
			invocation := getInvocationForDownload(testDownload)
			require.NotNil(t, invocation, "download test references unknown invocation")
			artifact := getArtifactForDownload(invocation, testDownload)
			require.NotNil(t, artifact, "download test references unknown artifact")

			statusCode, body := download(apiKeyForDownload(testDownload, invocation), invocation, artifact)
			if !testDownload.allowed {
				if testDownload.reader == "" && !invocation.public {
					// TODO: return 403 instead of 500 for anonymous access to
					// private invocation artifacts.
					require.Contains(t, []int{http.StatusForbidden, http.StatusInternalServerError}, statusCode)
					return
				}
				require.Equal(t, http.StatusForbidden, statusCode)
				return
			}
			require.Equal(t, http.StatusOK, statusCode)
			require.Equal(t, artifact.contents, body)
		})
	}
}

func buildStartedEvent(apiKey string) *bespb.BuildEvent {
	options := "--remote_upload_local_results"
	if apiKey != "" {
		options += fmt.Sprintf(" --remote_header='%s=%s'", authutil.APIKeyHeader, apiKey)
	}
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{OptionsDescription: options},
		},
	}
}

func buildMetadataEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{
			BuildMetadata: &bespb.BuildMetadata{
				Metadata: map[string]string{"VISIBILITY": "PUBLIC"},
			},
		},
	}
}

func buildToolLogsEvent(logs []*bespb.File) *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		Payload: &bespb.BuildEvent_BuildToolLogs{
			BuildToolLogs: &bespb.BuildToolLogs{Log: logs},
		},
	}
}

func buildFinishedEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{ExitCode: &bespb.BuildFinished_ExitCode{}},
		},
	}
}
