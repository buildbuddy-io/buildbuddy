package permissions_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/permissionstest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

// Each identity gets a new browser session, not just a cleared auth cookie:
// selected organization and other auth state also live in local/session storage.
func login(t *testing.T, f *permissionstest.Fixture, name string) *webtester.WebTester {
	t.Helper()
	wt := webtester.New(t)
	wt.Get(f.LoginURL())
	wt.FindWithTimeout(fmt.Sprintf(`select[name="user"] option[value="%s"]`, f.Users[name].Subject), 10*time.Second).Click()
	wt.Find(`button[type="submit"]`).Click()
	wt.FindWithTimeout(`[debug-id="org-picker"]`, 10*time.Second)
	require.Equal(t, f.Users[name].Email, wt.Find(`.org-picker-profile-name`).GetAttribute("title"))
	return wt
}

func checkSettings(t *testing.T, f *permissionstest.Fixture, wt *webtester.WebTester, admin bool) {
	t.Helper()
	wt.Get(f.App.HTTPURL() + "/settings/org/api-keys")
	// Wait for the response before asserting that controls are absent.
	wt.FindWithTimeout(`.api-keys-list`, 10*time.Second)
	for _, selector := range []string{
		`.settings-tabs [href="/settings/org/details"]`,
		`.settings-tabs [href="/settings/org/members"]`,
		`[debug-id="create-new-api-key"]`,
	} {
		require.Equal(t, admin, len(wt.FindAll(selector)) > 0, "selector %s", selector)
	}
	for _, path := range []string{"/settings/org/details", "/settings/org/members"} {
		wt.Get(f.App.HTTPURL() + path)
		if admin {
			selector := `.organization-edit-form`
			if path == "/settings/org/members" {
				selector = `.org-members-list`
			}
			wt.FindWithTimeout(selector, 10*time.Second)
		} else {
			wt.FindWithTimeout(`.api-keys-list`, 10*time.Second)
			require.Equal(t, f.App.HTTPURL()+"/settings/", wt.CurrentURL())
			wt.AssertNotFound(`.organization-edit-form, .org-members`)
		}
	}
}

func TestRoleSettings(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)
	f := permissionstest.New(t)
	// Seed distinguishable keys so an empty list cannot satisfy isolation checks.
	for _, key := range []*tables.APIKey{
		{APIKeyID: "AK-browser-a-private", GroupID: f.OrgA, Label: "org-a-admin-only", Perms: perms.GROUP_READ | perms.GROUP_WRITE},
		{APIKeyID: "AK-browser-a-shared", GroupID: f.OrgA, Label: "org-a-shared", Perms: perms.GROUP_READ | perms.GROUP_WRITE, VisibleToDevelopers: true},
		{APIKeyID: "AK-browser-b-private", GroupID: f.OrgB, Label: "org-b-admin-only", Perms: perms.GROUP_READ | perms.GROUP_WRITE},
	} {
		require.NoError(t, f.App.DB().Create(key).Error)
	}
	for _, tc := range []struct {
		name  string
		admin bool
	}{
		{"admin", true}, {"developer", false}, {"writer", false}, {"reader", false}, {"outsider", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wt := login(t, f, tc.name)
			checkSettings(t, f, wt, tc.admin)
			wt.Get(f.App.HTTPURL() + "/settings/org/api-keys")
			keys := wt.FindWithTimeout(`.api-keys-list`, 10*time.Second).Text()
			if tc.name == "outsider" {
				require.Contains(t, keys, "org-b-admin-only")
				require.NotContains(t, keys, "org-a-")
			} else {
				require.Contains(t, keys, "org-a-shared")
				require.NotContains(t, keys, "org-b-")
				if tc.admin {
					require.Contains(t, keys, "org-a-admin-only")
				} else {
					require.NotContains(t, keys, "org-a-admin-only")
				}
			}
			// Every member can manage their own personal keys, including Readers.
			wt.Get(f.App.HTTPURL() + "/settings/personal/api-keys")
			wt.FindWithTimeout(`.api-keys-list`, 10*time.Second)
			wt.FindByDebugID("create-new-api-key")
		})
	}
}

func selectOrg(t *testing.T, wt *webtester.WebTester, name string) {
	t.Helper()
	webtester.ExpandSidebarOptions(wt)
	wt.FindWithTimeout(`.org-list`, 10*time.Second)
	for _, item := range wt.FindAll(`.org-list [role="menuitem"]`) {
		if item.Text() == name {
			item.Click()
			require.Eventually(t, func() bool {
				orgs := wt.FindAll(`.org-picker-profile-org`)
				return len(orgs) == 1 && orgs[0].Text() == name
			}, 10*time.Second, 100*time.Millisecond)
			return
		}
	}
	t.Fatalf("organization %q missing from picker", name)
}

func TestOrganizationSwitching(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)
	f := permissionstest.New(t)
	wt := login(t, f, "dual")
	var a, b tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&a).Error)
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgB).Take(&b).Error)
	selectOrg(t, wt, b.Name)
	checkSettings(t, f, wt, true)
	// Prove an authorized UI mutation reaches the real DB and only that org.
	wt.Get(f.App.HTTPURL() + "/settings/org/details")
	field := wt.Find(`.organization-edit-form [name="name"]`)
	field.Clear()
	field.SendKeys("Updated organization B")
	wt.Find(`.organization-form-submit-button`).Click()
	wt.FindWithTimeout(`.form-success-message`, 10*time.Second)
	var updated tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgB).Take(&updated).Error)
	require.Equal(t, "Updated organization B", updated.Name)
	selectOrg(t, wt, a.Name)
	checkSettings(t, f, wt, false)
	var unchanged tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&unchanged).Error)
	require.Equal(t, a.Name, unchanged.Name)

	selectOrg(t, wt, updated.Name)
	checkSettings(t, f, wt, true)
	// Another administrator downgrades this user while its browser session
	// remains logged in. A reload must not retain the old admin controls.
	admin := f.Login(t, f.Users["outsider"])
	require.NoError(t, admin.RPC("UpdateGroupUsers", &grpb.UpdateGroupUsersRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: f.OrgB},
		GroupId:        f.OrgB,
		Update: []*grpb.UpdateGroupUsersRequest_Update{{
			UserId: &uidpb.UserId{Id: f.Users["dual"].ID},
			Role:   grpb.Group_READER_ROLE,
		}},
	}, &grpb.UpdateGroupUsersResponse{}))
	checkSettings(t, f, wt, false)
	require.Equal(t, updated.Name, wt.Find(`.org-picker-profile-org`).Text(), "downgrade must be observed in B, not by falling back to Reader org A")
}

func checkInvocationRPCs(t *testing.T, f *permissionstest.Fixture, name, own, other string) {
	t.Helper()
	c := f.Login(t, f.Users[name])
	group, foreignGroup := f.OrgA, f.OrgB
	if name == "outsider" {
		group, foreignGroup = foreignGroup, group
	}
	ctx := &ctxpb.RequestContext{GroupId: group}
	inv := &inpb.GetInvocationResponse{}
	require.NoError(t, c.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: ctx, Lookup: &inpb.InvocationLookup{InvocationId: own},
	}, inv))
	require.Len(t, inv.GetInvocation(), 1)
	require.Equal(t, own, inv.GetInvocation()[0].GetInvocationId())
	denied := &inpb.GetInvocationResponse{}
	err := c.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: ctx, Lookup: &inpb.InvocationLookup{InvocationId: other},
	}, denied)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Empty(t, denied.GetInvocation())

	logs := &elpb.GetEventLogChunkResponse{}
	require.NoError(t, c.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: ctx, InvocationId: own,
	}, logs))
	require.NotEmpty(t, logs.GetBuffer(), "positive control: real uploaded build logs")
	deniedLogs := &elpb.GetEventLogChunkResponse{}
	err = c.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: ctx, InvocationId: other,
	}, deniedLogs)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Empty(t, deniedLogs.GetBuffer())

	search := &inpb.SearchInvocationResponse{}
	require.NoError(t, c.RPC("SearchInvocation", &inpb.SearchInvocationRequest{
		RequestContext: ctx, Query: &inpb.InvocationQuery{GroupId: group},
	}, search))
	require.Len(t, search.GetInvocation(), 1)
	require.Equal(t, own, search.GetInvocation()[0].GetInvocationId())
	foreignSearch := &inpb.SearchInvocationResponse{}
	require.NoError(t, c.RPC("SearchInvocation", &inpb.SearchInvocationRequest{
		RequestContext: ctx, Query: &inpb.InvocationQuery{GroupId: foreignGroup},
	}, foreignSearch))
	require.Empty(t, foreignSearch.GetInvocation(), "query group substitution must not bypass row ACLs")
}

func TestInvocationVisibility(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)
	f := permissionstest.New(t)
	ws := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "permission_sentinel", outs = ["sentinel.txt"], cmd = "echo permission-sentinel > $@")`,
	})
	// Upload one invocation using the organization API key used by typical CI,
	// and one using a personal API key. This keeps the browser fixture small
	// while exercising visibility for both authentication paths.
	ids := map[string]string{}
	for _, name := range []string{"admin", "outsider"} {
		t.Run("upload_"+name, func(t *testing.T) {
			c := f.Login(t, f.Users[name])
			group := f.OrgA
			if name == "outsider" {
				group = f.OrgB
			}
			key := &akpb.CreateApiKeyResponse{}
			method := "CreateUserApiKey"
			req := &akpb.CreateApiKeyRequest{
				RequestContext: &ctxpb.RequestContext{GroupId: group},
				UserId:         f.Users[name].ID,
				Label:          "invocation-upload-personal",
				Capability:     []cappb.Capability{cappb.Capability_CAS_WRITE},
			}
			if name == "outsider" {
				method = "CreateApiKey"
				req.UserId = ""
				req.Label = "invocation-upload-org"
			}
			require.NoError(t, c.RPC(method, req, key))
			require.NotEmpty(t, key.GetApiKey().GetValue())
			args := append([]string{"//:permission_sentinel", "--remote_header=x-buildbuddy-api-key=" + key.GetApiKey().GetValue()}, f.App.BESBazelFlags()...)
			result := testbazel.Invoke(context.Background(), t, ws, "build", args...)
			require.NoError(t, result.Error, "Bazel failed: %s", result.Stderr)
			require.NotEmpty(t, result.InvocationID)
			ids[name] = result.InvocationID
		})
	}
	require.Len(t, ids, 2)
	for _, name := range []string{"admin", "developer", "writer", "reader", "outsider"} {
		t.Run(name, func(t *testing.T) {
			wt := login(t, f, name)
			own, other := ids["admin"], ids["outsider"]
			if name == "outsider" {
				own, other = other, own
			}
			checkInvocationRPCs(t, f, name, own, other)
			wt.Get(f.App.HTTPURL())
			require.Eventually(t, func() bool {
				return len(wt.FindAll(fmt.Sprintf(`[href="/invocation/%s"]`, own))) > 0
			}, 10*time.Second, 100*time.Millisecond)
			wt.AssertNotFound(fmt.Sprintf(`[href="/invocation/%s"]`, other))
			wt.Get(f.App.HTTPURL() + "/invocation/" + own)
			details := wt.FindWithTimeout(`[debug-id="invocation-details"]`, 10*time.Second).Text()
			require.Contains(t, details, "Succeeded")
			require.Contains(t, details, "permission_sentinel")
			wt.Get(f.App.HTTPURL() + "/invocation/" + other)
			denied := wt.FindWithTimeout(`[debug-id="invocation-not-found"]`, 10*time.Second).Text()
			require.Contains(t, denied, "Permission denied")
			wt.AssertNotFound(`[debug-id="invocation-details"]`)
			require.NotContains(t, wt.Find("body").Text(), "permission_sentinel")
			webtester.Logout(wt)
			wt.Get(f.App.HTTPURL() + "/invocation/" + own)
			wt.FindWithTimeout(`[debug-id="login-button"]`, 10*time.Second)
			wt.AssertNotFound(`[debug-id="invocation-details"]`)
		})
	}
	t.Run("anonymous_private", func(t *testing.T) {
		wt := webtester.New(t)
		wt.Get(f.App.HTTPURL() + "/invocation/" + ids["admin"])
		wt.FindWithTimeout(`[debug-id="login-button"]`, 10*time.Second)
		wt.AssertNotFound(`[debug-id="invocation-details"]`)
	})
	anon := f.AnonymousClient()
	privateInvocation := &inpb.GetInvocationResponse{}
	err := anon.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: anon.RequestContext,
		Lookup:         &inpb.InvocationLookup{InvocationId: ids["admin"]},
	}, privateInvocation)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Empty(t, privateInvocation.GetInvocation())
	privateLogs := &elpb.GetEventLogChunkResponse{}
	err = anon.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: anon.RequestContext,
		InvocationId:   ids["admin"],
	}, privateLogs)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Empty(t, privateLogs.GetBuffer())

	// Sharing changes the same existing resource through the production RPC, so
	// the successful public case is a positive control for the negative direct-link
	// assertions above. Enable the owning organization's sharing policy through
	// UpdateGroup while preserving its other fixture settings.
	admin := f.Login(t, f.Users["admin"])
	var org tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&org).Error)
	require.NoError(t, admin.RPC("UpdateGroup", &grpb.UpdateGroupRequest{
		RequestContext:                    &ctxpb.RequestContext{GroupId: f.OrgA},
		Id:                                f.OrgA,
		Name:                              org.Name,
		UrlIdentifier:                     org.URLIdentifier,
		SharingEnabled:                    true,
		UseGroupOwnedExecutors:            org.UseGroupOwnedExecutors,
		SuggestionPreference:              org.SuggestionPreference,
		UserOwnedKeysEnabled:              org.UserOwnedKeysEnabled,
		RestrictCleanWorkflowRunsToAdmins: org.RestrictCleanWorkflowRunsToAdmins,
		BotSuggestionsEnabled:             org.BotSuggestionsEnabled,
		DeveloperOrgCreationEnabled:       org.DeveloperOrgCreationEnabled,
		CodeSearchEnabled:                 org.CodeSearchEnabled,
	}, &grpb.UpdateGroupResponse{}))
	var sharingEnabled tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&sharingEnabled).Error)
	require.True(t, sharingEnabled.SharingEnabled)
	before := &inpb.GetInvocationResponse{}
	require.NoError(t, admin.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: f.OrgA},
		Lookup:         &inpb.InvocationLookup{InvocationId: ids["admin"]},
	}, before))
	require.Len(t, before.GetInvocation(), 1)
	acl := before.GetInvocation()[0].GetAcl()
	require.NotNil(t, acl)
	publicACL := &aclpb.ACL{
		UserId:            acl.GetUserId(),
		GroupId:           acl.GetGroupId(),
		OwnerPermissions:  acl.GetOwnerPermissions(),
		GroupPermissions:  acl.GetGroupPermissions(),
		OthersPermissions: &aclpb.ACL_Permissions{Read: true},
	}
	require.NoError(t, admin.RPC("UpdateInvocation", &inpb.UpdateInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: f.OrgA},
		InvocationId:   ids["admin"],
		Acl:            publicACL,
	}, &inpb.UpdateInvocationResponse{}))
	var shared tables.Invocation
	require.NoError(t, f.App.DB().Where("invocation_id = ?", ids["admin"]).Take(&shared).Error)
	require.EqualValues(t, perms.OTHERS_READ, shared.Perms&perms.OTHERS_READ)
	require.EqualValues(t, before.GetInvocation()[0].GetAcl().GetOwnerPermissions().GetRead(), shared.Perms&perms.OWNER_READ != 0)
	require.EqualValues(t, before.GetInvocation()[0].GetAcl().GetOwnerPermissions().GetWrite(), shared.Perms&perms.OWNER_WRITE != 0)
	after := &inpb.GetInvocationResponse{}
	require.NoError(t, admin.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: f.OrgA},
		Lookup:         &inpb.InvocationLookup{InvocationId: ids["admin"]},
	}, after))
	require.Len(t, after.GetInvocation(), 1)
	require.Equal(t, publicACL, after.GetInvocation()[0].GetAcl())
	publicInvocation := &inpb.GetInvocationResponse{}
	require.NoError(t, anon.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: anon.RequestContext,
		Lookup:         &inpb.InvocationLookup{InvocationId: ids["admin"]},
	}, publicInvocation))
	require.Len(t, publicInvocation.GetInvocation(), 1)
	require.Equal(t, ids["admin"], publicInvocation.GetInvocation()[0].GetInvocationId())
	publicLogs := &elpb.GetEventLogChunkResponse{}
	require.NoError(t, anon.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: anon.RequestContext,
		InvocationId:   ids["admin"],
	}, publicLogs))
	require.NotEmpty(t, publicLogs.GetBuffer(), "public log read must use the uploaded BEP blob")
	for _, name := range []string{"outsider", "anonymous"} {
		t.Run("public_"+name, func(t *testing.T) {
			var wt *webtester.WebTester
			if name == "anonymous" {
				wt = webtester.New(t)
			} else {
				wt = login(t, f, name)
			}
			wt.Get(f.App.HTTPURL() + "/invocation/" + ids["admin"])
			require.Contains(t, wt.FindWithTimeout(`[debug-id="invocation-details"]`, 10*time.Second).Text(), "permission_sentinel")
		})
	}
}
