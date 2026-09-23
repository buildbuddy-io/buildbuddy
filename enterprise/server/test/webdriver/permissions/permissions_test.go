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

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

// Each identity gets a new browser session, not just a cleared auth cookie:
// selected organization and other auth state also live in local/session storage.
func login(t *testing.T, f *permissionstest.Fixture, name string) *webtester.WebTester {
	t.Helper()
	wt := webtester.New(t)
	wt.Get(f.LoginURL())
	wt.FindWithTimeout(`select[name="user"]`, 10*time.Second).SendKeys(f.Users[name].Email)
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
				selector = `.org-members`
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
	for _, item := range wt.FindAll(`.org-list [role="menuitem"]`) {
		if item.Text() == name {
			item.Click()
			require.Eventually(t, func() bool {
				return wt.Find(`.org-picker-profile-org`).Text() == name
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
}

func TestInvocationVisibility(t *testing.T) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)
	f := permissionstest.New(t)
	ws := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "permission_sentinel", outs = ["sentinel.txt"], cmd = "echo permission-sentinel > $@")`,
	})
	// Upload real BEP data and logs, rather than seeding an invocation row whose
	// missing blob could make an authorization failure look like a missing build.
	ids := map[string]string{}
	for _, name := range []string{"admin", "outsider"} {
		t.Run("upload_"+name, func(t *testing.T) {
			wt := login(t, f, name)
			key := webtester.GetOrCreatePersonalAPIKey(wt, f.App.HTTPURL())
			args := append([]string{"//:permission_sentinel", "--remote_header=x-buildbuddy-api-key=" + key}, f.App.BESBazelFlags()...)
			result := testbazel.Invoke(context.Background(), t, ws, "build", args...)
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
	// Sharing changes the same existing resource, so the successful public case
	// is a positive control for the negative direct-link assertions above.
	result := f.App.DB().Model(&tables.Invocation{}).Where("invocation_id = ?", ids["admin"]).Update("perms", perms.GROUP_READ|perms.GROUP_WRITE|perms.OTHERS_READ)
	require.NoError(t, result.Error)
	require.EqualValues(t, 1, result.RowsAffected)
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
