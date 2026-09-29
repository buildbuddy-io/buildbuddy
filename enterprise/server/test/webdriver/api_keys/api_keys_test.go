package api_keys_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

const (
	orgAPIKeysPath      = "/settings/org/api-keys"
	personalAPIKeysPath = "/settings/personal/api-keys"

	visibleToDevelopersCheckbox = `[debug-id="visible-to-developers-checkbox"]`

	adminsOnly     = int32(akpb.Visibility_VISIBLE_TO_GROUP_ADMINS)
	adminsAndDevs  = int32(akpb.Visibility_VISIBLE_TO_GROUP_ADMINS | akpb.Visibility_VISIBLE_TO_DEVELOPERS)
	waitForTimeout = 10 * time.Second
)

// apiKeyRow holds the visibility-related columns of an APIKeys row.
type apiKeyRow struct {
	VisibleToDevelopers bool
	Visibility          int32
}

// setup starts a local app and logs into it. These tests read and write the DB
// directly, so they only run against a local app.
func setup(t *testing.T) (*webtester.WebTester, *app.App) {
	buildbuddy_enterprise.MarkTestLocalOnly(t)
	a := buildbuddy_enterprise.SetupWebTarget(t).(*app.App)
	wt := webtester.New(t)
	webtester.Login(wt, a)
	return wt, a
}

func getAPIKeyRow(t *testing.T, a *app.App, label string) apiKeyRow {
	row := apiKeyRow{}
	res := a.DB().Raw(`
		SELECT visible_to_developers, visibility
		FROM "APIKeys"
		WHERE label = ?
		`,
		label,
	).Scan(&row)
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected, "expected exactly one API key labeled %q", label)
	return row
}

// requireAPIKeyRow waits for the DB row for the key with the given label to
// match the expected value, since the UI writes it asynchronously.
func requireAPIKeyRow(t *testing.T, a *app.App, label string, expected apiKeyRow) {
	var row apiKeyRow
	require.Eventually(t, func() bool {
		row = getAPIKeyRow(t, a, label)
		return row == expected
	}, waitForTimeout, 50*time.Millisecond, "API key %q: got %+v, want %+v", label, row, expected)
}

func waitForDialogClosed(t *testing.T, wt *webtester.WebTester) {
	require.Eventually(t, func() bool {
		return len(wt.FindAll(".api-keys-form")) == 0
	}, waitForTimeout, 50*time.Millisecond, "API key dialog did not close")
}

// findAPIKey returns the list item for the API key with the given label.
func findAPIKey(t *testing.T, wt *webtester.WebTester, label string) *webtester.Element {
	var item *webtester.Element
	require.Eventually(t, func() bool {
		for _, el := range wt.FindAll(".api-key-list-item") {
			if el.Find(".api-key-label").Text() == label {
				item = el
				return true
			}
		}
		return false
	}, waitForTimeout, 50*time.Millisecond, "API key %q not found", label)
	return item
}

// createAPIKey creates an API key from the API keys page that is currently
// open. If visibleToDevelopers is nil, the visibility checkbox is left alone.
func createAPIKey(t *testing.T, wt *webtester.WebTester, label string, visibleToDevelopers *bool) {
	wt.FindByDebugID("create-new-api-key").Click()
	wt.Find(`.api-keys-form [name="label"]`).SendKeys(label)
	if visibleToDevelopers != nil {
		wt.Find(visibleToDevelopersCheckbox).SetChecked(*visibleToDevelopers)
	}
	wt.Find(`.api-keys-form button[type="submit"]`).Click()
	waitForDialogClosed(t, wt)
	findAPIKey(t, wt, label)
}

func openEditDialog(t *testing.T, wt *webtester.WebTester, label string) {
	findAPIKey(t, wt, label).Find(".api-key-edit-button").Click()
	wt.Find(".api-keys-form")
}

func submitEditDialog(t *testing.T, wt *webtester.WebTester) {
	wt.Find(`.api-keys-form button[type="submit"]`).Click()
	waitForDialogClosed(t, wt)
}

func TestCreateAPIKey_DefaultVisibility(t *testing.T) {
	wt, a := setup(t)
	wt.Get(a.HTTPURL() + orgAPIKeysPath)

	createAPIKey(t, wt, "default-key", nil)

	requireAPIKeyRow(t, a, "default-key", apiKeyRow{VisibleToDevelopers: false, Visibility: adminsOnly})
}

func TestCreateAPIKey_VisibleToDevelopers(t *testing.T) {
	wt, a := setup(t)
	wt.Get(a.HTTPURL() + orgAPIKeysPath)

	visible := true
	createAPIKey(t, wt, "dev-key", &visible)

	requireAPIKeyRow(t, a, "dev-key", apiKeyRow{VisibleToDevelopers: true, Visibility: adminsAndDevs})
	openEditDialog(t, wt, "dev-key")
	require.True(t, wt.Find(visibleToDevelopersCheckbox).IsSelected())
}

func TestUpdateAPIKey_ToggleVisibleToDevelopers(t *testing.T) {
	wt, a := setup(t)
	wt.Get(a.HTTPURL() + orgAPIKeysPath)
	createAPIKey(t, wt, "toggle-key", nil)

	// Toggle on, then off, reloading the page in between to make sure the
	// dialog reflects what was saved rather than leftover form state.
	for _, visible := range []bool{true, false} {
		openEditDialog(t, wt, "toggle-key")
		// SetChecked fails the test if the checkbox doesn't reflect the click,
		// which catches form updates clobbering each other.
		require.True(t, wt.Find(visibleToDevelopersCheckbox).SetChecked(visible))
		submitEditDialog(t, wt)

		want := apiKeyRow{VisibleToDevelopers: visible, Visibility: adminsOnly}
		if visible {
			want.Visibility = adminsAndDevs
		}
		requireAPIKeyRow(t, a, "toggle-key", want)

		wt.Refresh()
		openEditDialog(t, wt, "toggle-key")
		require.Equal(t, visible, wt.Find(visibleToDevelopersCheckbox).IsSelected())
		wt.FindByDebugID("api-key-form-cancel").Click()
		waitForDialogClosed(t, wt)
	}
}

func TestUpdateAPIKey_EditLabelKeepsVisibility(t *testing.T) {
	wt, a := setup(t)
	wt.Get(a.HTTPURL() + orgAPIKeysPath)
	visible := true
	createAPIKey(t, wt, "old-label", &visible)

	openEditDialog(t, wt, "old-label")
	label := wt.Find(`.api-keys-form [name="label"]`)
	label.Clear()
	label.SendKeys("new-label")
	submitEditDialog(t, wt)

	findAPIKey(t, wt, "new-label")
	requireAPIKeyRow(t, a, "new-label", apiKeyRow{VisibleToDevelopers: true, Visibility: adminsAndDevs})
}

// Keys created before the visibility column was populated have visibility 0,
// and the server returns them with an empty visibility. Editing one must not
// drop group admins from its visibility. The server currently derives
// visibility from visible_to_developers on write, so this mainly guards the
// point at which it starts honoring the visibility in the request.
func testUpdateUnmigratedAPIKey(t *testing.T, visibleToDevelopers bool) {
	wt, a := setup(t)
	wt.Get(a.HTTPURL() + orgAPIKeysPath)
	createAPIKey(t, wt, "legacy-key", nil)
	res := a.DB().Exec(`UPDATE "APIKeys" SET visibility = 0 WHERE label = ?`, "legacy-key")
	require.NoError(t, res.Error)
	require.Equal(t, int64(1), res.RowsAffected)

	wt.Refresh()
	openEditDialog(t, wt, "legacy-key")
	wt.Find(visibleToDevelopersCheckbox).SetChecked(visibleToDevelopers)
	submitEditDialog(t, wt)

	want := apiKeyRow{VisibleToDevelopers: visibleToDevelopers, Visibility: adminsOnly}
	if visibleToDevelopers {
		want.Visibility = adminsAndDevs
	}
	requireAPIKeyRow(t, a, "legacy-key", want)
}

// webtester doesn't support subtests, so each case is its own test.
func TestUpdateAPIKey_UnmigratedKey_MakeVisibleToDevelopers(t *testing.T) {
	testUpdateUnmigratedAPIKey(t, true)
}

func TestUpdateAPIKey_UnmigratedKey_KeepAdminsOnly(t *testing.T) {
	testUpdateUnmigratedAPIKey(t, false)
}

func TestCreateAPIKey_FormResetsAfterCancel(t *testing.T) {
	wt, a := setup(t)
	wt.Get(a.HTTPURL() + orgAPIKeysPath)

	wt.FindByDebugID("create-new-api-key").Click()
	wt.Find(visibleToDevelopersCheckbox).SetChecked(true)
	wt.FindByDebugID("api-key-form-cancel").Click()
	waitForDialogClosed(t, wt)

	createAPIKey(t, wt, "after-cancel", nil)
	requireAPIKeyRow(t, a, "after-cancel", apiKeyRow{VisibleToDevelopers: false, Visibility: adminsOnly})
}

func TestPersonalAPIKey_NoVisibilityCheckbox(t *testing.T) {
	wt, a := setup(t)
	webtester.UpdateSelectedOrg(wt, a.HTTPURL(), "Test", "test", webtester.EnableUserOwnedAPIKeys)
	wt.Get(a.HTTPURL() + personalAPIKeysPath)

	wt.FindByDebugID("create-new-api-key").Click()
	wt.Find(`.api-keys-form [name="label"]`).SendKeys("personal-key")
	wt.AssertNotFound(visibleToDevelopersCheckbox)
	wt.FindByDebugID("cas-only-radio-button").Click()
	wt.Find(`.api-keys-form button[type="submit"]`).Click()
	waitForDialogClosed(t, wt)

	requireAPIKeyRow(t, a, "personal-key", apiKeyRow{VisibleToDevelopers: false, Visibility: adminsOnly})

	// Editing a personal key must not make it visible to developers, which
	// the server rejects for user-owned keys.
	openEditDialog(t, wt, "personal-key")
	wt.AssertNotFound(visibleToDevelopersCheckbox)
	submitEditDialog(t, wt)
	requireAPIKeyRow(t, a, "personal-key", apiKeyRow{VisibleToDevelopers: false, Visibility: adminsOnly})
}
