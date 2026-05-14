package invocation_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
)

func TestShortcutsDisabled(t *testing.T) {
	wt, _ := newWebTester(t)
	sendShortcutKeys(wt, "?")
	wt.AssertNotFound(".keyboard-shortcut-title")
}

func TestHelpMenu(t *testing.T) {
	wt, _ := newWebTester(t)
	enableShortcuts(t, wt)

	sendShortcutKeys(wt, "?")
	shortcutHelp := wt.Find(".keyboard-shortcut-title")
	require.Contains(t, "BuildBuddy Keyboard Shortcuts", shortcutHelp.Text())

	sendShortcutKeys(wt, selenium.EscapeKey)
	wt.AssertNotFound(".keyboard-shortcut-title")
}

func TestNavShortcuts(t *testing.T) {
	wt, target := newWebTester(t)
	enableShortcuts(t, wt)

	sendShortcutKeys(wt, "ga")
	require.Equal(t, target.HTTPURL()+"/", wt.CurrentURL())
	sendShortcutKeys(wt, "gr")
	require.Equal(t, target.HTTPURL()+"/trends/", wt.CurrentURL())
	sendShortcutKeys(wt, "gt")
	require.Equal(t, target.HTTPURL()+"/tests/", wt.CurrentURL())
	sendShortcutKeys(wt, "gx")
	require.Equal(t, target.HTTPURL()+"/executors/", wt.CurrentURL())
	sendShortcutKeys(wt, "gq")
	require.Equal(t, target.HTTPURL()+"/docs/setup/", wt.CurrentURL())
	sendShortcutKeys(wt, "gg")
	require.Equal(t, target.HTTPURL()+"/settings/", wt.CurrentURL())
}

func TestInvocationNavShortcuts(t *testing.T) {
	wt, target := newWebTester(t)
	invocationIDs := []string{addBuild(t, wt, target), addBuild(t, wt, target), addBuild(t, wt, target)}

	enableShortcuts(t, wt)

	sendShortcutKeys(wt, "ga")
	require.Equal(t, 3, len(wt.FindAll(".invocation-card")))

	sendShortcutKeys(wt, "j")
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[2], wt.CurrentURL())

	sendShortcutKeys(wt, "u")
	for i := 1; i < 5; i++ {
		sendShortcutKeys(wt, "j")
	}
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[0], wt.CurrentURL())

	sendShortcutKeys(wt, "u")
	sendShortcutKeys(wt, "k")
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[1], wt.CurrentURL())

	sendShortcutKeys(wt, "u")
	for i := 1; i < 5; i++ {
		sendShortcutKeys(wt, "k")
	}
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[2], wt.CurrentURL())
}

// TODO: add a test for shift-c that copies the invocation URL on the
// invocation page. It's a little funky to test because the javascript that
// renders the banner doesn't work great remotely and testing the clipboard
// values interferes with the local clipboard if running locally.

func newWebTester(t *testing.T) (*webtester.WebTester, buildbuddy_enterprise.WebTarget) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)
	webtester.Login(wt, target)
	return wt, target
}

func enableShortcuts(t *testing.T, wt *webtester.WebTester) {
	webtester.ClickSidebarItem(wt, "Settings")
	wt.FindByDebugID("personal-preferences").Click()
	shortcutsButton := wt.FindByDebugID("keyboard-shortcuts-button")
	require.Equal(t, "Enable keyboard shortcuts", shortcutsButton.Text())
	shortcutsButton.Click()
	require.Equal(t, "Disable keyboard shortcuts", shortcutsButton.Text())
}

func addBuild(t *testing.T, wt *webtester.WebTester, target buildbuddy_enterprise.WebTarget) string {
	// Get the build flags needed for BuildBuddy, including API key, bes results url, bes backend, and remote cache
	setupPageOpts := []webtester.SetupPageOption{webtester.WithEnableCache}
	buildbuddyBuildFlags := webtester.GetBazelBuildFlags(wt, target.HTTPURL(), setupPageOpts...)

	workspacePath := testbazel.MakeTempModule(t, map[string]string{
		"BUILD": `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	buildArgs := append([]string{"//:a"}, buildbuddyBuildFlags...)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)
	return result.InvocationID
}

func sendShortcutKeys(wt *webtester.WebTester, shortcut string) {
	wt.Find("body").SendKeys(shortcut)
}
