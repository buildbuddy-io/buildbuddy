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
	wt.FindBody().SendKeys("?")
	wt.AssertNotFound(".keyboard-shortcut-title")
}

func TestHelpMenu(t *testing.T) {
	wt, _ := newWebTester(t)
	enableShortcuts(t, wt)

	wt.FindBody().SendKeys("?")
	shortcutHelp := wt.Find(".keyboard-shortcut-title")
	require.Contains(t, "BuildBuddy Keyboard Shortcuts", shortcutHelp.Text())

	wt.FindBody().SendKeys(selenium.EscapeKey)
	wt.AssertNotFound(".keyboard-shortcut-title")
}

func TestNavShortcuts(t *testing.T) {
	wt, target := newWebTester(t)
	enableShortcuts(t, wt)

	wt.FindBody().SendKeys("ga")
	require.Equal(t, target.HTTPURL()+"/", wt.CurrentURL())
	wt.FindBody().SendKeys("gr")
	require.Equal(t, target.HTTPURL()+"/trends/", wt.CurrentURL())
	wt.FindBody().SendKeys("gt")
	require.Equal(t, target.HTTPURL()+"/tests/", wt.CurrentURL())
	wt.FindBody().SendKeys("gx")
	require.Equal(t, target.HTTPURL()+"/executors/", wt.CurrentURL())
	wt.FindBody().SendKeys("gq")
	require.Equal(t, target.HTTPURL()+"/docs/setup/", wt.CurrentURL())
	wt.FindBody().SendKeys("gg")
	require.Equal(t, target.HTTPURL()+"/settings/", wt.CurrentURL())
}

func TestInvocationNavShortcuts(t *testing.T) {
	wt, target := newWebTester(t)
	invocationIDs := []string{addBuild(t, wt, target), addBuild(t, wt, target), addBuild(t, wt, target)}

	enableShortcuts(t, wt)

	wt.FindBody().SendKeys("ga")
	wt.FindBody().SendKeys("j")
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[2], wt.CurrentURL())

	wt.FindBody().SendKeys("u")
	for i := 1; i < 5; i++ {
		wt.FindBody().SendKeys("j")
	}
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[0], wt.CurrentURL())

	wt.FindBody().SendKeys("u")
	wt.FindBody().SendKeys("k")
	wt.Find(".selected-keyboard-shortcuts").SendKeys(selenium.EnterKey)
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[1], wt.CurrentURL())

	wt.FindBody().SendKeys("u")
	for i := 1; i < 5; i++ {
		wt.FindBody().SendKeys("k")
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
	workspacePath := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "a", outs = ["a.sh"], cmd_bash = "touch $@")`,
	})
	// Get the build flags needed for BuildBuddy, including API key, bes results url, bes backend, and remote cache
	buildArgs := append([]string{"//:a"}, webtester.GetBazelBuildFlags(wt, target.HTTPURL())...)
	result := testbazel.Invoke(context.Background(), t, workspacePath, "build", buildArgs...)
	require.NotEmpty(t, result.InvocationID)
	return result.InvocationID
}
