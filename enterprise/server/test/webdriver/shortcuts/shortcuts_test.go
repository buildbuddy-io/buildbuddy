package invocation_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
)

func TestShortcutsDisabled(t *testing.T) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)
	webtester.Login(wt, target)
	wt.Get(target.HTTPURL())

	wt.FindBody().SendKeys("?")
	wt.AssertNotFound(".keyboard-shortcut-title")
}

func TestHelpMenu(t *testing.T) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)
	webtester.Login(wt, target)
	wt.Get(target.HTTPURL())

	enableShortcuts(t, wt)

	wt.FindBody().SendKeys("?")
	shortcutHelp := wt.Find(".keyboard-shortcut-title")
	require.Contains(t, "BuildBuddy Keyboard Shortcuts", shortcutHelp.Text())

	wt.FindBody().SendKeys(selenium.EscapeKey)
	wt.AssertNotFound(".keyboard-shortcut-title")
}

func TestNavShortcuts(t *testing.T) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)
	webtester.Login(wt, target)
	wt.Get(target.HTTPURL())

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
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)
	webtester.Login(wt, target)
	wt.Get(target.HTTPURL())

	enableShortcuts(t, wt)

	invocationIDs := []string{addBuild(t, wt, target), addBuild(t, wt, target), addBuild(t, wt, target)}

	wt.FindBody().SendKeys("ga")
	wt.FindBody().SendKeys("j")
	wt.SendKeyCombo([]string{selenium.EnterKey})
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[2], wt.CurrentURL())

	wt.FindBody().SendKeys("u")
	for i := 1; i < 5; i++ {
		wt.FindBody().SendKeys("j")
	}
	wt.SendKeyCombo([]string{selenium.EnterKey})
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[0], wt.CurrentURL())

	wt.FindBody().SendKeys("u")
	wt.FindBody().SendKeys("k")
	wt.SendKeyCombo([]string{selenium.EnterKey})
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[1], wt.CurrentURL())

	wt.FindBody().SendKeys("u")
	for i := 1; i < 5; i++ {
		wt.FindBody().SendKeys("k")
	}
	wt.SendKeyCombo([]string{selenium.EnterKey})
	require.Equal(t, target.HTTPURL()+"/invocation/"+invocationIDs[2], wt.CurrentURL())
}

func TestCopyInvocationLink(t *testing.T) {
	wt := webtester.New(t)
	target := buildbuddy_enterprise.SetupWebTarget(t)
	webtester.Login(wt, target)
	wt.Get(target.HTTPURL())

	enableShortcuts(t, wt)

	invocationID := addBuild(t, wt, target)

	wt.Get(target.HTTPURL() + "/invocation/" + invocationID)

	wt.SendKeyCombo([]string{selenium.ShiftKey, "c"})

	// TODO: it'd be nice to verify the contents of the clipboard, but selenium
	// doesn't have great support for that. The current recommendation is to
	// paste into a textbox and then read the contents. For now, just verify
	// that the toast shows.
	// It takes a sec for the banner to render, give it 100ms before failing.
	wt.WaitWithTimeout(
		func() bool {
			return wt.Has(".banner.banner-success") && wt.Find(".banner.banner-success").Find(".banner-content").Text() == "Copied invocation link to clipboard"
		},
		100*time.Millisecond)
}

func enableShortcuts(t *testing.T, wt *webtester.WebTester) {
	webtester.ClickSidebarItem(wt, "Settings")
	preferencesButton := wt.FindByDebugID("personal-preferences")
	preferencesButton.Click()
	shortcutsButton := wt.FindByDebugID("keyboard-shortcuts-button")
	require.Equal(t, "Enable keyboard shortcuts", shortcutsButton.Text())
	shortcutsButton.Click()
	shortcutsButton = wt.FindByDebugID("keyboard-shortcuts-button")
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
