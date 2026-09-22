//go:build darwin && !ios

package install

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"github.com/stretchr/testify/require"
)

func TestHelperPlist(t *testing.T) {
	plist, err := helperPlist(helperPlistParams{Label: launchdLabel, Exe: "/opt/bb/bb helper", UID: 501, CIDR: "198.18.0.0/16", Log: "/var/log/x.log"})
	require.NoError(t, err)
	require.Contains(t, plist, "<string>/opt/bb/bb helper</string>\n\t\t<string>--uid</string>\n\t\t<string>501</string>\n\t\t<string>--cidr</string>\n\t\t<string>198.18.0.0/16</string>")
	require.Contains(t, plist, "<key>SuccessfulExit</key>\n\t\t<false/>")
	require.Equal(t, 501, installedFor([]byte(plist)))
	path := filepath.Join(t.TempDir(), "x.plist")
	require.NoError(t, os.WriteFile(path, []byte(plist), 0o644))
	out, err := exec.Command("plutil", "-lint", path).CombinedOutput()
	require.NoError(t, err, string(out))
}

func TestNeeded(t *testing.T) {
	dir := t.TempDir()
	plistPath, resolvers, helper, device := launchdPlistPath, resolverDir, helperPath, helperDevice
	t.Cleanup(func() { launchdPlistPath, resolverDir, helperPath, helperDevice = plistPath, resolvers, helper, device })
	launchdPlistPath, resolverDir, helperPath = filepath.Join(dir, "helper.plist"), filepath.Join(dir, "resolver"), filepath.Join(dir, "helper")
	name, helperErr := "", errors.New("no helper")
	helperDevice = func() (string, error) { return name, helperErr }
	cfg := tunnelconfig.Default()
	// The loopback carries the first address of this range.
	cfg.FakeCIDR = "127.0.0.0/8"
	check := func(want string) {
		t.Helper()
		reason, err := needed(cfg)
		require.NoError(t, err)
		require.Contains(t, reason, want)
	}

	check("the device helper is not installed")

	uid, err := invokingUID()
	require.NoError(t, err)
	p, err := helperParams(cfg, uid)
	require.NoError(t, err)
	plist, err := helperPlist(p)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(launchdPlistPath, []byte(plist), 0o644))
	check("the device helper is not installed")

	// An older helper is replaced; a newer one is kept.
	installHelperVersion := func(v int) {
		script := fmt.Sprintf("#!/bin/sh\necho %d\n", v)
		require.NoError(t, os.WriteFile(helperPath, []byte(script), 0o755))
	}
	installHelperVersion(tunhelperutil.Version - 1)
	check("out of date")
	installHelperVersion(tunhelperutil.Version + 1)
	check("not answering (no helper)")

	name, helperErr = "nosuch0", nil
	check("nosuch0, which does not exist")

	// Someone else's helper is not taken over.
	other := p
	other.UID = uid + 1
	otherPlist, err := helperPlist(other)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(launchdPlistPath, []byte(otherPlist), 0o644))
	_, err = needed(cfg)
	require.ErrorContains(t, err, "single user")
	require.NoError(t, os.WriteFile(launchdPlistPath, []byte(plist), 0o644))

	name = "lo0"
	check("DNS for")

	require.NoError(t, os.MkdirAll(resolverDir, 0o755))
	for _, domain := range daemon.ResolverDomains(cfg) {
		require.NoError(t, os.WriteFile(filepath.Join(resolverDir, domain), []byte(resolverFileContent("127.0.0.1", "5533")), 0o644))
	}
	reason, err := needed(cfg)
	require.NoError(t, err)
	require.Empty(t, reason)
}
