//go:build darwin && !ios

// Package install performs the privileged setup the tunnel needs on Mac:
//   - installs the tun device helper, a small embedded binary that runs as a
//     daemon (started via launchd) and hands out the configured tun device file
//     descriptor to bbaccess
//   - writes the resolver file that routes the internal zone to the bbaccess
//     DNS resolver.
package install

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

const (
	launchdLabel  = "io.buildbuddy.bbaccess-tunnel"
	helperLogPath = "/var/log/bbaccess-tunnel-device.log"
	// Long enough for launchd to relaunch a helper that failed once.
	helperStartTimeout = 20 * time.Second
	marker             = `Written by "bbaccess tunnel install"`
)

// Can be modified by tests.
var (
	resolverDir      = "/etc/resolver"
	launchdPlistPath = "/Library/LaunchDaemons/" + launchdLabel + ".plist"
	helperPath       = "/Library/PrivilegedHelperTools/" + launchdLabel
	// helperDevice returns the interface the device helper serves.
	helperDevice = func() (string, error) {
		fd, name, conn, err := tunhelperutil.Receive()
		if err == nil {
			syscall.Close(fd)
			conn.Close()
		}
		return name, err
	}
)

// pingTunHelper checks whether the tun device helper is responding.
func pingTunHelper() error {
	_, err := helperDevice()
	return err
}

// socketOwnedBy reports whether uid can connect to the helper.
func socketOwnedBy(uid int) bool {
	st, err := os.Stat(tunhelperutil.SocketPath)
	if err != nil {
		return false
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	return ok && int(sys.Uid) == uid && st.Mode().Perm() == 0o600
}

// Install sets up the device helper as a launchd daemon and writes an
// /etc/resolver file for the zones' parent and for the reverse zone of the
// fake range.
func Install(cfg *tunnelconfig.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel install")
	}
	// The device belongs to the person who ran sudo, not to root.
	owner := os.Getenv("SUDO_USER")
	if owner == "" {
		return fmt.Errorf("cannot tell which user should own the TUN device: run this through sudo")
	}
	u, err := user.Lookup(owner)
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return err
	}
	if err := installTunHelper(cfg, owner, uid); err != nil {
		return err
	}

	host, port, err := splitHostPort(cfg.DNSListen)
	if err != nil {
		return fmt.Errorf("parsing dns_listen %q: %w", cfg.DNSListen, err)
	}
	if err := os.MkdirAll(resolverDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", resolverDir, err)
	}
	for _, domain := range daemon.ResolverDomains(cfg) {
		path := filepath.Join(resolverDir, domain)
		content := resolverFileContent(host, port)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", path, err)
		}
		fmt.Printf("Configured %s → %s\n", domain, cfg.DNSListen)
	}

	fmt.Printf("\nInstalled. Start the daemon with:\n\n    bbaccess tunnel start\n\n")
	return nil
}

// installHelper installs the device helper and its launchd daemon, starts it
// and waits for the device.
func installTunHelper(cfg *tunnelconfig.Config, owner string, uid int) error {
	p, err := tunHelperParams(cfg, uid)
	if err != nil {
		return err
	}
	plist, err := helperPlist(p)
	if err != nil {
		return err
	}
	if got, err := os.ReadFile(launchdPlistPath); err == nil {
		if other := installedFor(got); other >= 0 && other != uid {
			return fmt.Errorf("the device helper is installed for %s, not for %s; the tunnel supports a single user per machine", userName(other), owner)
		}
		// If everything is up to date, we can return early.
		if string(got) == plist && tunHelperOutdated() == "" && pingTunHelper() == nil && socketOwnedBy(uid) {
			fmt.Printf("The device helper is already running\n")
			return nil
		}
	}
	out, err := exec.Command("launchctl", "bootout", "system/"+launchdLabel).CombinedOutput()
	reloaded := err == nil
	if !reloaded && pingTunHelper() == nil {
		return fmt.Errorf("launchctl bootout: %w: %s", err, bytes.TrimSpace(out))
	}
	// Only now, so that a failed reload does not pass for installed.
	if tunHelperOutdated() != "" {
		if err := writeTunHelper(); err != nil {
			return err
		}
	}
	if err := os.WriteFile(launchdPlistPath, []byte(plist), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", launchdPlistPath, err)
	}
	// bootout may still be tearing the previous instance down.
	if err := retry(helperStartTimeout, func() error {
		out, err := exec.Command("launchctl", "bootstrap", "system", launchdPlistPath).CombinedOutput()
		if err != nil {
			return fmt.Errorf("launchctl bootstrap: %w: %s", err, bytes.TrimSpace(out))
		}
		return nil
	}); err != nil {
		return err
	}
	// The daemon attaches right after this, so wait until the helper answers.
	if err := retry(helperStartTimeout, func() error { return pingTunHelper() }); err != nil {
		return fmt.Errorf("the device helper did not start within %s (%s); the end of %s:\n%s", helperStartTimeout, err, helperLogPath, logTail(helperLogPath))
	}
	fmt.Printf("Created a utun device for %s, routing %s; %s recreates it at boot\n", owner, cfg.FakeCIDR, launchdLabel)
	if reloaded {
		fmt.Printf("The device changed: a running daemon exits, and the next start attaches to the new one\n")
	}
	return nil
}

// retry calls f until it succeeds or timeout passes.
func retry(timeout time.Duration, f func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := f()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// logTail returns the end of the helper's log, for an error message.
func logTail(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > 10 {
		lines = lines[len(lines)-10:]
	}
	return strings.Join(lines, "\n")
}

func resolverFileContent(host, port string) string {
	return fmt.Sprintf("# %s. Remove with \"bbaccess tunnel uninstall\".\nnameserver %s\nport %s\n", marker, host, port)
}

// needed reports why the tunnel is not installed for the invoking user, or ""
// if it is.
func needed(cfg *tunnelconfig.Config) (string, error) {
	uid, err := invokingUID()
	if err != nil {
		return "", err
	}
	p, err := tunHelperParams(cfg, uid)
	if err != nil {
		return "", err
	}
	want, err := helperPlist(p)
	if err != nil {
		return "", err
	}
	got, err := os.ReadFile(launchdPlistPath)
	// The tunnel is expected to be used by a single user.
	if other := installedFor(got); other >= 0 && other != uid {
		return "", fmt.Errorf("the device helper is installed for %s, not for you; the tunnel supports a single user per machine", userName(other))
	}
	if err != nil {
		return "the device helper is not installed", nil
	}
	if string(got) != want {
		return "the device helper's configuration changed", nil
	}
	if reason := tunHelperOutdated(); reason != "" {
		return reason, nil
	}
	if err := pingTunHelper(); err != nil {
		return fmt.Sprintf("the device helper is not answering (%s)", err), nil
	}

	host, port, err := splitHostPort(cfg.DNSListen)
	if err != nil {
		return "", fmt.Errorf("parsing dns_listen %q: %w", cfg.DNSListen, err)
	}
	for _, domain := range daemon.ResolverDomains(cfg) {
		got, err := os.ReadFile(filepath.Join(resolverDir, domain))
		if err != nil || string(got) != resolverFileContent(host, port) {
			return fmt.Sprintf("DNS for %s is not routed to the daemon", domain), nil
		}
	}
	return "", nil
}

// invokingUID is who the tunnel is for.
func invokingUID() (int, error) {
	if name := os.Getenv("SUDO_USER"); os.Geteuid() == 0 && name != "" {
		u, err := user.Lookup(name)
		if err != nil {
			return 0, err
		}
		return strconv.Atoi(u.Uid)
	}
	return os.Getuid(), nil
}

// Uninstall removes the device helper and the resolver files Install wrote.
func Uninstall(cfg *tunnelconfig.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel uninstall")
	}
	b, err := os.ReadFile(launchdPlistPath)
	switch {
	case err == nil && !strings.Contains(string(b), marker):
		fmt.Printf("Leaving %s alone: it was not written by bbaccess tunnel install\n", launchdPlistPath)
	case err == nil:
		out, err := exec.Command("launchctl", "bootout", "system/"+launchdLabel).CombinedOutput()
		if err != nil && pingTunHelper() == nil {
			return fmt.Errorf("launchctl bootout: %w: %s", err, bytes.TrimSpace(out))
		}
		if err := os.Remove(launchdPlistPath); err != nil {
			return fmt.Errorf("removing %s: %w", launchdPlistPath, err)
		}
		os.Remove(tunhelperutil.SocketPath)
		fmt.Printf("Unloaded the device helper and removed %s; a running daemon exits with it\n", launchdPlistPath)
		fallthrough
	default:
		if err := os.Remove(helperPath); err == nil {
			fmt.Printf("Removed %s\n", helperPath)
		}
	}
	for _, domain := range daemon.ResolverDomains(cfg) {
		path := filepath.Join(resolverDir, domain)
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if !strings.Contains(string(b), marker) {
			fmt.Printf("Leaving %s alone: it was not written by bbaccess tunnel install\n", path)
			continue
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("removing %s: %w", path, err)
		}
		fmt.Printf("Removed %s\n", path)
	}
	return nil
}

// helperPlistParams fills the launchd plist that runs the device helper.
type helperPlistParams struct {
	Label string
	Exe   string // the installed helper
	UID   int    // owner of the device
	CIDR  string // the range routed to it
	Log   string
}

func tunHelperParams(cfg *tunnelconfig.Config, uid int) (helperPlistParams, error) {
	if _, err := tunhelperutil.LocalAddr(cfg.FakeCIDR); err != nil {
		return helperPlistParams{}, fmt.Errorf("fake_cidr: %w", err)
	}
	return helperPlistParams{Label: launchdLabel, Exe: helperPath, UID: uid, CIDR: cfg.FakeCIDR, Log: helperLogPath}, nil
}

// plistUID finds the owner in a plist we wrote.
var plistUID = regexp.MustCompile(`<string>--uid</string>\s*<string>(\d+)</string>`)

// installedFor returns the owner's uid a plist we wrote names, or -1.
func installedFor(plist []byte) int {
	m := plistUID.FindSubmatch(plist)
	if m == nil {
		return -1
	}
	uid, err := strconv.Atoi(string(m[1]))
	if err != nil {
		return -1
	}
	return uid
}

// KeepAlive only after a failed exit.
var helperPlistTemplate = template.Must(template.New("plist").Funcs(template.FuncMap{"xml": xmlEscape}).Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- ` + marker + `. Remove with "bbaccess tunnel uninstall". -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{{.Label}}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{{xml .Exe}}</string>
		<string>--uid</string>
		<string>{{.UID}}</string>
		<string>--cidr</string>
		<string>{{xml .CIDR}}</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>StandardOutPath</key>
	<string>{{xml .Log}}</string>
	<key>StandardErrorPath</key>
	<string>{{xml .Log}}</string>
</dict>
</plist>
`))

func helperPlist(p helperPlistParams) (string, error) {
	var b strings.Builder
	if err := helperPlistTemplate.Execute(&b, p); err != nil {
		return "", err
	}
	return b.String(), nil
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}
