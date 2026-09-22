//go:build linux && !android

// Package install performs the privileged setup the tunnel needs on Linux:
//   - installs the tun device helper, a small embedded binary that configures
//     the tun device as root and a systemd unit that runs it at boot to create
//     the TUN device through which relayed traffic flows.
//   - writes a systemd-resolved drop-in routing the relayed suffix to the
//     bbaccess DNS resolver.
package install

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunhelperutil"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

const (
	resolvedDropIn = "/etc/systemd/resolved.conf.d/buildbuddy-tunnel.conf"
	deviceUnitName = "bbaccess-tunnel-device.service"
	deviceUnitPath = "/etc/systemd/system/" + deviceUnitName
)

// helperPath is where the helper is installed, owned by root.
// Can be modified by tests.
var helperPath = "/usr/local/libexec/bbaccess-tunnel-helper"

func Install(cfg *tunnelconfig.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel install")
	}
	// The TUN device belongs to the person who ran sudo, not to root.
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
	// Sanity check values before we write the unit file.
	if !unitWord.MatchString(cfg.TUNName) || len(cfg.TUNName) > maxIfaceName {
		return fmt.Errorf("tun_name %q must be 1-%d characters from [A-Za-z0-9._-]", cfg.TUNName, maxIfaceName)
	}
	unit, err := deviceUnitFor(cfg, uid)
	if err != nil {
		return fmt.Errorf("rendering %s: %w", deviceUnitName, err)
	}
	if tunHelperOutdated() != "" {
		if err := writeTunHelper(); err != nil {
			return err
		}
	}
	if err := os.WriteFile(deviceUnitPath, []byte(unit), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", deviceUnitPath, err)
	}
	for _, args := range [][]string{
		{"daemon-reload"},
		{"enable", deviceUnitName},
		{"restart", deviceUnitName},
	} {
		if out, err := exec.Command("systemctl", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, out)
		}
	}
	fmt.Printf("Created %s (owner %s), routing %s; %s recreates it at boot\n", cfg.TUNName, owner, cfg.FakeCIDR, deviceUnitName)

	if err := installResolved(cfg); err != nil {
		return err
	}
	fmt.Printf("\nInstalled. Start the daemon with:\n\n    bbaccess tunnel start\n\n")
	return nil
}

// installResolved writes a systemd-resolved drop-in that routes the zones'
// parent to the daemon's DNS server.
func installResolved(cfg *tunnelconfig.Config) error {
	host, port, err := splitListen(cfg.DNSListen)
	if err != nil {
		return err
	}
	domains := daemon.ResolverDomains(cfg)
	content := resolvedDropInContent(host, port, domains)

	if err := os.MkdirAll("/etc/systemd/resolved.conf.d", 0o755); err != nil {
		return fmt.Errorf("creating /etc/systemd/resolved.conf.d: %w", err)
	}
	if err := os.WriteFile(resolvedDropIn, []byte(content), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", resolvedDropIn, err)
	}
	if out, err := exec.Command("systemctl", "restart", "systemd-resolved").CombinedOutput(); err != nil {
		return fmt.Errorf("restarting systemd-resolved: %w: %s", err, out)
	}
	fmt.Printf("Configured systemd-resolved: %s → %s\n", strings.Join(domains, ", "), cfg.DNSListen)
	return nil
}

// Uninstall reverses Install.
func Uninstall(cfg *tunnelconfig.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel uninstall")
	}
	run("systemctl", "disable", "--now", deviceUnitName)
	if err := os.Remove(deviceUnitPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s: %w", deviceUnitPath, err)
	}
	run("systemctl", "daemon-reload")
	if err := os.Remove(resolvedDropIn); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s: %w", resolvedDropIn, err)
	}
	run("systemctl", "restart", "systemd-resolved")
	if err := os.Remove(helperPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s: %w", helperPath, err)
	}
	fmt.Printf("Removed %s, %s, %s and %s\n", cfg.TUNName, deviceUnitPath, resolvedDropIn, helperPath)
	return nil
}

// resolvedDropInContent renders the systemd-resolved drop-in to send lookups
// to our DNS server.
func resolvedDropInContent(host, port string, domains []string) string {
	routing := make([]string, 0, len(domains))
	for _, d := range domains {
		routing = append(routing, "~"+d)
	}
	return fmt.Sprintf(`# Written by "bbaccess tunnel install". Remove with "bbaccess tunnel uninstall".
[Resolve]
DNS=%s:%s
Domains=%s
`, host, port, strings.Join(routing, " "))
}

// sysClassNet is where the kernel describes network devices
// Can be modified by tests.
var sysClassNet = "/sys/class/net"

// deviceOwner returns the uid that owns a TUN device.
func deviceOwner(dev string) (int, error) {
	b, err := os.ReadFile(filepath.Join(sysClassNet, dev, "owner"))
	if os.IsNotExist(err) {
		return -1, nil
	}
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

// needed reports why the tunnel is not installed for the invoking user, or ""
// if it is.
func needed(cfg *tunnelconfig.Config) (string, error) {
	uid := os.Getuid()
	if name := os.Getenv("SUDO_USER"); os.Geteuid() == 0 && name != "" {
		// Under sudo, the device must belong to the person, not to root.
		u, err := user.Lookup(name)
		if err != nil {
			return "", err
		}
		uid, _ = strconv.Atoi(u.Uid)
	}
	addr, err := tunhelperutil.LocalAddr(cfg.FakeCIDR)
	if err != nil {
		return "", fmt.Errorf("fake_cidr: %w", err)
	}

	ifc, err := net.InterfaceByName(cfg.TUNName)
	if err != nil {
		return fmt.Sprintf("the TUN device %s does not exist", cfg.TUNName), nil
	}
	owner, err := deviceOwner(cfg.TUNName)
	if err != nil {
		return "", err
	}
	if owner != uid {
		// The tunnel is expected to be used by a single user.
		// Error out on user mismatch.
		return "", fmt.Errorf("the TUN device %s is owned by %s, not by you; the tunnel supports a single user per machine", cfg.TUNName, userName(owner))
	}
	want := netip.PrefixFrom(addr, netip.MustParsePrefix(cfg.FakeCIDR).Bits()).String()
	addrs, err := ifc.Addrs()
	if err != nil {
		return "", err
	}
	hasAddr := false
	for _, a := range addrs {
		if a.String() == want {
			hasAddr = true
		}
	}
	if !hasAddr {
		return fmt.Sprintf("the TUN device %s does not carry %s", cfg.TUNName, want), nil
	}
	unit, err := deviceUnitFor(cfg, uid)
	if err != nil {
		return "", err
	}
	got, err := os.ReadFile(deviceUnitPath)
	if err != nil {
		return "the device would not survive a reboot", nil
	}
	if string(got) != unit {
		return fmt.Sprintf("%s is out of date", deviceUnitPath), nil
	}
	if reason := tunHelperOutdated(); reason != "" {
		return reason, nil
	}

	host, port, err := splitListen(cfg.DNSListen)
	if err != nil {
		return "", err
	}
	got, err = os.ReadFile(resolvedDropIn)
	if err != nil || string(got) != resolvedDropInContent(host, port, daemon.ResolverDomains(cfg)) {
		return fmt.Sprintf("DNS for %s is not routed to the daemon", tunnelconfig.Parent), nil
	}
	return "", nil
}

func run(name string, args ...string) {
	exec.Command(name, args...).Run()
}

// maxIfaceName is IFNAMSIZ minus the terminator.
const maxIfaceName = 15

// unitWord is used to sanity check the tun device name and username in the unit
var unitWord = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// deviceUnitParams fills the systemd unit that creates the TUN device.
type deviceUnitParams struct {
	Helper string // the installed helper
	UID    int    // owner of the device
	CIDR   string // the range routed to it
	Dev    string // interface name
}

// deviceUnitTemplate is the template for the systemd unit that sets up the
// tun devices.
var deviceUnitTemplate = template.Must(template.New("unit").Parse(`# Written by "bbaccess tunnel install". Remove with "bbaccess tunnel uninstall".
[Unit]
Description=bbaccess tunnel: TUN device {{.Dev}} for the placeholder range
Before=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={{.Helper}} --uid {{.UID}} --cidr {{.CIDR}} --dev {{.Dev}}
ExecStop={{.Helper}} --down --dev {{.Dev}}

[Install]
WantedBy=multi-user.target
`))

func deviceUnit(p deviceUnitParams) (string, error) {
	var b strings.Builder
	if err := deviceUnitTemplate.Execute(&b, p); err != nil {
		return "", err
	}
	return b.String(), nil
}

// deviceUnitFor renders the unit for cfg's device, owned by uid.
func deviceUnitFor(cfg *tunnelconfig.Config, uid int) (string, error) {
	if _, err := tunhelperutil.LocalAddr(cfg.FakeCIDR); err != nil {
		return "", fmt.Errorf("fake_cidr: %w", err)
	}
	return deviceUnit(deviceUnitParams{Helper: helperPath, UID: uid, CIDR: cfg.FakeCIDR, Dev: cfg.TUNName})
}

func splitListen(listen string) (host, port string, err error) {
	h, p, err := splitHostPort(listen)
	if err != nil {
		return "", "", fmt.Errorf("parsing dns_listen %q: %w", listen, err)
	}
	return h, p, nil
}
