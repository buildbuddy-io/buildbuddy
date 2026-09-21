//go:build !android

// Package install performs the privileged setup the tunnel needs:
//   - a systemd unit that creates the TUN device through which relayed traffic
//     flows so that it survives reboots.
//   - a systemd-resolved drop-in routing the relayed suffix to the bbaccess
//     DNS resolver.
package install

import (
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"text/template"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

const (
	resolvedDropIn = "/etc/systemd/resolved.conf.d/buildbuddy-tunnel.conf"
	deviceUnitName = "bbaccess-tunnel-device.service"
	deviceUnitPath = "/etc/systemd/system/" + deviceUnitName
)

func Install(cfg *tunnelconfig.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel install")
	}
	// The TUN device belongs to the person who ran sudo, not to root.
	user := os.Getenv("SUDO_USER")
	if user == "" {
		return fmt.Errorf("cannot tell which user should own the TUN device: run this through sudo")
	}
	// Sanity check values before we write the unit file.
	if !unitWord.MatchString(user) {
		return fmt.Errorf("SUDO_USER %q is not a plain user name", user)
	}
	if !unitWord.MatchString(cfg.TUNName) || len(cfg.TUNName) > maxIfaceName {
		return fmt.Errorf("tun_name %q must be 1-%d characters from [A-Za-z0-9._-]", cfg.TUNName, maxIfaceName)
	}

	prefix, err := netip.ParsePrefix(cfg.FakeCIDR)
	if err != nil {
		return fmt.Errorf("parsing fake_cidr %q: %w", cfg.FakeCIDR, err)
	}
	if !prefix.Addr().Is4() {
		return fmt.Errorf("fake_cidr %q must be an IPv4 range", cfg.FakeCIDR)
	}
	addr := netip.PrefixFrom(firstAddr(prefix), prefix.Bits())

	// The unit needs an absolute path.
	// ip lives in /sbin or /usr/sbin depending on the distribution.
	ipPath, err := exec.LookPath("ip")
	if err != nil {
		return fmt.Errorf("finding the ip command: %w", err)
	}
	unit, err := deviceUnit(deviceUnitParams{IPCommand: ipPath, User: user, Dev: cfg.TUNName, Addr: addr, MTU: tunMTU})
	if err != nil {
		return fmt.Errorf("rendering %s: %w", deviceUnitName, err)
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
	fmt.Printf("Created %s (owner %s), routing %s; %s recreates it at boot\n", cfg.TUNName, user, cfg.FakeCIDR, deviceUnitName)

	if err := installResolved(cfg); err != nil {
		return err
	}
	fmt.Printf("\nInstalled. Start the daemon with:\n\n    bbaccess tunnel run\n\n")
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
	routing := make([]string, 0, len(domains))
	for _, d := range domains {
		routing = append(routing, "~"+d)
	}

	content := fmt.Sprintf(`# Written by "bbaccess tunnel install". Remove with "bbaccess tunnel uninstall".
[Resolve]
DNS=%s:%s
Domains=%s
`, host, port, strings.Join(routing, " "))

	if err := os.MkdirAll("/etc/systemd/resolved.conf.d", 0o755); err != nil {
		return fmt.Errorf("creating /etc/systemd/resolved.conf.d: %w", err)
	}
	if err := os.WriteFile(resolvedDropIn, []byte(content), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", resolvedDropIn, err)
	}
	if out, err := exec.Command("systemctl", "restart", "systemd-resolved").CombinedOutput(); err != nil {
		return fmt.Errorf("restarting systemd-resolved: %w: %s\n\nIf this machine does not use systemd-resolved, "+
			"point your resolver at %s for these domains manually: %s",
			err, out, cfg.DNSListen, strings.Join(domains, " "))
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
	fmt.Printf("Removed %s, %s and %s\n", cfg.TUNName, deviceUnitPath, resolvedDropIn)
	return nil
}

func run(name string, args ...string) {
	exec.Command(name, args...).Run()
}

const tunMTU = 1400

// maxIfaceName is IFNAMSIZ minus the terminator.
const maxIfaceName = 15

// unitWord is used to sanity check the tun device name and username in the unit
var unitWord = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// deviceUnitParams fills the systemd unit that creates the TUN device.
type deviceUnitParams struct {
	IPCommand string       // absolute path of the ip command
	User      string       // owner of the device
	Dev       string       // interface name
	Addr      netip.Prefix // the device's address
	MTU       int
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
ExecStartPre=-{{.IPCommand}} link del {{.Dev}}
ExecStart={{.IPCommand}} tuntap add dev {{.Dev}} mode tun user {{.User}}
ExecStart={{.IPCommand}} link set dev {{.Dev}} mtu {{.MTU}} up
ExecStart={{.IPCommand}} addr add {{.Addr}} dev {{.Dev}}
ExecStop={{.IPCommand}} link del {{.Dev}}

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

func firstAddr(prefix netip.Prefix) netip.Addr {
	b := prefix.Masked().Addr().As4()
	return netip.AddrFrom4([4]byte{b[0], b[1], b[2], b[3] | 1})
}

func splitListen(listen string) (host, port string, err error) {
	h, p, err := splitHostPort(listen)
	if err != nil {
		return "", "", fmt.Errorf("parsing dns_listen %q: %w", listen, err)
	}
	return h, p, nil
}
