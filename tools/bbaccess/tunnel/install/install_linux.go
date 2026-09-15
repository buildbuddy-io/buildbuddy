// Package install performs the one-time privileged setup the tunnel needs:
// a TUN interface the daemon can open unprivileged, a route for the fake range,
// and split-DNS configuration pointing the zones' parent at the local
// resolver. None of it depends on the zone list, so zones can come and go
// afterwards without running it again.
package install

import (
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
)

const resolvedDropIn = "/etc/systemd/resolved.conf.d/buildbuddy-tunnel.conf"

// Install creates the persistent TUN device, configures it, and points the
// zones' parent domain at the daemon.
//
// Everything privileged happens here, once, so that `bbaccess tunnel run` needs no
// privileges at all: the TUN device is created with `user`, which lets the
// developer's own account open it later.
func Install(cfg *config.Config, user string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel install")
	}
	if user == "" {
		user = os.Getenv("SUDO_USER")
	}
	if user == "" {
		return fmt.Errorf("cannot determine which user should own the TUN device; pass --user")
	}

	prefix, err := netip.ParsePrefix(cfg.FakeCIDR)
	if err != nil {
		return fmt.Errorf("parsing fake_cidr %q: %w", cfg.FakeCIDR, err)
	}
	addr := firstAddr(prefix)

	// Recreate the interface so a changed owner or CIDR takes effect.
	run("ip", "link", "del", cfg.TUNName)

	if out, err := exec.Command("ip", "tuntap", "add", "dev", cfg.TUNName, "mode", "tun", "user", user).CombinedOutput(); err != nil {
		return fmt.Errorf("creating %s: %w: %s", cfg.TUNName, err, out)
	}
	// The MTU is set here because changing it later needs CAP_NET_ADMIN, which
	// the unprivileged daemon will not have.
	if out, err := exec.Command("ip", "link", "set", "dev", cfg.TUNName, "mtu", "1400", "up").CombinedOutput(); err != nil {
		return fmt.Errorf("bringing up %s: %w: %s", cfg.TUNName, err, out)
	}
	// Assigning the whole prefix to the interface installs the route for the
	// entire fake range as a connected route.
	if out, err := exec.Command("ip", "addr", "add", fmt.Sprintf("%s/%d", addr, prefix.Bits()), "dev", cfg.TUNName).CombinedOutput(); err != nil {
		return fmt.Errorf("assigning %s to %s: %w: %s", addr, cfg.TUNName, err, out)
	}
	fmt.Printf("Created %s (owner %s), routing %s\n", cfg.TUNName, user, cfg.FakeCIDR)

	if err := installResolved(cfg); err != nil {
		return err
	}
	fmt.Printf("\nInstalled. Start the daemon with:\n\n    bbaccess tunnel run\n\n")
	return nil
}

// installResolved writes a systemd-resolved drop-in that routes the zones'
// parent to the daemon's DNS server. The "~" prefix makes each entry a routing
// domain: queries for those suffixes go here, everything else is untouched.
func installResolved(cfg *config.Config) error {
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
func Uninstall(cfg *config.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel uninstall")
	}
	run("ip", "link", "del", cfg.TUNName)
	if err := os.Remove(resolvedDropIn); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s: %w", resolvedDropIn, err)
	}
	run("systemctl", "restart", "systemd-resolved")
	fmt.Printf("Removed %s and %s\n", cfg.TUNName, resolvedDropIn)
	return nil
}

func run(name string, args ...string) {
	exec.Command(name, args...).Run()
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
