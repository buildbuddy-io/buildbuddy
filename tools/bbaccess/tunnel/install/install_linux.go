// Package install performs the privileged setup the tunnel needs:
//   - creates the TUN device through which relayed traffic flows and adds routes
//     to send the reserved relay range to the TUN device.
//   - configures systemd DNS resolver to route the relayed suffix to the bbaccess
//     DNS resolver
package install

import (
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

const resolvedDropIn = "/etc/systemd/resolved.conf.d/buildbuddy-tunnel.conf"

func Install(cfg *tunnelconfig.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel install")
	}
	// The TUN device belongs to the person who ran sudo, not to root.
	user := os.Getenv("SUDO_USER")
	if user == "" {
		return fmt.Errorf("cannot tell which user should own the TUN device: run this through sudo")
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
