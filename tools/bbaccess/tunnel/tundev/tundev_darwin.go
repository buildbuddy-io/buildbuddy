package tundev

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"

	"golang.zx2c4.com/wireguard/tun"
)

// Open creates a utun interface.
//
// Unlike Linux, macOS utun devices cannot be persistent: the interface exists
// only while the file descriptor that created it is open, and creating one
// requires root. So the daemon itself must run privileged on macOS (`sudo bb
// tunnel run`), and address/route configuration happens at startup rather than
// at install time.
func Open(name string) (tun.Device, error) {
	if os.Geteuid() != 0 {
		return nil, fmt.Errorf("creating a utun interface requires root on macOS; run: sudo bbaccess tunnel run")
	}
	// The "utun" prefix asks the kernel for the next free unit.
	dev, err := tun.CreateTUN("utun", 1400)
	if err != nil {
		return nil, fmt.Errorf("creating utun interface: %w", err)
	}
	return dev, nil
}

// EnsureConfigured assigns the fake range to the interface and routes it there.
// On macOS this must happen every time the daemon starts, because the interface
// is created fresh each time.
func EnsureConfigured(dev tun.Device, cidr string) error {
	name, err := dev.Name()
	if err != nil {
		return fmt.Errorf("reading interface name: %w", err)
	}
	// ifconfig wants a point-to-point pair; the addresses themselves don't
	// matter as long as they are inside the range we route here.
	local, peer, mask, err := pointToPoint(cidr)
	if err != nil {
		return err
	}
	if out, err := exec.Command("ifconfig", name, local, peer, "netmask", mask, "up").CombinedOutput(); err != nil {
		return fmt.Errorf("ifconfig %s: %w: %s", name, err, out)
	}
	if out, err := exec.Command("route", "-q", "-n", "add", "-net", cidr, "-interface", name).CombinedOutput(); err != nil {
		return fmt.Errorf("route add %s: %w: %s", cidr, err, out)
	}
	return nil
}

// RequiresRoot reports whether the daemon needs to run as root on this OS.
func RequiresRoot() bool { return true }

// pointToPoint derives the local address, peer address, and netmask to
// configure on the interface serving cidr. It uses the first two addresses in
// the range, which the fake-IP allocator reserves.
func pointToPoint(cidr string) (local, peer, mask string, err error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return "", "", "", fmt.Errorf("parsing %q: %w", cidr, err)
	}
	if !prefix.Addr().Is4() {
		return "", "", "", fmt.Errorf("%q is not an IPv4 range", cidr)
	}
	base := prefix.Masked().Addr().As4()
	localAddr := netip.AddrFrom4([4]byte{base[0], base[1], base[2], base[3] | 1})
	peerAddr := netip.AddrFrom4([4]byte{base[0], base[1], base[2], base[3] | 2})
	m := net.CIDRMask(prefix.Bits(), 32)
	return localAddr.String(), peerAddr.String(), net.IP(m).String(), nil
}
