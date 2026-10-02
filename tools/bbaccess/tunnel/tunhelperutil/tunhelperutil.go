package tunhelperutil

import (
	"fmt"
	"net/netip"
)

// Version is the device helper's version. Bump it to make bbaccess replace
// installed helpers.
const Version = 1

// LocalAddr returns the IP assigned to the tun device.
func LocalAddr(cidr string) (netip.Addr, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("parsing %q: %w", cidr, err)
	}
	if !prefix.Addr().Is4() {
		return netip.Addr{}, fmt.Errorf("%q is not an IPv4 range", cidr)
	}
	return prefix.Masked().Addr().Next(), nil
}
