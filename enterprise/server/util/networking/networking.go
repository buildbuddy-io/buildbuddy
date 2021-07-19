package networking

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

// runCommand runs the provided command, prepending sudo if the calling user is
// not already root.
func runCommand(ctx context.Context, args ...string) error {
	// If we're not running as root, use sudo.
	if unix.Getuid() != 0 {
		args = append([]string{"sudo"}, args...)
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return status.InternalErrorf("Error running %q %s: %s", cmd.String(), string(out), err)
	}
	log.Printf("Succesfully ran: %q", cmd.String())
	return nil
}

// namespace prepends the provided command with 'ip netns exec "netNamespace"'
// so that the provided command is run inside the network namespace.
func namespace(netNamespace string, args ...string) []string {
	return append([]string{"ip", "netns", "exec", netNamespace}, args...)
}

// CreateNetNamespace is equivalent to:
//   $ sudo ip netns add "netNamespace"
func CreateNetNamespace(ctx context.Context, netNamespace string) error {
	return runCommand(ctx, "ip", "netns", "add", netNamespace)
}

// CreateTapInNamespace is equivalent to:
//  $ sudo ip netns exec "netNamespace" ip tuntap add name "tapName" mode tap
func CreateTapInNamespace(ctx context.Context, netNamespace, tapName string) error {
	return runCommand(ctx, namespace(netNamespace, "ip", "tuntap", "add", "name", tapName, "mode", "tap")...)
}

// ConfigureTapInNamespace is equivalent to:
//  $ sudo ip netns exec "netNamespace" ip addr add "address" dev "tapName"
func ConfigureTapInNamespace(ctx context.Context, netNamespace, tapName, tapAddr string) error {
	return runCommand(ctx, namespace(netNamespace, "ip", "addr", "add", tapAddr, "dev", tapName)...)
}

// BringUpTapInNamespace is equivalent to:
//  $ sudo ip netns exec "netNamespace" ip link set "tapName" up
func BringUpTapInNamespace(ctx context.Context, netNamespace, tapName string) error {
	return runCommand(ctx, namespace(netNamespace, "ip", "link", "set", tapName, "up")...)
}

// randomVethName picks a random veth name like "veth0cD42A"
func randomVethName(prefix string) (string, error) {
	suffix, err := random.RandomString(5)
	if err != nil {
		return "", err
	}
	return prefix + suffix, nil
}

// createRandomVethPair attempts to create a veth pair with random names, the veth1 end of which will
// be in the root namespace.
func createRandomVethPair(ctx context.Context, netNamespace string) (string, string, error) {
	// compute unique veth names
	var veth0, veth1 string
	var err error
	for i := 0; i < 100; i++ {
		veth0, err = randomVethName("veth0")
		if err != nil {
			break
		}
		veth1, err = randomVethName("veth1")
		if err != nil {
			break
		}
		err = runCommand(ctx, namespace(netNamespace, "ip", "link", "add", veth1, "type", "veth", "peer", "name", veth0)...)
		if err != nil {
			if strings.Contains(err.Error(), "File exists") {
				continue
			}
			return "", "", err
		}
		break
	}
	//# move the veth1 end of the pair into the root namespace
	//sudo ip netns exec fc0 ip link set veth1 netns 1
	if err == nil {
		err = runCommand(ctx, namespace(netNamespace, "ip", "link", "set", veth1, "netns", "1")...)
	}
	return veth0, veth1, err
}

func incrementIP(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

func attachAddressToVeth(ctx context.Context, netNamespace, ipAddr, vethName string) error {
	if netNamespace != "" {
		return runCommand(ctx, namespace(netNamespace, "ip", "addr", "add", ipAddr, "dev", vethName)...)
	} else {
		return runCommand(ctx, "ip", "addr", "add", ipAddr, "dev", vethName)
	}
}

func RemoveVethPair(ctx context.Context, netNamespace string, vmIdx int) error {
	// This will delete the veth pair too, and the address attached
	// to the host-size of the veth pair.
	err := runCommand(ctx, "ip", "netns", "delete", netNamespace)
	if err != nil {
		return err
	}
	return nil
}

// SetupVethPair is equivalent to:
//  # create a new veth pair
//  $ sudo ip netns exec fc0 ip link add veth1 type veth peer name veth0
//
//  # move the veth1 end of the pair into the root namespace
//  $ sudo ip netns exec fc0 ip link set veth1 netns 1
//
//  # add the ip addr 10.0.0.2/24 to the veth0 end of the pair
//  $ sudo ip netns exec fc0 ip addr add 10.0.0.2/24 dev veth0
//
//  # bring the veth0 end of the pair up
//  $ sudo ip netns exec fc0 ip link set dev veth0 up
//
//  # add the ip addr 10.0.0.1/24 to the veth1 end of the pair
//  $ sudo ip addr add 10.0.0.1/24 dev veth1
//
//  # bring the veth1 end of the pair up
//  $ sudo ip link set dev veth1 up
//
//  # add a default route in the namespace to use 10.0.0.1 (aka the veth1 end of the pair)
//  $ sudo ip netns exec fc0 ip route add default via 10.0.0.1
//
//  # add an iptables mapping inside the namespace to rewrite 192.168.241.2 -> 192.168.0.3
//  $ sudo ip netns exec fc0 iptables -t nat -A POSTROUTING -o veth0 -s 192.168.241.2 -j SNAT --to 192.168.0.3
//
//  # add an iptables mapping inside the namespace to rewrite 192.168.0.3 -> 192.168.241.2
//  $ sudo ip netns exec fc0 iptables -t nat -A PREROUTING -i veth0 -d 192.168.0.3 -j DNAT --to 192.168.241.2
//
//  # add a route in the root namespace so that traffic to 192.168.0.3 hits 10.0.0.2, the veth0 end of the pair
//  $ sudo ip route add 192.168.0.3 via 10.0.0.2
func SetupVethPair(ctx context.Context, netNamespace, vmIP string, vmIdx int) error {
	// compute unique addresses for endpoints
	veth0, veth1, err := createRandomVethPair(ctx, netNamespace)
	if err != nil {
		return err
	}

	// This addr will be used for the host-side of the veth pair, so it
	// needs to to be unique on the host.
	hostEndpointNet := fmt.Sprintf("10.%d.%d.1/24", vmIdx/30, (vmIdx%30)*8)
	hostEndpointAddr := strings.SplitN(hostEndpointNet, "/", 2)[0]

	// Same as above, but ends with .2 instead of .1. Can be anything
	// because it's in a namespace.
	cloneEndpointNet := fmt.Sprintf("10.%d.%d.2/24", vmIdx/30, (vmIdx%30)*8)
	cloneEndpointAddr := strings.SplitN(cloneEndpointNet, "/", 2)[0]

	// This IP will be used as the clone-address so must be unique on the
	// host.
	cloneIP := fmt.Sprintf("192.168.%d.%d", vmIdx/30, ((vmIdx%30)*8)+3)

	err = attachAddressToVeth(ctx, netNamespace, cloneEndpointNet, veth0)
	if err != nil {
		return err
	}

	err = runCommand(ctx, namespace(netNamespace, "ip", "link", "set", "dev", veth0, "up")...)
	if err != nil {
		return err
	}

	err = attachAddressToVeth(ctx, "" /*no namespace*/, hostEndpointNet, veth1)
	if err != nil {
		return err
	}
	err = runCommand(ctx, "ip", "link", "set", "dev", veth1, "up")
	if err != nil {
		return err
	}

	err = runCommand(ctx, namespace(netNamespace, "ip", "route", "add", "default", "via", hostEndpointAddr)...)
	if err != nil {
		return err
	}

	err = runCommand(ctx, namespace(netNamespace, "iptables", "-t", "nat", "-A", "POSTROUTING", "-o", veth0, "-s", vmIP, "-j", "SNAT", "--to", cloneIP)...)
	if err != nil {
		return err
	}

	err = runCommand(ctx, namespace(netNamespace, "iptables", "-t", "nat", "-A", "PREROUTING", "-i", veth0, "-d", cloneIP, "-j", "DNAT", "--to", vmIP)...)
	if err != nil {
		return err
	}

	return runCommand(ctx, "ip", "route", "add", cloneIP, "via", cloneEndpointAddr)
}
