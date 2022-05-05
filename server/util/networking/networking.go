package networking

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
)

var (
	devicePrefix = flag.String("device_prefix", "default", "The prefix in the ip route to locate a device: either 'default' or the ip range of the subnet e.g. 172.24.0.0/18")
)

// runCommand runs the provided command, prepending sudo if the calling user is
// not already root. Output and errors are returned.
func sudoCommand(ctx context.Context, args ...string) ([]byte, error) {
	// If we're not running as root, use sudo.
	if unix.Getuid() != 0 {
		args = append([]string{"sudo"}, args...)
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, status.InternalErrorf("Error running %q %s: %s", cmd.String(), string(out), err)
	}
	log.Debugf("Succesfully ran: %q", cmd.String())
	return out, nil
}

// runCommand runs the provided command, prepending sudo if the calling user is
// not already root, and returns any error encountered.
func runCommand(ctx context.Context, args ...string) error {
	_, err := sudoCommand(ctx, args...)
	return err
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

func removeForwardAcceptRule(ctx context.Context, vethName, defaultDevice string) error {
	return runCommand(ctx, "iptables", "--delete", "FORWARD", "-i", vethName, "-o", defaultDevice, "-j", "ACCEPT")
}

func RemoveNetNamespace(ctx context.Context, netNamespace string) error {
	// This will delete the veth pair too, and the address attached
	// to the host-size of the veth pair.
	return runCommand(ctx, "ip", "netns", "delete", netNamespace)
}

func DeleteRoute(ctx context.Context, vmIdx int) error {
	cloneIP := fmt.Sprintf("192.168.%d.%d", vmIdx/30, ((vmIdx%30)*8)+3)
	return runCommand(ctx, "ip", "route", "delete", cloneIP)
}

// SetupVethPair creates a new veth pair with one end in the given network
// namespace and the other end in the root namespace. It returns a cleanup
// function that removes firewall rules associated with the pair.
//
// It is equivalent to:
//
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
//  # add a firewall rule to allow forwarding traffic from the veth1 pair to the
//  # default device, in case forwarding is not allowed by default
//  $ sudo iptables -A FORWARD -i veth1 -o eth0 -j ACCEPT
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
func SetupVethPair(ctx context.Context, netNamespace, vmIP string, vmIdx int) (func(context.Context) error, error) {
	defaultDevice, err := findDevice(ctx)
	if err != nil {
		return nil, err
	}

	// compute unique addresses for endpoints
	veth0, veth1, err := createRandomVethPair(ctx, netNamespace)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	err = runCommand(ctx, namespace(netNamespace, "ip", "link", "set", "dev", veth0, "up")...)
	if err != nil {
		return nil, err
	}

	err = attachAddressToVeth(ctx, "" /*no namespace*/, hostEndpointNet, veth1)
	if err != nil {
		return nil, err
	}

	err = runCommand(ctx, "ip", "link", "set", "dev", veth1, "up")
	if err != nil {
		return nil, err
	}

	err = runCommand(ctx, namespace(netNamespace, "ip", "route", "add", "default", "via", hostEndpointAddr)...)
	if err != nil {
		return nil, err
	}

	err = runCommand(ctx, namespace(netNamespace, "iptables", "-t", "nat", "-A", "POSTROUTING", "-o", veth0, "-s", vmIP, "-j", "SNAT", "--to", cloneIP)...)
	if err != nil {
		return nil, err
	}

	err = runCommand(ctx, namespace(netNamespace, "iptables", "-t", "nat", "-A", "PREROUTING", "-i", veth0, "-d", cloneIP, "-j", "DNAT", "--to", vmIP)...)
	if err != nil {
		return nil, err
	}

	err = runCommand(ctx, "ip", "route", "add", cloneIP, "via", cloneEndpointAddr)
	if err != nil {
		return nil, err
	}

	err = runCommand(ctx, "iptables", "-A", "FORWARD", "-i", veth1, "-o", defaultDevice, "-j", "ACCEPT")
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		return removeForwardAcceptRule(ctx, veth1, defaultDevice)
	}, nil
}

func DefaultInterface(ctx context.Context) (*net.Interface, error) {
	defaultDevName, err := findDevice(ctx)
	if err != nil {
		return nil, err
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Name == defaultDevName {
			return &iface, nil
		}
	}
	return nil, status.NotFoundErrorf("could not find interface %q", defaultDevName)
}

// findDevice find's the device used for the default route.
// Equivalent to "ip route | grep default | awk '{print $5}'"
func findDevice(ctx context.Context) (string, error) {
	out, err := sudoCommand(ctx, "ip", "route")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, *devicePrefix) {
			if parts := strings.Split(line, " "); len(parts) > 5 {
				return parts[4], nil
			}
		}
	}
	return "", status.FailedPreconditionError("Unable to determine default device.")
}

// FindDefaultRouteIP finds the default IP used for routing traffic, typically
// corresponding to a local router or access point.
//
// Equivalent to "ip route | grep default | awk '{print $3}'.
func FindDefaultRouteIP(ctx context.Context) (string, error) {
	out, err := sudoCommand(ctx, "ip", "route")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "default") {
			if parts := strings.Split(line, " "); len(parts) > 5 {
				return parts[2], nil
			}
		}
	}
	return "", status.FailedPreconditionError("Unable to determine default route.")
}

// EnableMasquerading turns on ipmasq for the default device. This is required
// for networking to work on vms.
func EnableMasquerading(ctx context.Context) error {
	defaultDevice, err := findDevice(ctx)
	if err != nil {
		return err
	}
	// Skip appending the rule if it's already in the table.
	err = runCommand(ctx, "iptables", "-t", "nat", "--check", "POSTROUTING", "-o", defaultDevice, "-j", "MASQUERADE")
	if err == nil {
		return nil
	}
	return runCommand(ctx, "iptables", "-t", "nat", "-A", "POSTROUTING", "-o", defaultDevice, "-j", "MASQUERADE")
}
