package networking

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/vishvananda/netlink"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var (
	routePrefix                   = flag.String("executor.route_prefix", defaultRoute, "The prefix in the ip route to locate a device: either 'default' or the ip range of the subnet e.g. 172.24.0.0/18")
	blackholePrivateRanges        = flag.Bool("executor.blackhole_private_ranges", false, "If true, no traffic will be allowed to RFC1918 ranges.")
	preserveExistingNetNamespaces = flag.Bool("executor.preserve_existing_netns", false, "Preserve existing bb-executor net namespaces. By default all \"bb-executor\" net namespaces are removed on executor startup, but if multiple executors are running on the same machine this behavior should be disabled to prevent them interfering with each other.")
	natSourcePortRange            = flag.String("executor.nat_source_port_range", "", "If set, restrict the source ports for NATed traffic to this range. ")
	networkLockDir                = flag.String("executor.network_lock_directory", "", "If set, use this directory to store lockfiles for allocated IP ranges. This is required if running multiple executors within the same networking environment.")

	// Private IP ranges, as defined in RFC1918.
	PrivateIPRanges = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16"}
)

const (
	defaultRoute         = "default"
	routingTableFilename = "/etc/iproute2/rt_tables"
	// The routingTableID for the new routing table we add.
	routingTableID = 1
	// The routingTableName for the new routing table we add.
	routingTableName = "rt1"
	// netns prefix to use to identify executor namespaces.
	netNamespacePrefix = "bb-executor-"
	// Total number of available host IP ranges that can be allocated to VMs.
	numAssignableNetworks = 1000
	// Time to allow for networking cleanup. We intentionally use a long-ish
	// timeout here because if cleanup fails then we might leave the network in
	// a bad state, preventing a host IP from being usable.
	networkingCleanupTimeout = 1 * time.Minute

	// CIDR suffix for veth-based networks. We only need 2 IP addresses, one for
	// the host end and one for the namespaced end.
	cidrSuffix = "/30"

	// CIDR matching all container networks on the host.
	containerNetworkingCIDR = "192.168.0.0/16"
)

var (
	// Default pool size limit to use when network pooling is enabled and the
	// default size limit is requested.
	//
	// This value is big enough to allow an executor to burst from 0%
	// utilization to 100% utilization while allowing all tasks to use a pooled
	// network. (The number 4 is based on the current min CPU task size estimate
	// of 250m)
	defaultContainerNetworkPoolSizeLimit = runtime.NumCPU() * 4
)

// runCommand runs the provided command, prepending sudo if the calling user is
// not already root. Output and errors are returned.
func sudoCommand(ctx context.Context, args ...string) ([]byte, error) {
	// If we're not running as root, use sudo.
	// Use "-A" to ensure we never get stuck prompting for
	// a password interactively.
	if unix.Getuid() != 0 {
		args = append([]string{"sudo", "-A"}, args...)
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, status.InternalErrorf("run %q: %s: %s", cmd, err, string(out))
	}
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
func namespace(netns *Namespace, args ...string) []string {
	return append([]string{"ip", "netns", "exec", netns.name}, args...)
}

// Deletes all of the executor net namespaces. These can be left behind if the
// executor doesn't exit gracefully.
func DeleteNetNamespaces(ctx context.Context) error {
	// "ip netns delete" doesn't support patterns, so we list all
	// namespaces then delete the ones that match the bb executor pattern.
	b, err := sudoCommand(ctx, "ip", "netns", "list")
	if err != nil {
		return err
	}
	output := strings.TrimSpace(string(b))
	if len(output) == 0 {
		return nil
	}
	var lastErr error
	for _, ns := range strings.Split(output, "\n") {
		// Sometimes the output contains spaces, like
		//     bb-executor-1
		//     bb-executor-2
		//     3fe4313e-eb76-4b6d-9d61-53caf12b87e6 (id: 344)
		//     2ab15e85-d1c3-47bc-ad40-74e2941157a4 (id: 332)
		// So we get just the first column here.
		fields := strings.Fields(ns)
		if len(fields) > 0 {
			ns = fields[0]
		}
		if !strings.HasPrefix(ns, netNamespacePrefix) {
			continue
		}
		if _, err := sudoCommand(ctx, "ip", "netns", "delete", ns); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Namespace represents a network namespace that has been created.
type Namespace struct {
	name string
}

// createUniqueNetNamespace creates a new unique net namespace.
func createUniqueNetNamespace(ctx context.Context) (*Namespace, error) {
	name := netNamespacePrefix + uuid.New()
	if err := runCommand(ctx, "ip", "netns", "add", name); err != nil {
		return nil, err
	}
	return &Namespace{name: name}, nil
}

// Path returns the full filesystem path of the provided network namespace.
func (ns *Namespace) Path() string {
	return "/var/run/netns/" + ns.name
}

// Delete deletes the namespace.
//
// Deleting a namespace also deletes all resources inside the namespace, but
// this cleanup happens asynchronously - possibly after this function has
// already returned. If any guarantees are needed about when the namespaced
// resources are cleaned up, then the resources in the namespace should be
// deleted explicitly.
//
// Deleting a namespace does not delete iptables rules on the host that were
// responsible for routing traffic to/from the interfaces within the namespace.
func (ns *Namespace) Delete(ctx context.Context) error {
	return runCommand(ctx, "ip", "netns", "delete", ns.name)
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
func createRandomVethPair(ctx context.Context, netns *Namespace) (string, string, error) {
	var namespacedVeth, hostVeth string
	var err error
	for i := 0; i < 100; i++ {
		// Compute unique veth names
		namespacedVeth, err = randomVethName("veth0")
		if err != nil {
			break
		}
		hostVeth, err = randomVethName("veth1")
		if err != nil {
			break
		}
		err = runCommand(ctx, namespace(netns, "ip", "link", "add", hostVeth, "type", "veth", "peer", "name", namespacedVeth)...)
		if err != nil {
			if strings.Contains(err.Error(), "File exists") {
				continue
			}
			return "", "", err
		}
		break
	}
	// Move one end of the pair to the host.
	if err == nil {
		err = runCommand(ctx, namespace(netns, "ip", "link", "set", hostVeth, "netns", "1")...)
	}
	return namespacedVeth, hostVeth, err
}

func attachAddressToVeth(ctx context.Context, netns *Namespace, ipAddr, vethName string) error {
	if netns != nil {
		return runCommand(ctx, namespace(netns, "ip", "addr", "add", ipAddr, "dev", vethName)...)
	} else {
		return runCommand(ctx, "ip", "addr", "add", ipAddr, "dev", vethName)
	}
}

// ContainerNetworkPool holds a pool of container networks that can be reused
// across container instances. This pooling helps to reduce the performance
// overhead associated with rapidly creating and destroying networks along with
// all of their associated configuration.
//
// TODO: consolidate logic so that VM networks can be pooled too. VMs have an
// additional TAP device which isn't needed for container networks, but most of
// the other setup is the same.
type ContainerNetworkPool struct {
	sizeLimit int

	mu           sync.Mutex
	resources    []*ContainerNetwork
	shuttingDown bool
}

func NewContainerNetworkPool(sizeLimit int) *ContainerNetworkPool {
	if sizeLimit < 0 {
		sizeLimit = defaultContainerNetworkPoolSizeLimit
	}
	return &ContainerNetworkPool{
		sizeLimit: sizeLimit,
	}
}

// Get returns a pooled veth pair, or nil if there are no pooled veth pairs
// available.
func (p *ContainerNetworkPool) Get(ctx context.Context) (n *ContainerNetwork) {
	n = p.get()
	if n == nil {
		return nil
	}

	// If we fail to fully set up the network, then we're on the hook for
	// cleaning it up, since we already took it from the pool.
	defer func() {
		if n != nil {
			return
		}
		ctx, cancel := background.ExtendContextForFinalization(ctx, networkingCleanupTimeout)
		defer cancel()
		if err := n.Cleanup(ctx); err != nil {
			log.CtxErrorf(ctx, "Failed to clean up pooled network in partially set up state: %s", err)
		}
	}()

	// Assign a new IP before returning the network from the pool.
	network, err := hostNetAllocator.Get()
	if err != nil {
		log.CtxErrorf(ctx, "Failed to allocate new IP range for pooled network: %s", err)
		return nil
	}
	n.vethPair.network = network

	// Assign IPs to the host and namespaced side, and create the default route
	// in the namespace.
	if err := attachAddressToVeth(ctx, nil /*=namespace*/, network.HostIPWithCIDR(), n.vethPair.hostDevice); err != nil {
		log.CtxErrorf(ctx, "Failed to attach address to pooled host veth interface: %s", err)
		return nil
	}
	if err := attachAddressToVeth(ctx, n.vethPair.netns, network.NamespacedIPWithCIDR(), n.vethPair.namespacedDevice); err != nil {
		log.CtxErrorf(ctx, "Failed to attach address to pooled namespaced veth interface: %s", err)
		return nil
	}
	if err := runCommand(ctx, namespace(n.vethPair.netns, "ip", "route", "add", "default", "via", network.HostIP())...); err != nil {
		log.CtxErrorf(ctx, "Failed to set up default route in namespace: %s", err)
		return nil
	}

	return n
}

func (p *ContainerNetworkPool) get() *ContainerNetwork {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.resources) == 0 {
		return nil
	}

	head, tail := p.resources[0], p.resources[1:]
	p.resources = tail
	return head
}

// Add adds a veth pair to the pool.
// It returns whether the veth pair was successfully added.
// The caller should clean up the veth pair if this returns false.
func (p *ContainerNetworkPool) Add(ctx context.Context, n *ContainerNetwork) (ok bool) {
	// Unassign the IP addresses before adding to the pool. We'll later assign a
	// new IP when taking the network back out of the pool.
	if err := n.vethPair.RemoveAddrs(ctx); err != nil {
		log.CtxErrorf(ctx, "Failed to remove IP addresses from network before adding to pool: %s", err)
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shuttingDown || len(p.resources) >= p.sizeLimit {
		return false
	}

	p.resources = append(p.resources, n)
	return true
}

// Shutdown cleans up any pooled resources and prevents new resources from being
// returned by the pool.
func (p *ContainerNetworkPool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.shuttingDown = true
	resources := p.resources
	p.resources = nil
	p.mu.Unlock()

	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU())
	for _, r := range resources {
		eg.Go(func() error {
			if err := r.Cleanup(ctx); err != nil {
				log.CtxErrorf(ctx, "Failed to cleanup veth pair: %s", err)
				return err
			}
			return nil
		})
	}
	return eg.Wait()
}

// HostNetAllocator assigns unique /30 networks from the host for use in VMs.
type HostNetAllocator struct {
	mu sync.Mutex
	// Next index to try locking; wraps around at numAssignableNetworks.
	// Since most tasks are short-lived, this usually will point to an index
	// that is not currently in use.
	idx      int
	networks [numAssignableNetworks]struct {
		// Whether the network is locked.
		locked bool
		// lockfile handle, if a lockfile directory is configured and the
		// network is locked.
		lockfile *os.File
	}
}

var hostNetAllocator = &HostNetAllocator{}

// HostNet represents a reserved /30 network from the host for use in a VM.
type HostNet struct {
	netIdx int
	unlock func()
}

func (n *HostNet) CIDR() string {
	return fmt.Sprintf("192.168.%d.%d", n.netIdx/30, (n.netIdx%30)*8+4) + cidrSuffix
}

func (n *HostNet) HostIP() string {
	return fmt.Sprintf("192.168.%d.%d", n.netIdx/30, (n.netIdx%30)*8+5)
}

func (n *HostNet) HostIPWithCIDR() string {
	return n.HostIP() + cidrSuffix
}

func (n *HostNet) NamespacedIP() string {
	return fmt.Sprintf("192.168.%d.%d", n.netIdx/30, ((n.netIdx%30)*8)+6)
}

func (n *HostNet) NamespacedIPWithCIDR() string {
	return n.NamespacedIP() + cidrSuffix
}

func (n *HostNet) Unlock() {
	if n.unlock == nil {
		alert.UnexpectedEvent("ip_range_double_unlock", "Attempted to unlock an assigned IP range more than once.")
		return
	}
	n.unlock()
	n.unlock = nil
}

// Get assigns a host network IP for the given VM index.
func (a *HostNetAllocator) Get() (*HostNet, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for attempt := 0; attempt < numAssignableNetworks; attempt++ {
		netIdx := a.idx
		a.idx = (a.idx + 1) % numAssignableNetworks

		net := &a.networks[netIdx]

		if net.locked {
			continue
		}

		// If a lock directory is configured (for locking across processes) then
		// try to acquire the lockfile for the netIdx before we mark it locked.
		if *networkLockDir != "" {
			f, err := a.tryFlock(netIdx)
			if err != nil {
				return nil, status.UnavailableErrorf("lock network index: %s", err)
			}
			if f == nil {
				// Locked by another process - try the next one.
				continue
			}
			net.lockfile = f
		}

		net.locked = true

		return &HostNet{
			netIdx: netIdx,
			unlock: func() { a.unlock(netIdx) },
		}, nil
	}
	return nil, status.ResourceExhaustedError("host IP address space exhausted")
}

// tryFlock attempts to acquire the lockfile for the given net index.
// It does not block - if the lock is already held then it returns a nil file
// handle.
// If a non-nil file handle is returned, the file must be closed to release the
// lock.
func (a *HostNetAllocator) tryFlock(netIdx int) (*os.File, error) {
	if err := os.MkdirAll(*networkLockDir, 0755); err != nil {
		return nil, fmt.Errorf("make lock dir %q: %s", *networkLockDir, err)
	}
	path := filepath.Join(*networkLockDir, fmt.Sprintf("ip_range.%d.lock", netIdx))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("init lockfile %q: %s", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if err == syscall.EWOULDBLOCK {
			// Lock is already held.
			return nil, nil
		}
		return nil, err
	}
	return f, nil
}

func (a *HostNetAllocator) unlock(netIdx int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	net := &a.networks[netIdx]
	net.locked = false
	if f := net.lockfile; f != nil {
		net.lockfile = nil
		if err := f.Close(); err != nil {
			alert.UnexpectedEvent("close_lockfile_failed", "Failed to release network lockfile: %s", err)
		}
	}
}

// vethPair represents a veth pair with one end in the host and the other end
// in a namespace.
type vethPair struct {
	// hostDevice is the name of the end of the veth pair which is in the
	// root net namespace.
	hostDevice string

	// namespacedDevice is the name of the end of the veth pair which is in the
	// namespace.
	namespacedDevice string

	// The network namespace where one end of the pair is located.
	netns *Namespace

	// Network information for the veth pair.
	network *HostNet

	// Cleanup deletes the veth pair and associated host IP configuration
	// changes.
	Cleanup func(ctx context.Context) error
}

// setupVethPair creates a new veth pair with one end in the given network
// namespace and the other end in the root namespace.
//
// The Cleanup method must be called on the returned struct to clean up all
// resources associated with it.
func setupVethPair(ctx context.Context, netns *Namespace) (_ *vethPair, err error) {
	// Keep a list of cleanup work to be done.
	var cleanupStack cleanupStack
	// If we return an error from this func then we need to clean up any
	// resources that were created before returning.
	defer func() {
		if err != nil {
			_ = cleanupStack.Cleanup(ctx)
		}
	}()

	r, err := findRoute(*routePrefix)
	if err != nil {
		return nil, err
	}
	device := r.device

	vp := &vethPair{netns: netns}

	// Reserve an IP range for the veth pair.
	vp.network, err = hostNetAllocator.Get()
	if err != nil {
		return nil, status.WrapError(err, "assign host network to VM")
	}
	cleanupStack = append(cleanupStack, func(ctx context.Context) error {
		// IP addresses are unassigned when adding the veth pair to a pool, so
		// check whether there is a network present here so that we don't
		// double-unlock.
		if vp.network != nil {
			vp.network.Unlock()
		}
		return nil
	})

	// Create a veth pair with randomly generated names.
	vp.namespacedDevice, vp.hostDevice, err = createRandomVethPair(ctx, netns)
	if err != nil {
		return nil, err
	}
	// Deleting the net namespace deletes veth1 automatically, but it does so
	// asynchronously. To avoid race conditions, we delete it manually here to
	// ensure that the cleanup happens before we release the lock on the
	// host IP range.
	cleanupStack = append(cleanupStack, func(ctx context.Context) error {
		return runCommand(ctx, "ip", "link", "delete", vp.hostDevice)
	})

	// Attach IP addresses to the host and namespaced ends of the veth pair,
	// and bring them up.
	err = attachAddressToVeth(ctx, netns, vp.network.NamespacedIPWithCIDR(), vp.namespacedDevice)
	if err != nil {
		return nil, status.WrapError(err, "attach address to veth device in namespace")
	}
	err = runCommand(ctx, namespace(netns, "ip", "link", "set", "dev", vp.namespacedDevice, "up")...)
	if err != nil {
		return nil, err
	}
	err = attachAddressToVeth(ctx, nil /*=namespace*/, vp.network.HostIPWithCIDR(), vp.hostDevice)
	if err != nil {
		return nil, err
	}
	err = runCommand(ctx, "ip", "link", "set", "dev", vp.hostDevice, "up")
	if err != nil {
		return nil, err
	}

	// Route traffic via the host end of the pair by default.
	err = runCommand(ctx, namespace(netns, "ip", "route", "add", "default", "via", vp.network.HostIP())...)
	if err != nil {
		return nil, status.WrapError(err, "add default route in namespace")
	}

	if IsSecondaryNetworkEnabled() {
		err = runCommand(ctx, "ip", "rule", "add", "from", vp.network.NamespacedIP(), "lookup", routingTableName)
		if err != nil {
			return nil, err
		}
		cleanupStack = append(cleanupStack, func(ctx context.Context) error {
			return runCommand(ctx, "ip", "rule", "del", "from", vp.network.NamespacedIP())
		})
	}

	iptablesRules := [][]string{
		// Allow forwarding traffic between the host side of the veth pair and
		// the device associated with the configured route prefix (usually the
		// default route). This is necessary on hosts with default-deny policies
		// in place.
		{"FORWARD", "-i", vp.hostDevice, "-o", device, "-j", "ACCEPT"},
		{"FORWARD", "-i", device, "-o", vp.hostDevice, "-j", "ACCEPT"},

		// Drop any traffic from the namespace that is targeting another
		// namespace.
		{"FORWARD", "-i", vp.hostDevice, "-d", containerNetworkingCIDR, "-j", "DROP"},
	}

	for _, rule := range iptablesRules {
		if err := runCommand(ctx, append([]string{"iptables", "--wait", "-A"}, rule...)...); err != nil {
			return nil, err
		}
		cleanupStack = append(cleanupStack, func(ctx context.Context) error {
			return runCommand(ctx, append([]string{"iptables", "--wait", "--delete"}, rule...)...)
		})
	}

	vp.Cleanup = cleanupStack.Cleanup
	return vp, nil
}

// RemoveAddrs unassigns the IP addresses from the host and veth side of the
// VethPair, and unlocks the associated host IP range.
func (v *vethPair) RemoveAddrs(ctx context.Context) error {
	var lastErr error
	if err := runCommand(ctx, "ip", "addr", "del", v.network.HostIPWithCIDR(), "dev", v.hostDevice); err != nil {
		log.CtxErrorf(ctx, "Failed to delete IP address %s from %s: %s", v.network.HostIPWithCIDR(), v.hostDevice, err)
		lastErr = err
	}
	if err := runCommand(ctx, namespace(v.netns, "ip", "addr", "del", v.network.NamespacedIPWithCIDR(), "dev", v.namespacedDevice)...); err != nil {
		log.CtxErrorf(ctx, "Failed to delete IP address %s from %s: %s", v.network.NamespacedIPWithCIDR(), v.namespacedDevice, err)
		lastErr = err
	}
	if lastErr == nil {
		v.network.Unlock()
		v.network = nil
	}
	return lastErr
}

// List of cleanup tasks which should be executed in the reverse order in which
// the corresponding resources were created.
type cleanupStack []func(ctx context.Context) error

func (s cleanupStack) Cleanup(ctx context.Context) error {
	ctx, cancel := background.ExtendContextForFinalization(ctx, networkingCleanupTimeout)
	defer cancel()
	// Pop and run cleanup funcs from the stack until empty.
	for len(s) > 0 {
		f := s[len(s)-1]
		s = s[:len(s)-1]
		if err := f(ctx); err != nil {
			// Short-circuit on the first error.
			alert.UnexpectedEvent("network_cleanup_failed", "Networking cleanup failed. If too many of these errors accumulate, networking may stop functioning correctly. Error: %s", err)
			return err
		}
	}
	return nil
}

// VMNetwork represents a fully-provisioned VM network, which consists of a net
// namespace, virtual network interfaces, and associated host configuration.
//
// Deleting a VM network deletes the net namespace as well as all associated
// resources, and reverts the applied host configuration.
type VMNetwork struct {
	netns    *Namespace
	vethPair *vethPair
	cleanup  func(ctx context.Context) error
}

// CreateVMNetwork initializes a network namespace, networking
// interfaces, and host configuration required for VM networking.
func CreateVMNetwork(ctx context.Context, tapDeviceName, tapAddr, vmIP string) (_ *VMNetwork, err error) {
	var cleanupStack cleanupStack
	defer func() {
		// If we failed to fully set up the network, make sure to clean up any
		// resources that were partially set up.
		if err != nil {
			_ = cleanupStack.Cleanup(ctx)
		}
	}()

	// Create a net namespace.
	netns, err := createUniqueNetNamespace(ctx)
	if err != nil {
		return nil, status.WrapError(err, "create net namespace")
	}
	cleanupStack = append(cleanupStack, func(ctx context.Context) error {
		return netns.Delete(ctx)
	})

	// Create a veth pair with one end in the namespace.
	vethPair, err := setupVethPair(ctx, netns)
	if err != nil {
		return nil, status.WrapError(err, "setup veth pair")
	}
	cleanupStack = append(cleanupStack, vethPair.Cleanup)

	// Create a TAP device in the namespace, attach the IP (with CIDR) to it,
	// and bring it up.
	//
	// Also, configure NAT in the namespace so that outgoing IP packets from the
	// tap device in the namespace are translated to the namespaced veth device
	// IP, and incoming IP packets to the tap device are translated to the VM
	// tap device IP. Since the host-side of the veth pair also has NAT
	// configured, this allows the VM to communicate with external networks.
	//
	// See this documentation for more info on why we use this setup:
	// https://github.com/firecracker-microvm/firecracker/blob/2914d5ad00d2fdfe3ecdb95b8ffa05975935f32d/docs/snapshotting/network-for-clones.md#network-namespaces
	for _, command := range [][]string{
		{"ip", "tuntap", "add", "name", tapDeviceName, "mode", "tap"},
		{"ip", "addr", "add", tapAddr, "dev", tapDeviceName},
		{"ip", "link", "set", tapDeviceName, "up"},
		{"iptables", "--wait", "-t", "nat", "-A", "POSTROUTING", "-o", vethPair.namespacedDevice, "-s", vmIP, "-j", "SNAT", "--to", vethPair.network.NamespacedIP()},
		{"iptables", "--wait", "-t", "nat", "-A", "PREROUTING", "-i", vethPair.namespacedDevice, "-d", vethPair.network.NamespacedIP(), "-j", "DNAT", "--to", vmIP},
	} {
		if err := runCommand(ctx, namespace(netns, command...)...); err != nil {
			return nil, status.WrapError(err, "set up tap device")
		}
	}

	return &VMNetwork{
		netns:    netns,
		vethPair: vethPair,
		cleanup:  cleanupStack.Cleanup,
	}, nil
}

func (v *VMNetwork) NamespacePath() string {
	return v.netns.Path()
}

func (v *VMNetwork) Cleanup(ctx context.Context) error {
	return v.cleanup(ctx)
}

// ContainerNetwork represents a fully-provisioned container network, which
// consists of a net namespace, virtual network interfaces, and associated host
// configuration.
//
// Deleting a container network deletes the net namespace as well as all
// associated resources, and reverts the applied host configuration.
type ContainerNetwork struct {
	netns    *Namespace
	vethPair *vethPair
	cleanup  func(ctx context.Context) error
}

// CreateContainerNetwork initializes a network namespace, networking
// interfaces, and host configuration required for container networking.
//
// If loopbackOnly is true, only a loopback interface will be created in the
// namespace, and the container will not be able to reach external addresses.
func CreateContainerNetwork(ctx context.Context, loopbackOnly bool) (_ *ContainerNetwork, err error) {
	var cleanupStack cleanupStack
	defer func() {
		// If we failed to fully set up the network, make sure to clean up any
		// resources that were partially set up.
		if err != nil {
			_ = cleanupStack.Cleanup(ctx)
		}
	}()

	// Create a net namespace.
	netns, err := createUniqueNetNamespace(ctx)
	if err != nil {
		return nil, status.WrapError(err, "create net namespace")
	}
	cleanupStack = append(cleanupStack, func(ctx context.Context) error {
		return netns.Delete(ctx)
	})

	// Bring up the loopback device in the namespace.
	if err := runCommand(ctx, namespace(netns, "ip", "link", "set", "lo", "up")...); err != nil {
		return nil, status.WrapError(err, "bring up loopback device")
	}

	var vethPair *vethPair
	if !loopbackOnly {
		// Create a veth pair with one end in the namespace.
		vp, err := setupVethPair(ctx, netns)
		if err != nil {
			return nil, status.WrapError(err, "setup veth pair")
		}
		cleanupStack = append(cleanupStack, vp.Cleanup)
		vethPair = vp
	}

	return &ContainerNetwork{
		netns:    netns,
		vethPair: vethPair,
		cleanup:  cleanupStack.Cleanup,
	}, nil
}

func (c *ContainerNetwork) NamespacePath() string {
	return c.netns.Path()
}

// HostNetwork returns the externally-connected network routed through the host.
// Returns nil if this is a loopback-only network.
func (c *ContainerNetwork) HostNetwork() *HostNet {
	if c.vethPair == nil {
		return nil
	}
	return c.vethPair.network
}

func (c *ContainerNetwork) Cleanup(ctx context.Context) error {
	return c.cleanup(ctx)
}

// DefaultIP returns the IPv4 address for the primary network.
func DefaultIP(ctx context.Context) (net.IP, error) {
	r, err := findRoute(defaultRoute)
	if err != nil {
		return nil, err
	}
	device := r.device
	return ipFromDevice(ctx, device)
}

// ipFromDevice return IPv4 address for the device.
func ipFromDevice(ctx context.Context, device string) (net.IP, error) {
	netIface, err := interfaceFromDevice(ctx, device)
	if err != nil {
		return nil, err
	}
	addrs, err := netIface.Addrs()
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		if v, ok := a.(*net.IPNet); ok {
			if ipv4Addr := v.IP.To4(); ipv4Addr != nil {
				return ipv4Addr, nil
			}
		}
	}
	return nil, fmt.Errorf("could not determine IP for device %q", device)
}

func interfaceFromDevice(ctx context.Context, device string) (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Name == device {
			return &iface, nil
		}
	}
	return nil, status.NotFoundErrorf("could not find interface %q", device)
}

type route struct {
	device  string
	gateway net.IP
	// Source IP for the route. May be nil.
	src net.IP
}

// findRoute finds the highest priority route with the given destination.
// Destination may be defaultRoute to find a default route.
func findRoute(destination string) (route, error) {
	// Get all routes.
	rs, err := netlink.RouteList(nil, 0)
	if err != nil {
		return route{}, status.UnknownErrorf("could not get ip routes: %s", err)
	}

	var targetDst *net.IPNet
	if destination != defaultRoute {
		_, targetDst, err = net.ParseCIDR(destination)
		if err != nil {
			return route{}, status.InvalidArgumentErrorf("could not parse destination: %s", err)
		}
	}

	for _, r := range rs {
		if targetDst.String() == r.Dst.String() {
			l, err := netlink.LinkByIndex(r.LinkIndex)
			if err != nil {
				return route{}, status.UnknownErrorf("could not lookup interface for route: %s", err)
			}
			return route{
				device:  l.Attrs().Name,
				gateway: r.Gw,
				src:     r.Src,
			}, nil
		}
	}

	return route{}, status.FailedPreconditionErrorf("Unable to determine device with prefix: %s", destination)
}

// EnableMasquerading turns on ipmasq for the device with --device_prefix. This is required
// for networking to work on vms.
func EnableMasquerading(ctx context.Context) error {
	route, err := findRoute(*routePrefix)
	if err != nil {
		return err
	}
	device := route.device

	for _, protocol := range []string{"tcp", "udp", ""} {
		args := []string{"POSTROUTING", "-o", device, "-j", "MASQUERADE"}
		if protocol != "" {
			args = append(args, "-p", protocol)
			if *natSourcePortRange != "" {
				args = append(args, "--to-ports", *natSourcePortRange)
			}
		}
		// Skip appending the rule if it's already in the table.
		if err = runCommand(ctx, slices.Concat([]string{"iptables", "--wait", "-t", "nat", "--check"}, args)...); err == nil {
			continue
		}
		if err := runCommand(ctx, slices.Concat([]string{"iptables", "--wait", "-t", "nat", "-A"}, args)...); err != nil {
			return err
		}
	}
	return nil
}

// AddRoutingTableEntryIfNotPresent adds [tableID, tableName] name pair to /etc/iproute2/rt_tables if
// the pair is not present.
// Equilvalent to 'echo "1 rt1" | sudo tee -a /etc/iproute2/rt_tables'.
func addRoutingTableEntryIfNotPresent(ctx context.Context) error {
	tableEntry := fmt.Sprintf("%d %s", routingTableID, routingTableName)
	exists, err := routingTableContainsTable(tableEntry)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	f, err := os.OpenFile(routingTableFilename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return status.InternalErrorf("failed to add routing table %s: %s", routingTableName, err)
	}
	defer f.Close()
	if _, err = f.WriteString(tableEntry + "\n"); err != nil {
		return status.InternalErrorf("failed to add routing table %s: %s", routingTableName, err)
	}
	return nil
}

// routingTableContainTable checks if /etc/iproute2/rt_tables contains <tableEntry>.
func routingTableContainsTable(tableEntry string) (bool, error) {
	file, err := os.Open(routingTableFilename)
	if err != nil {
		return false, status.InternalErrorf("failed to open routing table: %s", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == tableEntry {
			return true, nil
		}
	}
	return false, nil
}

func ConfigurePrivateRangeBlackholing(ctx context.Context, sourceRange string) error {
	for _, r := range PrivateIPRanges {
		if err := runCommand(ctx, "iptables", "--wait", "-I", "FORWARD", "-s", sourceRange, "-d", r, "-j", "REJECT"); err != nil {
			return err
		}
	}
	return nil
}

// ConfigureRoutingForIsolation sets up a routing table for handling network
// isolation via either a secondary network interface or blackholing.
func ConfigureRoutingForIsolation(ctx context.Context) error {
	if !IsSecondaryNetworkEnabled() && !IsPrivateRangeBlackholingEnabled() {
		// No need to add IP rule when we don't use secondary network
		return nil
	}

	if IsSecondaryNetworkEnabled() {
		// Adds a new routing table
		if err := addRoutingTableEntryIfNotPresent(ctx); err != nil {
			return err
		}
		return configurePolicyBasedRoutingForSecondaryNetwork(ctx)
	} else {
		if err := ConfigurePrivateRangeBlackholing(ctx, containerNetworkingCIDR); err != nil {
			return err
		}
	}

	return nil
}

// configurePolicyBasedRoutingForNetworkWIthRoutePrefix configures policy routing for secondary
// network interface. The secondary interface is identified by the --route_prefix.
// This function:
//   - adds a new routing table tableName.
//     Equivalent to: " echo '1 rt1' | sudo tee -a /etc/iproute2/rt_tables'
//   - adds two ip rules.
//     Equivalent to: 'ip rule add from 172.24.0.24 table rt1' and
//     'ip rule add to 172.24.0.24 table rt1' where 172.24.0.24 is the internal ip for the
//     secondary network interface.
//   - adds routes to table rt1.
//     Equivalent to: 'ip route add 172.24.0.1 src 172.24.0.24 dev ens5 table rt1' and
//     'ip route add default via 172.24.0.1 dev ens5 table rt1' where 172.24.0.1 and ens5 are
//     the gateway and interface name of the secondary network interface.
func configurePolicyBasedRoutingForSecondaryNetwork(ctx context.Context) error {
	if !IsSecondaryNetworkEnabled() {
		// No need to add IP rule when we don't use secondary network
		return nil
	}

	route, err := findRoute(*routePrefix)
	if err != nil {
		return err
	}

	// Adds two ip rules
	ip, err := ipFromDevice(ctx, route.device)
	if err != nil {
		return err
	}
	ipStr := ip.String()

	if err := AddIPRuleIfNotPresent(ctx, []string{"to", ipStr}); err != nil {
		return err
	}
	if err := AddIPRuleIfNotPresent(ctx, []string{"from", ipStr}); err != nil {
		return err
	}

	// Adds routes to routing table.
	ipRouteArgs := []string{route.gateway.String(), "dev", route.device, "scope", "link", "src", ipStr}
	if err := AddRouteIfNotPresent(ctx, ipRouteArgs); err != nil {
		return err
	}
	ipRouteArgs = []string{"default", "via", route.gateway.String(), "dev", route.device}
	if err := AddRouteIfNotPresent(ctx, ipRouteArgs); err != nil {
		return err
	}

	return nil
}

func appendRoutingTable(args []string) []string {
	return append(args, "table", routingTableName)
}

// AddIPRuleIfNotPresent adds a ip rule to look up routingTableName if this rule is not present.
func AddIPRuleIfNotPresent(ctx context.Context, ruleArgs []string) error {
	listArgs := append([]string{"ip", "rule", "list"}, ruleArgs...)
	listArgs = appendRoutingTable(listArgs)
	out, err := sudoCommand(ctx, listArgs...)
	if err != nil {
		return err
	}

	if len(out) == 0 {
		addArgs := append([]string{"ip", "rule", "add"}, ruleArgs...)
		addArgs = appendRoutingTable(addArgs)
		if err := runCommand(ctx, addArgs...); err != nil {
			return err
		}
	}
	log.Debugf("ip rule %v already exists", ruleArgs)
	return nil
}

// AddRouteIfNotPresent adds a route in the routing table with routingTableName if the route is not
// present.
func AddRouteIfNotPresent(ctx context.Context, routeArgs []string) error {
	listArgs := routeArgs
	if len(listArgs) > 0 && listArgs[0] == "blackhole" {
		listArgs = listArgs[1:]
	}
	listArgs = append([]string{"ip", "route", "list"}, listArgs...)
	listArgs = appendRoutingTable(listArgs)
	out, err := sudoCommand(ctx, listArgs...)
	addRoute := false

	if err == nil {
		addRoute = len(out) == 0
	} else {
		if strings.Contains(err.Error(), "ipv4: FIB table does not exist") {
			// if no routes has been added to rt1, "ip route list table rt1" will return an error.
			addRoute = true
		} else {
			return err
		}
	}

	if addRoute {
		addArgs := append([]string{"ip", "route", "add"}, routeArgs...)
		addArgs = appendRoutingTable(addArgs)
		if err := runCommand(ctx, addArgs...); err != nil {
			return err
		}
	}
	log.Debugf("ip route %v already exists", routeArgs)
	return nil
}

func IsSecondaryNetworkEnabled() bool {
	return *routePrefix != "default"
}

func IsPrivateRangeBlackholingEnabled() bool {
	return *blackholePrivateRanges
}

func PreserveExistingNetNamespaces() bool {
	return *preserveExistingNetNamespaces
}
