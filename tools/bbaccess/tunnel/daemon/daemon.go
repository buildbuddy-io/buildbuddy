// Package daemon runs the workstation side of the tunnel: local DNS, packet
// interception, and on-demand tunnels to gateways.
package daemon

import (
	"fmt"
	"net/netip"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/dnsserver"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/fakeip"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/interceptor"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tundev"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelmgr"
)

// reapInterval is how often idle tunnels are checked for teardown.
const reapInterval = 30 * time.Second

// refreshInterval is how often the gateway files bbaccess writes are checked
// for changes, so a morning bbaccess run adds a new cluster to a running
// daemon without a restart.
const refreshInterval = 5 * time.Second

type Daemon struct {
	cfg   *config.Config
	table *fakeip.Table
	mgr   *tunnelmgr.Manager
	dns   *dnsserver.Server
	intr  *interceptor.Interceptor
}

// Run starts the daemon and blocks until it is interrupted.
//
// creds authenticates with gateways. Employees pass a credential store backed
// by the certificates bbaccess writes; it reloads them per registration, so
// re-running bbaccess refreshes a running daemon.
func Run(cfg *config.Config, creds tunnelmgr.Credentials) error {
	prefix, err := netip.ParsePrefix(cfg.FakeCIDR)
	if err != nil {
		return fmt.Errorf("parsing fake_cidr %q: %w", cfg.FakeCIDR, err)
	}
	table, err := fakeip.NewTable(prefix)
	if err != nil {
		return err
	}

	idle, err := time.ParseDuration(cfg.IdleTimeout)
	if err != nil {
		return fmt.Errorf("parsing idle_timeout %q: %w", cfg.IdleTimeout, err)
	}

	d := &Daemon{
		cfg:   cfg,
		table: table,
		mgr:   tunnelmgr.New(creds, idle),
	}
	defer d.close()

	d.dns = dnsserver.New(cfg, table, d.mgr)
	if err := d.dns.Start(cfg.DNSListen); err != nil {
		return err
	}
	log.Printf("DNS server listening on %s", cfg.DNSListen)

	dev, err := tundev.Open(cfg.TUNName)
	if err != nil {
		return err
	}
	if err := tundev.EnsureConfigured(dev, cfg.FakeCIDR); err != nil {
		dev.Close()
		return err
	}
	name, _ := dev.Name()
	d.intr, err = interceptor.New(dev, cfg, table, d.mgr)
	if err != nil {
		dev.Close()
		return err
	}
	d.intr.Start()
	log.Printf("Intercepting %s on %s", cfg.FakeCIDR, name)

	logZones(cfg)
	log.Printf("Ready. Tunnels are established on first use and torn down after %s idle.", idle)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	reap := time.NewTicker(reapInterval)
	defer reap.Stop()
	refresh := time.NewTicker(refreshInterval)
	defer refresh.Stop()
	for {
		select {
		case <-reap.C:
			d.mgr.ReapIdle()
		case <-refresh.C:
			changed, err := cfg.Refresh()
			if err != nil {
				log.Warningf("Keeping the current zones: %s", err)
			} else if changed {
				log.Printf("Zones changed:")
				logZones(cfg)
			}
		case <-stop:
			log.Printf("Shutting down...")
			return nil
		}
	}
}

func logZones(cfg *config.Config) {
	log.Printf("Covered zones:")
	for _, z := range cfg.Zones {
		via := z.Gateway
		if z.RewriteTo != "" {
			via = fmt.Sprintf("%s (sent as *.%s)", z.Gateway, z.RewriteTo)
		}
		log.Printf("  *.%s → %s", z.Suffix, via)
	}
}

func (d *Daemon) close() {
	if d.intr != nil {
		d.intr.Close()
	}
	if d.dns != nil {
		d.dns.Shutdown()
	}
	if d.mgr != nil {
		d.mgr.Close()
	}
}

// PrintStatus prints what the daemon would do for a set of names, without
// needing a running daemon: which zone matches, which gateway serves it, and
// what name the gateway would be asked to dial.
func PrintStatus(cfg *config.Config, names []string) {
	fmt.Printf("Config:\n")
	fmt.Printf("  DNS listener:  %s\n", cfg.DNSListen)
	fmt.Printf("  Fake range:    %s\n", cfg.FakeCIDR)
	fmt.Printf("  TUN interface: %s\n", cfg.TUNName)
	fmt.Printf("  Idle timeout:  %s\n", cfg.IdleTimeout)
	fmt.Printf("\nZones:\n")
	if len(cfg.Zones) == 0 {
		fmt.Printf("  none — run bbaccess to fetch the relay gateways from the certificate server\n")
	}
	zones := make([]config.Zone, len(cfg.Zones))
	copy(zones, cfg.Zones)
	sort.Slice(zones, func(i, j int) bool { return zones[i].Suffix < zones[j].Suffix })
	for _, z := range zones {
		fmt.Printf("  *.%-40s → %s  (from %s)\n", z.Suffix, z.Gateway, z.Source)
		if z.RewriteTo != "" {
			fmt.Printf("  %-42s   (requested from the gateway as *.%s)\n", "", z.RewriteTo)
		}
	}
	if len(names) == 0 {
		return
	}
	fmt.Printf("\nName resolution:\n")
	for _, n := range names {
		zone, ok := cfg.MatchZone(n)
		switch {
		case ok:
			fmt.Printf("  %s: zone *.%s → %s, dialed as %s\n", n, zone.Suffix, zone.Gateway, zone.TargetName(n))
		case config.UnderParent(n):
			fmt.Printf("  %s: under %s but not in any zone (would not resolve)\n", n, config.Parent)
		default:
			fmt.Printf("  %s: not under %s (left to the system resolver)\n", n, config.Parent)
		}
	}
}

// ResolverDomains returns the DNS suffixes that must be routed to the local
// resolver: the parent of every zone, and the reverse zones for the fake
// range. Neither depends on the zone list, which is what lets the zones change
// after the split-DNS configuration is installed.
func ResolverDomains(cfg *config.Config) []string {
	out := []string{config.Parent}
	if prefix, err := netip.ParsePrefix(cfg.FakeCIDR); err == nil {
		out = append(out, dnsserver.ReverseZones(prefix)...)
	}
	return out
}
