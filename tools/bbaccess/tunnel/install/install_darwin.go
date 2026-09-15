// Package install performs the one-time privileged setup the tunnel needs.
//
// On macOS that is only the resolver configuration. The TUN device cannot be
// set up ahead of time: utun interfaces exist only while the descriptor that
// created them is open, and creating one requires root — so the daemon creates
// and configures its interface at startup and must itself run privileged
// (`sudo bbaccess tunnel run`).
package install

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
)

const resolverDir = "/etc/resolver"

// Install writes an /etc/resolver file for the zones' parent and for the
// reverse zone of the fake range.
func Install(cfg *config.Config, user string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel install")
	}
	host, port, err := splitHostPort(cfg.DNSListen)
	if err != nil {
		return fmt.Errorf("parsing dns_listen %q: %w", cfg.DNSListen, err)
	}
	if err := os.MkdirAll(resolverDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", resolverDir, err)
	}

	for _, domain := range daemon.ResolverDomains(cfg) {
		path := filepath.Join(resolverDir, domain)
		content := fmt.Sprintf("# Written by \"bbaccess tunnel install\". Remove with \"bbaccess tunnel uninstall\".\nnameserver %s\nport %s\n", host, port)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", path, err)
		}
		fmt.Printf("Configured %s → %s\n", domain, cfg.DNSListen)
	}

	fmt.Printf("\nInstalled. Start the daemon with:\n\n    sudo bbaccess tunnel run\n\n"+
		"(macOS requires root to create the %s interface; the Linux daemon does not.)\n", "utun")
	return nil
}

// Uninstall removes the resolver files Install wrote.
func Uninstall(cfg *config.Config) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("this must run as root: sudo bbaccess tunnel uninstall")
	}
	for _, domain := range daemon.ResolverDomains(cfg) {
		path := filepath.Join(resolverDir, domain)
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// Only remove files we wrote — someone may have their own resolver
		// entry for a domain that happens to overlap.
		if !strings.Contains(string(b), "bbaccess tunnel install") {
			fmt.Printf("Leaving %s alone: it was not written by bbaccess tunnel install\n", path)
			continue
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("removing %s: %w", path, err)
		}
		fmt.Printf("Removed %s\n", path)
	}
	return nil
}
