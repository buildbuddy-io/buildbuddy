// Package tunnel implements the `bbaccess tunnel` surface.
package tunnel

import (
	"fmt"
	"net"
	"os"
	"slices"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/credentials"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/install"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

const Usage = `usage: bbaccess tunnel <command>

Reach production infrastructure by its internal DNS names.

Commands:
  install     One-time privileged setup (TUN device and split DNS). Run with sudo.
  run         Run the daemon.
  status      Show the configuration, and how given names would be routed.
  uninstall   Undo install.
`

func Handle(subcommand string, args []string) error {
	if subcommand == "help" || slices.Contains(args, "-h") || slices.Contains(args, "--help") {
		fmt.Fprint(os.Stderr, Usage)
		return nil
	}

	cfg, path, err := LoadConfig()
	if err != nil {
		return err
	}
	if tunnelconfig.Exists(path) {
		log.Infof("Using tunnel config %s", path)
	}

	switch subcommand {
	case "install":
		return install.Install(cfg)
	case "uninstall":
		return install.Uninstall(cfg)
	case "status":
		daemon.PrintStatus(cfg, args)
		PrintCredentialStatus(cfg)
		return nil
	case "run":
		return RunDaemon(cfg)
	case "":
		fmt.Fprint(os.Stderr, Usage)
		return fmt.Errorf("no tunnel command given")
	default:
		return fmt.Errorf("unknown tunnel command %q", subcommand)
	}
}

// LoadConfig reads the daemon config from its fixed location and loads the
// zones the certificate servers sent. It returns the path it read, which
// callers report.
func LoadConfig() (*tunnelconfig.Config, string, error) {
	path, err := tunnelconfig.Path()
	if err != nil {
		return nil, "", fmt.Errorf("locating the config file: %w", err)
	}
	cfg, err := tunnelconfig.Load(path)
	if err != nil {
		return nil, path, err
	}
	useServerGateways(cfg)
	return cfg, path, nil
}

// useServerGateways loads cfg's zones from the gateways files in the
// credential directory.
func useServerGateways(cfg *tunnelconfig.Config) {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		log.Warningf("Not loading relay gateways: %s", err)
		return
	}
	if err := cfg.UseServerGateways(store.Dir()); err != nil {
		log.Warningf("Ignoring the relay gateways on disk until they change: %s", err)
	}
}

// RunDaemon starts the daemon in the foreground and blocks.
func RunDaemon(cfg *tunnelconfig.Config) error {
	// bbaccess may have written new gateway files since the config was loaded.
	if _, err := cfg.Refresh(); err != nil {
		return err
	}
	if len(cfg.Zones) == 0 {
		if err := cfg.LastError(); err != nil {
			return fmt.Errorf("no zones: the relay gateways on disk could not be loaded (%s); run bbaccess to rewrite them", err)
		}
		return fmt.Errorf("no zones: run bbaccess to fetch the relay gateways from the certificate server")
	}

	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	// Fail fast on a missing or unusable credential, but per zone: a gateways
	// file left behind by a server no longer in use must not keep every other
	// zone from working.
	usable := 0
	for _, zone := range cfg.Zones {
		if _, err := store.Load(zone.Credential); err != nil {
			log.Warningf("Zone *.%s is unusable until bbaccess is re-run: %s", zone.Suffix, err)
			continue
		}
		usable++
	}
	if usable == 0 {
		return fmt.Errorf("no zone has a usable credential; run bbaccess to get one")
	}
	// Last, since it may ask for a password.
	if err := install.EnsureInstalled(cfg); err != nil {
		return err
	}
	return daemon.Run(cfg, store)
}

// EnsureCredentialKey returns the PEM public key of the tunnel keypair stored
// under name, generating the keypair on first use.
func EnsureCredentialKey(cfg *tunnelconfig.Config, name string) ([]byte, error) {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return nil, err
	}
	return store.EnsureKey(name)
}

// StoreCredentialCert writes the certificate issued for the keypair stored
// under name, where the daemon will look for it.
func StoreCredentialCert(cfg *tunnelconfig.Config, name string, certPEM []byte) (string, error) {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return "", err
	}
	if err := store.WriteCert(name, certPEM); err != nil {
		return "", err
	}
	return store.Dir(), nil
}

// StoreGateways writes the relay gateways the server that issued credential
// name sent along with it, where the daemon will look for them. A running
// daemon picks them up on its own.
func StoreGateways(cfg *tunnelconfig.Config, name string, g tunnelconfig.ServerGateways) error {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	return store.WriteGateways(name, g)
}

// RemoveGateways forgets the relay gateways a server sent, for a server that
// no longer issues tunnel credentials.
func RemoveGateways(cfg *tunnelconfig.Config, name string) error {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	return store.RemoveGateways(name)
}

// PrintCredentialStatus reports which credentials are present, what identity
// they carry, when they expire, and whether a daemon appears to be running.
func PrintCredentialStatus(cfg *tunnelconfig.Config) {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		fmt.Printf("\nCredentials: %s\n", err)
		return
	}
	names, err := store.Names()
	if err != nil {
		fmt.Printf("\nCredentials: could not read %s: %s\n", store.Dir(), err)
		return
	}

	fmt.Printf("\nCredentials (%s):\n", store.Dir())
	if len(names) == 0 {
		fmt.Printf("  none — run bbaccess to get one\n")
	}
	for _, name := range names {
		signer, err := store.Load(name)
		if err != nil {
			fmt.Printf("  %s: unusable (%s)\n", name, err)
			continue
		}
		remaining := time.Until(signer.NotAfter())
		state := fmt.Sprintf("valid for %s", remaining.Round(time.Minute))
		if remaining <= 0 {
			state = "EXPIRED — re-run bbaccess"
		}
		fmt.Printf("  %s: %s, %s\n", name, signer.Email(), state)
	}

	fmt.Printf("\nDaemon:\n")
	if daemonRunning(cfg.DNSListen) {
		fmt.Printf("  running (DNS listener answering on %s)\n", cfg.DNSListen)
	} else {
		fmt.Printf("  not running — start it with: bbaccess tunnel run\n")
	}
}

// daemonRunning reports whether something is listening on the daemon's DNS
// address.
func daemonRunning(dnsListen string) bool {
	if dnsListen == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", dnsListen, 250*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
