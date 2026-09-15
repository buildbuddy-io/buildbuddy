// Package tunnel implements `bbaccess tunnel`, which makes production
// infrastructure reachable from a developer workstation by its internal names.
//
// Once the daemon is running, ordinary tools work unmodified:
//
//	ssh sjc-prod-abc.prod.buildbuddy.io
//	grpcurl otel-collector.monitor-dev.svc.uswest1.buildbuddy.io:4317 list
//
// There is no proxy to configure. Names in covered zones resolve to local
// placeholder addresses, connections to those addresses are intercepted, and a
// WireGuard tunnel to the right gateway is established on demand — on the first
// query, not at login — and torn down when idle.
//
// It lives in bbaccess rather than the bb CLI because it authenticates with the
// same short-lived certificate bbaccess already issues, and because bb is the
// binary we hand to customers.
package tunnel

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/credentials"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/install"
)

var (
	flags = flag.NewFlagSet("tunnel", flag.ContinueOnError)

	configPath    = flags.String("config", "", "Path to the tunnel config file (default: <user config dir>/buildbuddy/tunnel.yaml)")
	credentialDir = flags.String("credential_dir", "", "Directory holding bbaccess-issued tunnel certificates (default: <user config dir>/buildbuddy/tunnel)")
	apiKey        = flags.String("api_key", "", "Authenticate with a BuildBuddy API key instead of a tunnel certificate. For gateways with no tunnel CA configured.")
	installUser   = flags.String("user", "", "User that should own the TUN device (install only; defaults to $SUDO_USER)")
)

// Usage returns help text for the tunnel subcommands.
func Usage() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, `usage: bbaccess tunnel <command> [flags]

Reach production infrastructure by its internal DNS names.

Commands:
  install     One-time privileged setup (TUN device and split DNS). Run with sudo.
  run         Run the daemon.
  status      Show the configuration, and how given names would be routed.
  uninstall   Undo install.

Examples:
  sudo bbaccess tunnel install
  bbaccess tunnel run
  bbaccess tunnel status sjc-prod-abc.prod.bb.internal

Flags:
`)
	flags.SetOutput(&buf)
	flags.PrintDefaults()
	return buf.String()
}

// Handle runs a tunnel subcommand. args are the arguments after the
// subcommand name.
func Handle(subcommand string, args []string) error {
	names, err := parseFlags(args)
	if err != nil {
		if err == flag.ErrHelp {
			fmt.Fprint(os.Stderr, Usage())
			return nil
		}
		return err
	}

	cfg, path, err := LoadConfig(*configPath)
	if err != nil {
		return err
	}
	if config.Exists(path) {
		log.Infof("Using tunnel config %s", path)
	}
	if *credentialDir != "" {
		cfg.CredentialDir = *credentialDir
		if err := useServerGateways(cfg); err != nil {
			return err
		}
	}

	switch subcommand {
	case "install":
		return install.Install(cfg, *installUser)
	case "uninstall":
		return install.Uninstall(cfg)
	case "status":
		daemon.PrintStatus(cfg, names)
		PrintCredentialStatus(cfg)
		return nil
	case "run":
		return RunDaemon(cfg, *apiKey)
	case "":
		fmt.Fprint(os.Stderr, Usage())
		return fmt.Errorf("no tunnel command given")
	default:
		return fmt.Errorf("unknown tunnel command %q", subcommand)
	}
}

// parseFlags parses args and returns the positional arguments among them.
//
// The flag package stops at the first non-flag argument, which would make
// `tunnel status somehost.prod.buildbuddy.io --config=x` silently ignore
// --config. Resuming after each positional argument accepts flags in any
// position, which is what anyone typing this expects.
func parseFlags(args []string) ([]string, error) {
	var positional []string
	for {
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		rest := flags.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

// LoadConfig reads the daemon config, falling back to the default location,
// and merges in the zones the certificate servers sent. It returns the path it
// read, which callers report.
func LoadConfig(path string) (*config.Config, string, error) {
	if path == "" {
		p, err := config.Path()
		if err != nil {
			return nil, "", fmt.Errorf("locating the config file: %w", err)
		}
		path = p
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, path, err
	}
	if err := useServerGateways(cfg); err != nil {
		return nil, path, err
	}
	return cfg, path, nil
}

// useServerGateways merges the gateway files in the credential directory into
// cfg's zones.
func useServerGateways(cfg *config.Config) error {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	return cfg.UseServerGateways(store.Dir())
}

// RunDaemon starts the daemon in the foreground and blocks.
//
// If overrideAPIKey (or the config's api_key) is set, gateways are
// authenticated with that API key; otherwise the bbaccess-issued certificate is
// used, which is the normal path for employees.
func RunDaemon(cfg *config.Config, overrideAPIKey string) error {
	// bbaccess may have written new gateway files since the config was loaded.
	if _, err := cfg.Refresh(); err != nil {
		return err
	}
	if len(cfg.Zones) == 0 {
		return fmt.Errorf("no zones: run bbaccess to fetch the relay gateways from the certificate server, or add zones to the config file")
	}

	key := overrideAPIKey
	if key == "" {
		key = cfg.APIKey
	}
	if key != "" {
		log.Warningf("Authenticating with an API key rather than a bbaccess certificate.")
		return daemon.Run(cfg, credentials.APIKey(key))
	}

	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	// Fail fast on a missing or unusable credential rather than at the first
	// connection attempt, minutes later, from inside some unrelated tool.
	for _, zone := range cfg.Zones {
		if _, err := store.Load(zone.Credential); err != nil {
			return fmt.Errorf("zone *.%s: %w", zone.Suffix, err)
		}
	}
	return daemon.Run(cfg, store)
}

// EnsureCredentialKey returns the PEM public key of the tunnel keypair stored
// under name, generating the keypair on first use. The private key never
// leaves this machine; bbaccess sends only the public half to be certified.
func EnsureCredentialKey(cfg *config.Config, name string) ([]byte, error) {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return nil, err
	}
	return store.EnsureKey(name)
}

// StoreCredentialCert writes the certificate issued for the keypair stored
// under name, where the daemon will look for it.
func StoreCredentialCert(cfg *config.Config, name string, certPEM []byte) (string, error) {
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
func StoreGateways(cfg *config.Config, name string, g config.ServerGateways) error {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	return store.WriteGateways(name, g)
}

// RemoveGateways forgets the relay gateways a server sent, for a server that
// no longer issues tunnel credentials.
func RemoveGateways(cfg *config.Config, name string) error {
	store, err := credentials.NewStore(cfg.CredentialDir)
	if err != nil {
		return err
	}
	return store.RemoveGateways(name)
}

// PrintCredentialStatus reports which credentials are present, what identity
// they carry, when they expire, and whether a daemon appears to be running.
func PrintCredentialStatus(cfg *config.Config) {
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
// address. The DNS server binds TCP as well as UDP, so a TCP connect is a
// cheap liveness probe that needs no pidfile.
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

// ResolverDomains returns the DNS suffixes the tunnel claims.
func ResolverDomains(cfg *config.Config) []string { return daemon.ResolverDomains(cfg) }
