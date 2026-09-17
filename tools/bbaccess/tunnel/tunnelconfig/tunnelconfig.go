// Package tunnelconfig describes which DNS zones the tunnel covers and which gateway
// serves each one.
//
// A zone is a DNS suffix the daemon claims: names under it are answered locally
// with a fake IP and relayed to that zone's gateway. Because the gateway
// resolves the target name with its own resolver, "which gateway" is also
// "which cluster's names resolve" — one gateway per cluster is the unit of
// reachability.
//
// Zones come from the certificate servers: each sends the relay gateways its
// credential is for, and bbaccess stores them beside that credential. Every
// zone lives under Parent, which is the one suffix the installed split-DNS
// configuration routes to the daemon, so the zone list can change without
// touching anything privileged.
package tunnelconfig

import (
	"fmt"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Parent is the DNS suffix every zone lives under, and the suffix `tunnel
// install` routes to the daemon.
const Parent = "bb.internal"

// GatewaysSuffix names the file, beside a credential in the credential
// directory, holding the relay gateways the issuing server sent.
const GatewaysSuffix = "-gateways.yaml"

// UnderParent reports whether name is Parent or lies beneath it.
func UnderParent(name string) bool {
	n := normalize(name)
	return n == Parent || strings.HasSuffix(n, "."+Parent)
}

// Zone maps a DNS suffix to the gateway that can reach names under it. Zones
// are never written by hand: they are flattened from the gateways files the
// certificate servers' responses are stored in.
type Zone struct {
	// Suffix is the DNS suffix this zone claims, e.g. "foo.bb.internal".
	Suffix string

	// Gateway is the gRPC target of the gateway serving this zone.
	Gateway string

	// RewriteTo, if set, replaces Suffix with this value before the name is
	// sent to the gateway. This is how cluster-qualified aliases work:
	//
	//	suffix:     svc.foo.bb.internal
	//	rewrite_to: svc.cluster.local
	//
	// makes "bar.ns.svc.foo.bb.internal" reach the gateway as
	// "bar.ns.svc.cluster.local". Unambiguous cross-cluster names on the
	// workstation, ordinary Kubernetes names on the wire.
	RewriteTo string

	// Credential names the tunnel certificate to authenticate with: the one
	// issued by the server that sent this zone.
	Credential string
}

// Config is the daemon's configuration.
type Config struct {
	// Zones is the merged zone list from the gateways files, kept current by
	// Refresh.
	Zones []Zone `yaml:"-"`

	// DNSListen is the address the local DNS server binds.
	DNSListen string `yaml:"dns_listen,omitempty"`

	// TUNName is the TUN interface to attach to (Linux; created by
	// `bbaccess tunnel install`). On macOS the daemon creates a utun at startup.
	TUNName string `yaml:"tun_name,omitempty"`

	// FakeCIDR is the range fake IPs are allocated from.
	FakeCIDR string `yaml:"fake_cidr,omitempty"`

	// IdleTimeout is how long a gateway tunnel stays up with no traffic before
	// the daemon deregisters. Zero disables idle teardown.
	IdleTimeout string `yaml:"idle_timeout,omitempty"`

	// CredentialDir is where bbaccess writes tunnel certificates. Empty means the
	// default location under the user config directory.
	CredentialDir string `yaml:"credential_dir,omitempty"`

	// server, once UseServerGateways has been called, tracks the gateway files
	// the zones are merged from.
	server *serverZones
}

const (
	DefaultDNSListen   = "127.0.0.1:5533"
	DefaultFakeCIDR    = "198.18.0.0/16"
	DefaultTUNName     = "bbtun0"
	DefaultIdleTimeout = "5m"
)

// Default returns the built-in configuration, used when no config file exists.
// It has no zones: those come from the certificate servers.
func Default() *Config {
	return &Config{
		DNSListen:   DefaultDNSListen,
		FakeCIDR:    DefaultFakeCIDR,
		TUNName:     DefaultTUNName,
		IdleTimeout: DefaultIdleTimeout,
	}
}

// Path returns the path of the daemon config file.
func Path() (string, error) {
	dir, err := UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "buildbuddy", "tunnel.yaml"), nil
}

// UserConfigDir returns the config directory of the person running the command,
// which under sudo is not root's.
func UserConfigDir() (string, error) {
	if home, ok := sudoUserHome(os.Geteuid(), os.Getenv("SUDO_USER"), lookupHome); ok {
		return configDirForHome(home), nil
	}
	return os.UserConfigDir()
}

func lookupHome(name string) (string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return "", err
	}
	return u.HomeDir, nil
}

// sudoUserHome returns the home directory of the user who ran sudo, if we are
// running as root on their behalf.
func sudoUserHome(euid int, sudoUser string, lookup func(string) (string, error)) (string, bool) {
	if euid != 0 || sudoUser == "" || sudoUser == "root" {
		return "", false
	}
	home, err := lookup(sudoUser)
	if err != nil || home == "" {
		return "", false
	}
	return home, true
}

// configDirForHome mirrors os.UserConfigDir for an explicit home directory.
// XDG_CONFIG_HOME is deliberately not consulted: sudo's env_reset usually drops
// it, and a stale value from root's environment would be worse than the
// convention.
func configDirForHome(home string) string {
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Application Support")
	}
	return filepath.Join(home, ".config")
}

// Exists reports whether a config file is present at path.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Load reads the config file at path, falling back to Default() if it does not
// exist. Unset fields are filled in with defaults.
func Load(path string) (*Config, error) {
	cfg := Default()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	parsed := &Config{}
	if err := yaml.Unmarshal(b, parsed); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if parsed.DNSListen != "" {
		cfg.DNSListen = parsed.DNSListen
	}
	if parsed.TUNName != "" {
		cfg.TUNName = parsed.TUNName
	}
	if parsed.FakeCIDR != "" {
		cfg.FakeCIDR = parsed.FakeCIDR
	}
	if parsed.IdleTimeout != "" {
		cfg.IdleTimeout = parsed.IdleTimeout
	}
	if parsed.CredentialDir != "" {
		cfg.CredentialDir = parsed.CredentialDir
	}
	return cfg, nil
}

// Validate checks the zones: each has a gateway, lies under Parent, and
// overlaps no other, so that every name belongs to exactly one gateway.
func (c *Config) Validate() error {
	seen := make(map[string]Zone, len(c.Zones))
	gateways := make(map[string]string, len(c.Zones)) // gateway target -> credential
	for _, z := range c.Zones {
		s := normalize(z.Suffix)
		if s == "" {
			return fmt.Errorf("zone from %s: suffix is required", z.Credential)
		}
		if !UnderParent(s) {
			return fmt.Errorf("zone %q from %s: suffix must be under %s, the suffix routed to the tunnel", s, z.Credential, Parent)
		}
		g := strings.ToLower(strings.TrimSpace(z.Gateway))
		if g == "" {
			return fmt.Errorf("zone %q from %s: gateway is required", s, z.Credential)
		}
		// A tunnel to a gateway is authenticated with one credential, so a
		// gateway belongs to one server.
		if other, ok := gateways[g]; ok && other != z.Credential {
			return fmt.Errorf("gateway %q is listed by both %s and %s", g, other, z.Credential)
		}
		gateways[g] = z.Credential
		for o, oz := range seen {
			if s == o || strings.HasSuffix(s, "."+o) || strings.HasSuffix(o, "."+s) {
				return fmt.Errorf("zone %q from %s overlaps zone %q from %s", s, z.Credential, o, oz.Credential)
			}
		}
		seen[s] = z
	}
	return nil
}

// ServerGateways is what a certificate server sends along with the credential
// it issues: the relay gateways that credential is for, and the zones each
// serves. bbaccess stores it beside the credential, as <name>-gateways.yaml,
// and the daemon merges every such file into its zone list.
type ServerGateways struct {
	Gateways []ServerGateway `yaml:"gateways"`
}

// ServerGateway is one relay gateway and the zones routed to it.
type ServerGateway struct {
	Target string       `yaml:"target"`
	Zones  []ServerZone `yaml:"zones"`
}

// ServerZone is a zone as a server describes it: the credential and gateway
// are implied by where it is stored and listed.
type ServerZone struct {
	Suffix    string `yaml:"suffix"`
	RewriteTo string `yaml:"rewrite_to,omitempty"`
}

// Encode renders the gateways file.
func (g ServerGateways) Encode() ([]byte, error) {
	return yaml.Marshal(g)
}

// zones flattens the file into zones authenticated with the named credential.
func (g ServerGateways) zones(credential string) []Zone {
	var out []Zone
	for _, gw := range g.Gateways {
		for _, z := range gw.Zones {
			out = append(out, Zone{
				Suffix:     z.Suffix,
				Gateway:    gw.Target,
				RewriteTo:  z.RewriteTo,
				Credential: credential,
			})
		}
	}
	return out
}

// serverZones tracks the gateway files in the credential directory, so the
// zone list can follow them while the daemon runs.
type serverZones struct {
	mu   sync.RWMutex // guards Config.Zones once Refresh may replace it
	dir  string
	seen map[string]time.Time // gateway file -> modification time last loaded
}

// UseServerGateways loads the gateway files bbaccess wrote into dir, and makes
// Refresh follow them. A file that cannot be loaded is reported, but the files
// are followed regardless: the next bbaccess run rewrites it, and the Refresh
// after that picks it up.
func (c *Config) UseServerGateways(dir string) error {
	c.server = &serverZones{dir: dir}
	_, err := c.Refresh()
	return err
}

// Refresh re-reads the gateway files if any changed since the last load, and
// reports whether the zone list changed. Files that do not parse, or that
// conflict, leave the current zones in place; the error is reported once, and
// the files are read again when one of them changes.
func (c *Config) Refresh() (changed bool, err error) {
	s := c.server
	if s == nil {
		return false, nil
	}
	files, err := filepath.Glob(filepath.Join(s.dir, "*"+GatewaysSuffix))
	if err != nil {
		return false, err
	}
	now := make(map[string]time.Time, len(files))
	for _, f := range files {
		st, err := os.Stat(f)
		if err != nil {
			if os.IsNotExist(err) {
				continue // removed since the listing
			}
			return false, err
		}
		now[f] = st.ModTime()
	}
	if s.seen != nil && maps.Equal(s.seen, now) {
		return false, nil
	}
	s.seen = now

	var zones []Zone
	for _, f := range slices.Sorted(maps.Keys(now)) {
		b, err := os.ReadFile(f)
		if err != nil {
			if os.IsNotExist(err) {
				continue // removed since the listing
			}
			return false, err
		}
		var g ServerGateways
		if err := yaml.Unmarshal(b, &g); err != nil {
			return false, fmt.Errorf("parsing %s: %w", f, err)
		}
		zones = append(zones, g.zones(strings.TrimSuffix(filepath.Base(f), GatewaysSuffix))...)
	}
	if err := (&Config{Zones: zones}).Validate(); err != nil {
		return false, err
	}
	// A rewritten file with the same content, which is what every bbaccess
	// run produces, is not a change.
	if slices.Equal(c.Zones, zones) {
		return false, nil
	}
	s.mu.Lock()
	c.Zones = zones
	s.mu.Unlock()
	return true, nil
}

// MatchZone returns the zone covering name, if any. Matching is label-aligned
// and case-insensitive: "foo.bb.internal" covers "host.foo.bb.internal" but
// not "evil-foo.bb.internal". Zones never overlap (Validate), so the most
// specific match here is the only match.
func (c *Config) MatchZone(name string) (Zone, bool) {
	if c.server != nil {
		c.server.mu.RLock()
		defer c.server.mu.RUnlock()
	}
	n := normalize(name)
	var best Zone
	found := false
	for _, z := range c.Zones {
		s := normalize(z.Suffix)
		if s == "" {
			continue
		}
		if n != s && !strings.HasSuffix(n, "."+s) {
			continue
		}
		if !found || len(s) > len(normalize(best.Suffix)) {
			best, found = z, true
		}
	}
	return best, found
}

// TargetName returns the name to request from the gateway for a name matched by
// this zone, applying RewriteTo if configured.
func (z Zone) TargetName(name string) string {
	n := normalize(name)
	s := normalize(z.Suffix)
	if z.RewriteTo == "" {
		return n
	}
	if n == s {
		return normalize(z.RewriteTo)
	}
	return strings.TrimSuffix(n, "."+s) + "." + normalize(z.RewriteTo)
}

func normalize(s string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(s), "."), "."))
}
