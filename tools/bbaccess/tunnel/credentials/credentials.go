// Package credentials turns the short-lived certificates bbaccess issues into
// per-registration gateway credentials.
//
// A certificate is read from disk each time it is used rather than cached, so
// re-running bbaccess refreshes a running daemon's credential with no restart and
// no IPC: rewriting the file is the whole mechanism.
package credentials

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/relayauth"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"

	"google.golang.org/grpc/metadata"
)

const (
	certSuffix = "-cert.pem"
	keySuffix  = "-key.pem"
)

// DefaultDir is where bbaccess writes tunnel certificates.
//
// This follows the invoking user under sudo, which macOS depends on: the daemon
// has to run as root there, but the credentials belong to the human.
func DefaultDir() (string, error) {
	dir, err := config.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "buildbuddy", "tunnel"), nil
}

// Store holds the tunnel certificates on disk.
type Store struct {
	dir string
}

// NewStore opens the credential directory. An empty dir uses DefaultDir.
func NewStore(dir string) (*Store, error) {
	if dir == "" {
		d, err := DefaultDir()
		if err != nil {
			return nil, fmt.Errorf("locating the credential directory: %w", err)
		}
		dir = d
	}
	return &Store{dir: dir}, nil
}

// Dir returns the directory backing this store.
func (s *Store) Dir() string { return s.dir }

func (s *Store) paths(name string) (certPath, keyPath string) {
	return filepath.Join(s.dir, name+certSuffix), filepath.Join(s.dir, name+keySuffix)
}

// Write stores a complete credential under name, replacing any previous one.
// bbaccess itself uses EnsureKey and WriteCert, since it never holds a key it
// did not generate; this is for tests and for importing a credential by hand.
func (s *Store) Write(name string, certPEM, keyPEM []byte) error {
	if name == "" {
		return fmt.Errorf("credential name is required")
	}
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return fmt.Errorf("creating %s: %w", s.dir, err)
	}
	certPath, keyPath := s.paths(name)
	// The certificate is public, but the key is not.
	if err := writeFileAtomic(keyPath, keyPEM, 0600); err != nil {
		return err
	}
	return writeFileAtomic(certPath, certPEM, 0644)
}

// EnsureKey returns the PEM-encoded public half of the keypair stored under
// name, generating the keypair on first use. The private key is created here
// and never leaves this directory: bbaccess sends only the public key to be
// certified.
func (s *Store) EnsureKey(name string) ([]byte, error) {
	if name == "" {
		return nil, fmt.Errorf("credential name is required")
	}
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return nil, fmt.Errorf("creating %s: %w", s.dir, err)
	}
	_, keyPath := s.paths(name)

	keyPEM, err := os.ReadFile(keyPath)
	switch {
	case err == nil:
		if key, err := parsePrivateKeyPEM(keyPEM); err == nil {
			return publicKeyPEM(key.Public())
		}
		// A key we cannot use is a key we will never be able to sign with.
		// Replace it; the next certificate will be issued for the new one.
		log.Warningf("Replacing unreadable tunnel key %s", keyPath)
	case !os.IsNotExist(err):
		return nil, fmt.Errorf("reading %s: %w", keyPath, err)
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating tunnel key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("encoding tunnel key: %w", err)
	}
	if err := writeFileAtomic(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0600); err != nil {
		return nil, err
	}
	return publicKeyPEM(&priv.PublicKey)
}

// WriteCert stores a certificate issued for the key EnsureKey created under
// name. A certificate for some other key is refused: storing it would break
// the credential until the next bbaccess run, for a reason that is hard to see
// from the daemon's side.
func (s *Store) WriteCert(name string, certPEM []byte) error {
	certPath, keyPath := s.paths(name)
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", keyPath, err)
	}
	if _, err := relayauth.NewSigner(certPEM, keyPEM); err != nil {
		return fmt.Errorf("certificate does not belong with the key in %s: %w", keyPath, err)
	}
	return writeFileAtomic(certPath, certPEM, 0644)
}

func parsePrivateKeyPEM(keyPEM []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("not PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("key of type %T cannot sign", key)
	}
	return signer, nil
}

func publicKeyPEM(pub crypto.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("encoding public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

// writeFileAtomic writes data to path via a temporary file and rename, so a
// daemon that reloads the credential mid-write sees either the old file or
// the new one, never a partial one.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir, base := filepath.Split(path)
	tmp, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // a no-op once renamed
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

// WriteGateways stores the relay gateways the server that issued credential
// name sent along with it, where the daemon looks for them.
func (s *Store) WriteGateways(name string, g config.ServerGateways) error {
	if name == "" {
		return fmt.Errorf("credential name is required")
	}
	b, err := g.Encode()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return fmt.Errorf("creating %s: %w", s.dir, err)
	}
	return writeFileAtomic(s.gatewaysPath(name), b, 0644)
}

// RemoveGateways forgets the gateways a server sent, for a server that no
// longer sends any.
func (s *Store) RemoveGateways(name string) error {
	if err := os.Remove(s.gatewaysPath(name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *Store) gatewaysPath(name string) string {
	return filepath.Join(s.dir, name+config.GatewaysSuffix)
}

// Names lists the credentials present, sorted.
func (s *Store) Names() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if name, ok := strings.CutSuffix(e.Name(), certSuffix); ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Load returns a signer for the named credential. An empty name resolves to the
// only credential present, which is the common case for someone who runs bbaccess
// against a single environment.
func (s *Store) Load(name string) (*relayauth.Signer, error) {
	if name == "" {
		names, err := s.Names()
		if err != nil {
			return nil, fmt.Errorf("listing credentials in %s: %w", s.dir, err)
		}
		switch len(names) {
		case 0:
			return nil, fmt.Errorf("no tunnel credential in %s; run bbaccess to get one", s.dir)
		case 1:
			name = names[0]
		default:
			return nil, fmt.Errorf("several tunnel credentials in %s (%s); name one with 'credential:' in the zone config",
				s.dir, strings.Join(names, ", "))
		}
	}

	certPath, keyPath := s.paths(name)
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", certPath, err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", keyPath, err)
	}
	signer, err := relayauth.NewSigner(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("loading credential %q: %w", name, err)
	}
	return signer, nil
}

// Attach mints a credential for wgPublicKey at zone's gateway and puts it in
// ctx's outgoing metadata.
func (s *Store) Attach(ctx context.Context, zone config.Zone, wgPublicKey string) (context.Context, error) {
	audience, err := Audience(zone)
	if err != nil {
		return nil, err
	}
	signer, err := s.Load(zone.Credential)
	if err != nil {
		return nil, err
	}
	cred, err := signer.Sign(audience, wgPublicKey, relayauth.DefaultAssertionLifetime)
	if err != nil {
		return nil, err
	}
	return metadata.AppendToOutgoingContext(ctx, relayauth.CredentialHeader, cred), nil
}

// Audience returns the gateway identity a credential for zone must name.
func Audience(zone config.Zone) (string, error) {
	if zone.Audience != "" {
		return zone.Audience, nil
	}
	host := GatewayHost(zone.Gateway)
	if host == "" {
		return "", fmt.Errorf("could not derive an audience from gateway %q; set 'audience:' in the zone config", zone.Gateway)
	}
	return host, nil
}

// GatewayHost extracts the hostname from a gRPC target such as
// "grpcs://gateway.example.com:443" or "gateway.example.com:1985".
func GatewayHost(target string) string {
	// Strip a scheme if present. gRPC targets also allow "dns:///host:port",
	// which this handles by way of the leading-slash trim below.
	if _, rest, ok := strings.Cut(target, "://"); ok {
		target = rest
	}
	target = strings.TrimLeft(target, "/")
	// Drop any path.
	if i := strings.IndexByte(target, '/'); i >= 0 {
		target = target[:i]
	}
	if target == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(target); err == nil {
		return host
	}
	// No port, or an unbracketed IPv6 literal; either way there is nothing
	// further to strip.
	return strings.Trim(target, "[]")
}

// APIKey authenticates with a BuildBuddy API key instead of a certificate, for
// gateways with no tunnel CA configured.
type APIKey string

// Attach implements the same interface as Store.
func (k APIKey) Attach(ctx context.Context, zone config.Zone, wgPublicKey string) (context.Context, error) {
	if k == "" {
		return ctx, nil
	}
	return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", string(k)), nil
}
