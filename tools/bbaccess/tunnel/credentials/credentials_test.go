package credentials

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauthrelay"
	"github.com/buildbuddy-io/buildbuddy/server/util/relayauth"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestGatewayHost(t *testing.T) {
	for _, tc := range []struct {
		target string
		want   string
	}{
		{"grpcs://gateway.example", "gateway.example"},
		{"grpcs://Gateway.Example:443", "gateway.example"},
		{"grpcs://gateway.example:443", "gateway.example"},
		{"grpc://127.0.0.1:1985", "127.0.0.1"},
		{"gateway.example:1985", "gateway.example"},
		{"gateway.example", "gateway.example"},
		{"dns:///gateway.example:443", "gateway.example"},
		{"grpcs://[::1]:443", "::1"},
		{"", ""},
	} {
		t.Run(tc.target, func(t *testing.T) {
			assert.Equal(t, tc.want, GatewayHost(tc.target))
		})
	}
}

func TestAudience(t *testing.T) {
	// Derived from the gateway host, so the credential is bound to the host we
	// believe we are talking to.
	got, err := Audience(tunnelconfig.Zone{Gateway: "grpcs://gateway.foo.example:443"})
	require.NoError(t, err)
	assert.Equal(t, "gateway.foo.example", got)

	_, err = Audience(tunnelconfig.Zone{Gateway: ""})
	assert.Error(t, err)
}

// writeCredential puts a certificate and key where bbaccess would have, under
// the store's naming.
func writeCredential(t *testing.T, store *Store, name string, certPEM, keyPEM []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(store.Dir(), 0700))
	certPath, keyPath := store.paths(name)
	require.NoError(t, os.WriteFile(certPath, certPEM, 0644))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))
}

func TestStoreRoundTrip(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	certPEM, keyPEM := ca.IssuePEM(t, "someone@example.com", time.Time{})
	writeCredential(t, store, "certs_example", certPEM, keyPEM)

	names, err := store.Names()
	require.NoError(t, err)
	assert.Equal(t, []string{"certs_example"}, names)

	signer, err := store.Load("certs_example")
	require.NoError(t, err)
	assert.Equal(t, "someone@example.com", signer.Email())
}

func TestGatewaysRoundTrip(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, store.WriteGateways("certs_example", tunnelconfig.ServerGateways{Gateways: []tunnelconfig.ServerGateway{
		{Target: "grpcs://foo.bar.example", Zones: []tunnelconfig.ServerZone{{Suffix: "foo.bb.internal", RewriteTo: "cluster.local"}}},
	}}))

	// The daemon reads them back as zones bound to this credential.
	cfg := tunnelconfig.Default()
	require.NoError(t, cfg.UseServerGateways(store.Dir()))
	require.Len(t, cfg.Zones, 1)
	require.Equal(t, "certs_example", cfg.Zones[0].Credential)
	require.Equal(t, "grpcs://foo.bar.example", cfg.Zones[0].Gateway)
	require.Equal(t, "cluster.local", cfg.Zones[0].RewriteTo)

	names, err := store.Names()
	require.NoError(t, err)
	require.Empty(t, names, "a gateways file is not a credential")

	require.NoError(t, store.RemoveGateways("certs_example"))
	require.NoError(t, store.RemoveGateways("certs_example"), "removing twice is fine")
	changed, err := cfg.Refresh()
	require.NoError(t, err)
	require.True(t, changed)
	require.Empty(t, cfg.Zones)
}

func TestStoreKeyIsNotWorldReadable(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	require.NoError(t, err)
	_, err = store.EnsureKey("prod")
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "prod-key.pem"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm(), "the private key must not be readable by others")
}

func TestLoadRequiresAName(t *testing.T) {
	// Every zone names the credential of the server that sent it, so there is
	// never a reason to guess between dev and prod.
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	_, err = store.Load("")
	require.ErrorContains(t, err, "credential name is required")
}

func TestLoadWithNoCredentialPointsAtBbaccess(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "does-not-exist"))
	require.NoError(t, err)
	_, err = store.Load("certs_example")
	require.ErrorContains(t, err, "run bbaccess")
}

func TestAttachMintsACredentialForTheKey(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	certPEM, keyPEM := ca.IssuePEM(t, "someone@example.com", time.Time{})
	writeCredential(t, store, "prod", certPEM, keyPEM)

	const wgKey = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	zone := tunnelconfig.Zone{Gateway: "grpcs://gateway.example", Credential: "prod"}
	ctx, err := store.Attach(context.Background(), zone, wgKey)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	values := md.Get(relayauth.CredentialHeader)
	require.Len(t, values, 1)

	// The credential must verify against the CA, for this gateway, and name the
	// key it was minted for.
	v, err := relayauth.NewVerifier([]byte(ca.PEM()), "gateway.example")
	require.NoError(t, err)
	id, err := v.Verify(values[0])
	require.NoError(t, err)
	assert.Equal(t, "someone@example.com", id.Email)
	assert.Equal(t, wgKey, id.WireGuardPublicKey)
}

func TestAttachReloadsARefreshedCertificate(t *testing.T) {
	// Re-running bbaccess must take effect in a running daemon without a restart,
	// which is only true if the certificate is read at use time.
	ca := testauthrelay.NewCA(t)
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	stale, staleKey := ca.IssuePEM(t, "old@example.com", time.Time{})
	writeCredential(t, store, "prod", stale, staleKey)

	zone := tunnelconfig.Zone{Gateway: "grpcs://gateway.example", Credential: "prod"}
	const wgKey = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	v, err := relayauth.NewVerifier([]byte(ca.PEM()), "gateway.example")
	require.NoError(t, err)

	ctx, err := store.Attach(context.Background(), zone, wgKey)
	require.NoError(t, err)
	md, _ := metadata.FromOutgoingContext(ctx)
	id, err := v.Verify(md.Get(relayauth.CredentialHeader)[0])
	require.NoError(t, err)
	require.Equal(t, "old@example.com", id.Email)

	// Simulate a fresh bbaccess run overwriting the files.
	fresh, freshKey := ca.IssuePEM(t, "new@example.com", time.Time{})
	writeCredential(t, store, "prod", fresh, freshKey)

	ctx, err = store.Attach(context.Background(), zone, wgKey)
	require.NoError(t, err)
	md, _ = metadata.FromOutgoingContext(ctx)
	id, err = v.Verify(md.Get(relayauth.CredentialHeader)[0])
	require.NoError(t, err)
	assert.Equal(t, "new@example.com", id.Email)
}
