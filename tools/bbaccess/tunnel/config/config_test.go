package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMatchZone(t *testing.T) {
	cfg := &Config{Zones: []Zone{
		{Suffix: "foo.bb.internal", Gateway: "a"},
		{Suffix: "bar.baz.bb.internal", Gateway: "b", RewriteTo: "cluster.local"},
	}}

	for _, tc := range []struct {
		name        string
		wantGateway string
		wantMatch   bool
	}{
		{"host.foo.bb.internal", "a", true},
		{"foo.bb.internal", "a", true},
		{"svc.ns.bar.baz.bb.internal", "b", true},
		{"HOST.Foo.BB.Internal", "a", true},
		// Matching is label-aligned, so a name that merely ends in the same
		// characters is not covered.
		{"evil-foo.bb.internal", "", false},
		{"qux.bb.internal", "", false},
		{"www.example.com", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zone, ok := cfg.MatchZone(tc.name)
			require.Equal(t, tc.wantMatch, ok)
			if tc.wantMatch {
				require.Equal(t, tc.wantGateway, zone.Gateway)
			}
		})
	}
}

func TestTargetName(t *testing.T) {
	for _, tc := range []struct {
		desc string
		zone Zone
		name string
		want string
	}{
		{
			desc: "no rewrite passes the name through",
			zone: Zone{Suffix: "foo.bb.internal"},
			name: "host.foo.bb.internal",
			want: "host.foo.bb.internal",
		},
		{
			desc: "cluster-qualified name becomes the cluster's own name",
			zone: Zone{Suffix: "bar.bb.internal", RewriteTo: "cluster.local"},
			name: "otel.monitoring.svc.bar.bb.internal",
			want: "otel.monitoring.svc.cluster.local",
		},
		{
			desc: "the zone apex itself rewrites",
			zone: Zone{Suffix: "host.foo.bb.internal", RewriteTo: "localhost"},
			name: "host.foo.bb.internal",
			want: "localhost",
		},
		{
			desc: "trailing dots and case are normalized",
			zone: Zone{Suffix: "bar.bb.internal", RewriteTo: "cluster.local"},
			name: "OTEL.Monitoring.SVC.Bar.BB.Internal.",
			want: "otel.monitoring.svc.cluster.local",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.want, tc.zone.TargetName(tc.name))
		})
	}
}

func TestValidate(t *testing.T) {
	ok := &Config{Zones: []Zone{
		{Suffix: "foo.bb.internal", Gateway: "a"},
		{Suffix: "notfoo.bb.internal", Gateway: "b"}, // shares characters, not labels
		{Suffix: "Bar.BB.Internal.", Gateway: "c"},
	}}
	require.NoError(t, ok.Validate())
	require.NoError(t, (&Config{}).Validate(), "no zones is valid: installing needs none")

	for _, tc := range []struct {
		name  string
		zones []Zone
		want  string
	}{
		{"outside the parent", []Zone{{Suffix: "foo.bar.example", Gateway: "a"}}, "must be under bb.internal"},
		{"the bare TLD", []Zone{{Suffix: "internal", Gateway: "a"}}, "must be under bb.internal"},
		{"a lookalike", []Zone{{Suffix: "notbb.internal", Gateway: "a"}}, "must be under bb.internal"},
		{"no suffix", []Zone{{Gateway: "a"}}, "suffix is required"},
		{"no gateway", []Zone{{Suffix: "foo.bb.internal"}}, "gateway is required"},
		{"repeated", []Zone{{Suffix: "foo.bb.internal", Gateway: "a"}, {Suffix: "FOO.bb.internal", Gateway: "b"}}, "overlaps zone"},
		{"a child of an earlier zone", []Zone{{Suffix: "bb.internal", Gateway: "a"}, {Suffix: "foo.bb.internal", Gateway: "b"}}, "overlaps zone"},
		{"a parent of an earlier zone", []Zone{{Suffix: "foo.bb.internal", Gateway: "a"}, {Suffix: "bb.internal", Gateway: "b"}}, "overlaps zone"},
		{"deeper nesting", []Zone{{Suffix: "foo.bb.internal", Gateway: "a"}, {Suffix: "a.b.foo.bb.internal", Gateway: "b"}}, "overlaps zone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (&Config{Zones: tc.zones}).Validate()
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLoadAppliesCredentialSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tunnel.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
credential_dir: /tmp/creds
zones:
  - suffix: foo.bb.internal
    gateway: grpcs://foo.bar.example
    credential: certs_example
    audience: foo.bar.example
`), 0644))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "/tmp/creds", cfg.CredentialDir)
	require.Len(t, cfg.Zones, 1)
	require.Equal(t, "certs_example", cfg.Zones[0].Credential)
	require.Equal(t, "foo.bar.example", cfg.Zones[0].Audience)
	require.Equal(t, "tunnel.yaml", cfg.Zones[0].Source)

	require.NoError(t, os.WriteFile(path, []byte("zones:\n  - suffix: foo.bar.example\n    gateway: grpcs://g\n"), 0644))
	_, err = Load(path)
	require.ErrorContains(t, err, "must be under bb.internal", "a config-file zone is held to the same rule as a server's")
}

// writeGateways stands in for bbaccess storing what a certificate server sent.
func writeGateways(t *testing.T, dir, name string, g ServerGateways) string {
	t.Helper()
	b, err := g.Encode()
	require.NoError(t, err)
	path := filepath.Join(dir, name+GatewaysSuffix)
	require.NoError(t, os.WriteFile(path, b, 0644))
	// Modification times are what Refresh watches; make sure consecutive
	// writes within the test never share one.
	require.NoError(t, os.Chtimes(path, time.Now(), time.Now().Add(time.Duration(len(b))*time.Second)))
	return path
}

func TestServerGateways(t *testing.T) {
	dir := t.TempDir()
	writeGateways(t, dir, "certs_example", ServerGateways{Gateways: []ServerGateway{
		{Target: "grpcs://foo.bar.example", Zones: []ServerZone{
			{Suffix: "foo.bb.internal", RewriteTo: "cluster.local"},
			{Suffix: "bar.bb.internal"},
		}},
	}})

	// Server zones are bound to the credential from the same server, and
	// merged with the config file's own zones.
	cfg := Default()
	cfg.Zones = []Zone{{Suffix: "local.bb.internal", Gateway: "grpc://127.0.0.1:1", Source: "tunnel.yaml"}}
	require.NoError(t, cfg.UseServerGateways(dir))
	require.Len(t, cfg.Zones, 3)
	foo, ok := cfg.MatchZone("host.foo.bb.internal")
	require.True(t, ok)
	require.Equal(t, "grpcs://foo.bar.example", foo.Gateway)
	require.Equal(t, "cluster.local", foo.RewriteTo)
	require.Equal(t, "certs_example", foo.Credential)
	require.Equal(t, "certs_example-gateways.yaml", foo.Source)
	local, ok := cfg.MatchZone("host.local.bb.internal")
	require.True(t, ok)
	require.Equal(t, "grpc://127.0.0.1:1", local.Gateway)

	changed, err := cfg.Refresh()
	require.NoError(t, err)
	require.False(t, changed, "nothing changed on disk")

	// A config-file zone with the same suffix replaces the server's, so a
	// gateway can be pointed elsewhere for testing.
	override := Default()
	override.Zones = []Zone{{Suffix: "foo.bb.internal", Gateway: "grpc://127.0.0.1:2", Source: "tunnel.yaml"}}
	require.NoError(t, override.UseServerGateways(dir))
	require.Len(t, override.Zones, 2)
	foo, _ = override.MatchZone("host.foo.bb.internal")
	require.Equal(t, "grpc://127.0.0.1:2", foo.Gateway)

	// Any other overlap with a server zone is an error.
	clash := Default()
	clash.Zones = []Zone{{Suffix: "x.foo.bb.internal", Gateway: "grpc://127.0.0.1:3", Source: "tunnel.yaml"}}
	require.ErrorContains(t, clash.UseServerGateways(dir), "overlaps zone")

	// A change on disk is picked up by the next Refresh.
	path := writeGateways(t, dir, "certs_example", ServerGateways{Gateways: []ServerGateway{
		{Target: "grpcs://foo.bar.example", Zones: []ServerZone{{Suffix: "foo.bb.internal"}, {Suffix: "bar.bb.internal"}, {Suffix: "qux.bb.internal"}}},
	}})
	changed, err = cfg.Refresh()
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, cfg.Zones, 4)

	// A second server's file binds its zones to its own credential.
	writeGateways(t, dir, "other_example", ServerGateways{Gateways: []ServerGateway{
		{Target: "grpcs://other.example", Zones: []ServerZone{{Suffix: "other.bb.internal"}}},
	}})
	changed, err = cfg.Refresh()
	require.NoError(t, err)
	require.True(t, changed)
	other, ok := cfg.MatchZone("host.other.bb.internal")
	require.True(t, ok)
	require.Equal(t, "other_example", other.Credential)

	// A file that no longer parses leaves the current zones in place, and is
	// not reported again until it changes.
	require.NoError(t, os.WriteFile(path, []byte("gateways: ["), 0644))
	require.NoError(t, os.Chtimes(path, time.Now(), time.Now().Add(time.Hour)))
	changed, err = cfg.Refresh()
	require.Error(t, err)
	require.False(t, changed)
	require.Len(t, cfg.Zones, 5)
	changed, err = cfg.Refresh()
	require.NoError(t, err)
	require.False(t, changed)

	// Removing a server's file drops its zones.
	require.NoError(t, os.Remove(path))
	changed, err = cfg.Refresh()
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, cfg.Zones, 2)
	_, ok = cfg.MatchZone("host.foo.bb.internal")
	require.False(t, ok)
}

// `tunnel install` needs root and the macOS daemon runs as root, but both must
// read the invoking user's config. Resolving to root's home instead silently
// configured the built-in default zones.
func TestSudoUserHome(t *testing.T) {
	homes := map[string]string{"vadim": "/home/vadim"}
	lookup := func(name string) (string, error) {
		home, ok := homes[name]
		if !ok {
			return "", os.ErrNotExist
		}
		return home, nil
	}

	for _, tc := range []struct {
		name      string
		euid      int
		sudoUser  string
		wantHome  string
		wantFound bool
	}{
		{name: "root via sudo uses the invoking user", euid: 0, sudoUser: "vadim", wantHome: "/home/vadim", wantFound: true},
		{name: "not root falls through", euid: 1000, sudoUser: "vadim", wantFound: false},
		{name: "root with no sudo falls through", euid: 0, sudoUser: "", wantFound: false},
		{name: "root sudo-ing from root falls through", euid: 0, sudoUser: "root", wantFound: false},
		{name: "unknown user falls through", euid: 0, sudoUser: "ghost", wantFound: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home, ok := sudoUserHome(tc.euid, tc.sudoUser, lookup)
			require.Equal(t, tc.wantFound, ok)
			require.Equal(t, tc.wantHome, home)
		})
	}
}

func TestConfigDirForHome(t *testing.T) {
	got := configDirForHome("/home/vadim")
	if runtime.GOOS == "darwin" {
		require.Equal(t, "/home/vadim/Library/Application Support", got)
	} else {
		require.Equal(t, "/home/vadim/.config", got)
	}
}
