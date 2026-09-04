package update

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// publisher stands in for the publish workflow: it holds the signing key and
// serves a bucket.
type publisher struct {
	pub   ed25519.PublicKey
	priv  ed25519.PrivateKey
	files map[string][]byte
	srv   *httptest.Server
}

func newPublisher(t *testing.T) *publisher {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p := &publisher{pub: pub, priv: priv, files: map[string][]byte{}}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := p.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write(b)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// publish writes a binary for the test platform at the conventional path, a
// manifest naming it, and the manifest's signature, the way the workflow
// does.
func (p *publisher) publish(t *testing.T, commit string, binary []byte, latest bool) *Manifest {
	t.Helper()
	p.files["/"+commit+"/bbcert-test"] = binary
	digest := sha256.Sum256(binary)
	m := &Manifest{
		Commit:      commit,
		PublishedAt: "2026-09-04T00:00:00Z",
		SHA256:      map[string]string{"test": hex.EncodeToString(digest[:])},
	}
	body, err := json.Marshal(m)
	require.NoError(t, err)
	sig := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(p.priv, body)))
	p.files["/"+commit+"/manifest.json"] = body
	p.files["/"+commit+"/manifest.json.sig"] = sig
	if latest {
		p.files["/latest.json"] = body
		p.files["/latest.json.sig"] = sig
	}
	return m
}

func (p *publisher) updater(t *testing.T, exe string) *Updater {
	t.Helper()
	return &Updater{
		BaseURL:    p.srv.URL,
		PublicKey:  p.pub,
		Executable: exe,
		Platform:   "test",
		Client:     p.srv.Client(),
	}
}

func TestLatest_VerifiesAndParses(t *testing.T) {
	p := newPublisher(t)
	want := p.publish(t, "abc123", []byte("new binary"), true /*=latest*/)

	got, err := p.updater(t, "").Latest(context.Background())
	require.NoError(t, err)
	require.Equal(t, want.Commit, got.Commit)
	require.Equal(t, want.SHA256["test"], got.SHA256["test"])
}

func TestLatest_RefusesTamperedOrForeignManifests(t *testing.T) {
	p := newPublisher(t)
	p.publish(t, "abc123", []byte("new binary"), true /*=latest*/)

	t.Run("edited manifest", func(t *testing.T) {
		orig := p.files["/latest.json"]
		defer func() { p.files["/latest.json"] = orig }()
		p.files["/latest.json"] = []byte(`{"commit":"evil","sha256":{"test":"00"}}`)
		_, err := p.updater(t, "").Latest(context.Background())
		require.ErrorContains(t, err, "signature does not verify")
	})

	t.Run("wrong key", func(t *testing.T) {
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		u := p.updater(t, "")
		u.PublicKey = otherPub
		_, err = u.Latest(context.Background())
		require.ErrorContains(t, err, "signature does not verify")
	})

	t.Run("missing signature", func(t *testing.T) {
		sig := p.files["/latest.json.sig"]
		delete(p.files, "/latest.json.sig")
		defer func() { p.files["/latest.json.sig"] = sig }()
		_, err := p.updater(t, "").Latest(context.Background())
		require.Error(t, err)
	})
}

func TestApply_ReplacesTheExecutableAtomically(t *testing.T) {
	p := newPublisher(t)
	m := p.publish(t, "abc123", []byte("#!/bin/sh\necho new\n"), true /*=latest*/)

	dir := t.TempDir()
	exe := filepath.Join(dir, "bbcert")
	require.NoError(t, os.WriteFile(exe, []byte("old"), 0o755))

	require.NoError(t, p.updater(t, exe).Apply(context.Background(), m))

	got, err := os.ReadFile(exe)
	require.NoError(t, err)
	require.Equal(t, "#!/bin/sh\necho new\n", string(got))
	info, err := os.Stat(exe)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temp file left behind")
}

func TestApply_RefusesABinaryThatDoesNotMatchTheManifest(t *testing.T) {
	p := newPublisher(t)
	m := p.publish(t, "abc123", []byte("published"), true /*=latest*/)
	// The manifest is signed; the binary is not. Swapping it must fail.
	p.files["/abc123/bbcert-test"] = []byte("swapped")

	exe := filepath.Join(t.TempDir(), "bbcert")
	require.NoError(t, os.WriteFile(exe, []byte("old"), 0o755))

	err := p.updater(t, exe).Apply(context.Background(), m)
	require.ErrorContains(t, err, "does not match the digest")
	got, err := os.ReadFile(exe)
	require.NoError(t, err)
	require.Equal(t, "old", string(got), "the executable must be untouched")
}

func TestApply_RefusesAnUnpublishedPlatform(t *testing.T) {
	p := newPublisher(t)
	m := p.publish(t, "abc123", []byte("published"), true /*=latest*/)
	u := p.updater(t, filepath.Join(t.TempDir(), "bbcert"))
	u.Platform = "plan9-mips"
	require.ErrorContains(t, u.Apply(context.Background(), m), "not published for plan9-mips")
}

func TestCheck_ReportsWhetherThePublishedBuildDiffers(t *testing.T) {
	p := newPublisher(t)
	p.publish(t, "abc123", []byte("published"), true /*=latest*/)
	u := p.updater(t, "")

	defer func(v string) { commitSHA = v }(commitSHA)
	commitSHA = "abc123"
	_, needed, err := u.Check(context.Background())
	require.NoError(t, err)
	require.False(t, needed, "running the published commit")

	// Any other commit, older or newer: publishing is manual, and
	// publishing an older commit is how a bad build is rolled back.
	commitSHA = "999999"
	m, needed, err := u.Check(context.Background())
	require.NoError(t, err)
	require.True(t, needed)
	require.Equal(t, "abc123", m.Commit)
}

func TestForCommit_FetchesACanary(t *testing.T) {
	p := newPublisher(t)
	p.publish(t, "aaa", []byte("latest"), true /*=latest*/)
	p.publish(t, "bbb", []byte("canary"), false /*=latest*/)

	u := p.updater(t, "")
	latest, err := u.Latest(context.Background())
	require.NoError(t, err)
	require.Equal(t, "aaa", latest.Commit)
	canary, err := u.ForCommit(context.Background(), "bbb")
	require.NoError(t, err)
	require.Equal(t, "bbb", canary.Commit)
}

func TestRun_RejectsPositionalArguments(t *testing.T) {
	// A commit is selected with -commit; a bare one must not quietly install latest.
	require.Equal(t, 2, Run(context.Background(), []string{"abc123"}))
}

func TestParsePublicKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	got, err := parsePublicKey(string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})))
	require.NoError(t, err)
	require.Equal(t, pub, got)

	// The key shipped in source is what published binaries verify updates
	// with, so it has to parse and be the key Default() uses.
	shipped, err := parsePublicKey(publicKeyPEM)
	require.NoError(t, err)
	defer func(v string) { baseURL = v }(baseURL)
	baseURL = "https://example.com/bbcert"
	u, err := Default()
	require.NoError(t, err)
	require.Equal(t, shipped, u.PublicKey)
}

func TestBaseURL_IsEmptyUnlessStamped(t *testing.T) {
	defer func(v string) { baseURL = v }(baseURL)
	baseURL = ""
	require.Equal(t, "", BaseURL())
	baseURL = "{STABLE_BBCERT_UPDATE_URL}"
	require.Equal(t, "", BaseURL(), "an unstamped build keeps the placeholder")
	baseURL = "https://example.com/bbcert/"
	require.Equal(t, "https://example.com/bbcert", BaseURL())
}
