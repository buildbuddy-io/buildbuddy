package xds

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Serves a Node document over TLS and records the Authorization header of
// each request; the test writes the server's certificate as the "mounted"
// CA bundle so the client must verify it (no InsecureSkipVerify anywhere).
func newTLSNodeServer(t *testing.T) (*httptest.Server, *[]string) {
	var auths []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))
		if r.URL.Path != "/api/v1/nodes/node-a" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"labels": map[string]string{"topology.kubernetes.io/zone": "z1"}}})
	}))
	t.Cleanup(srv.Close)
	return srv, &auths
}

func writeFile(t *testing.T, dir, name, content string) string {
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0600))
	return p
}

func TestInClusterNodeLabelGetter(t *testing.T) {
	srv, auths := newTLSNodeServer(t)
	host, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	dir := t.TempDir()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	caFile := writeFile(t, dir, "ca.crt", string(caPEM))
	tokenFile := writeFile(t, dir, "token", "token-1\n")

	getter, err := newInClusterNodeLabelGetter(host, portStr, tokenFile, caFile)
	require.NoError(t, err)
	labels, err := getter.NodeLabels(context.Background(), "node-a")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"topology.kubernetes.io/zone": "z1"}, labels)

	// Token rotation: the file is re-read on the next request.
	require.NoError(t, os.WriteFile(tokenFile, []byte("token-2"), 0600))
	_, err = getter.NodeLabels(context.Background(), "node-a")
	require.NoError(t, err)
	require.Equal(t, []string{"Bearer token-1", "Bearer token-2"}, *auths)

	// Token file disappearing fails the request (fail closed).
	require.NoError(t, os.Remove(tokenFile))
	_, err = getter.NodeLabels(context.Background(), "node-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "service account token")

	// IPv6 hosts are bracketed correctly.
	g6, err := newInClusterNodeLabelGetter("::1", "6443", caFile, caFile)
	require.NoError(t, err)
	require.Equal(t, "https://[::1]:6443", g6.(*restNodeLabelGetter).base.String())
}

func TestInClusterNodeLabelGetter_TLSVerification(t *testing.T) {
	srv, _ := newTLSNodeServer(t)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	dir := t.TempDir()
	tokenFile := writeFile(t, dir, "token", "t")

	// A CA bundle that does not contain the server's certificate must make
	// the request fail with a certificate error. (httptest servers all share
	// one built-in certificate, so mint an unrelated self-signed one.)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "unrelated"}, NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	otherCA := writeFile(t, dir, "other-ca.crt", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	getter, err := newInClusterNodeLabelGetter(host, portStr, tokenFile, otherCA)
	require.NoError(t, err)
	_, err = getter.NodeLabels(context.Background(), "node-a")
	require.Error(t, err)
	var unknownAuthority x509.UnknownAuthorityError
	require.ErrorAs(t, err, &unknownAuthority)

	// Missing env / unreadable or empty CA are construction errors.
	_, err = newInClusterNodeLabelGetter("", "", tokenFile, otherCA)
	require.Error(t, err)
	_, err = newInClusterNodeLabelGetter(host, portStr, tokenFile, filepath.Join(dir, "missing"))
	require.Error(t, err)
	empty := writeFile(t, dir, "empty.crt", "not a certificate")
	_, err = newInClusterNodeLabelGetter(host, portStr, tokenFile, empty)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no certificates")
	_, err = newInClusterNodeLabelGetter(host, portStr, filepath.Join(dir, "missing"), otherCA)
	require.Error(t, err)
}
