package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLookupFamily(t *testing.T) {
	for _, tc := range []struct {
		name, osRelease, want, wantErr string
	}{
		{"quoted", "NAME=\"Ubuntu\"\nID=ubuntu\nVERSION_ID=\"22.04\"\n", "22.04", ""},
		{"unquoted", "ID=ubuntu\nVERSION_ID=24.04\n", "24.04", ""},
		{"single quoted", "# comment\n\nID='ubuntu'\nVERSION_ID='20.04'", "20.04", ""},
		{"not ubuntu", "ID=debian\nVERSION_ID=\"12\"\n", "", "not ubuntu"},
		{"no version", "ID=ubuntu\n", "", `unsupported Ubuntu release ""`},
		{"unknown version", "ID=ubuntu\nVERSION_ID=\"18.04\"\n", "", `unsupported Ubuntu release "18.04"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, f, err := lookupFamily([]byte(tc.osRelease))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.NotEmpty(t, f.glibc)
		})
	}
}

func TestDistrustedRootsFormat(t *testing.T) {
	re := regexp.MustCompile(`^([0-9A-F]{2}:){31}[0-9A-F]{2}$`)
	require.NotEmpty(t, distrustedRoots)
	for fp := range distrustedRoots {
		require.Regexp(t, re, fp)
	}
}

// newCert returns a self-signed certificate's DER bytes.
func newCert(t *testing.T, cn string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return der
}

func TestCheckBundle(t *testing.T) {
	var good [][]byte
	for range 5 {
		good = append(good, newCert(t, "good"))
	}
	bad1 := newCert(t, "bad1")
	bad2 := newCert(t, "bad2")
	deny := map[string]string{
		fingerprint(bad1): "Bad Root One",
		fingerprint(bad2): "Bad Root Two",
	}
	bundle := func(ders ...[]byte) []byte {
		var b bytes.Buffer
		for _, d := range ders {
			b.Write(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: d}))
		}
		return b.Bytes()
	}
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	clean := bundle(good...)
	withHeaders := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: map[string]string{"Note": "x"}, Bytes: good[0]})
	const min = 3

	for _, tc := range []struct {
		name    string
		data    []byte
		wantErr []string
	}{
		{name: "clean", data: clean},
		{name: "clean with blank lines", data: append(bytes.ReplaceAll(clean, []byte("-----\n-----BEGIN"), []byte("-----\n\n-----BEGIN")), '\n', '\n')},
		{name: "CRLF", data: bytes.ReplaceAll(clean, []byte("\n"), []byte("\r\n"))},
		{name: "two denied", data: bundle(bad2, good[0], good[1], bad1), wantErr: []string{"Bad Root One", "Bad Root Two"}},
		{name: "denied with trailing byte", data: bundle(good[0], good[1], append(bytes.Clone(bad1), 0)), wantErr: []string{"PEM block 3 isn't a single DER SEQUENCE"}},
		{name: "trusted certificate block", data: cat(clean, pem.EncodeToMemory(&pem.Block{Type: "TRUSTED CERTIFICATE", Bytes: bad1})), wantErr: []string{"TRUSTED CERTIFICATE"}},
		{name: "PEM headers", data: cat(clean, withHeaders), wantErr: []string{"PEM block 6 has headers"}},
		{name: "garbage before first block", data: cat([]byte("garbage\n"), clean), wantErr: []string{"unparseable data after 0 PEM blocks"}},
		{name: "garbage between blocks", data: cat(bundle(good[0]), []byte("garbage\n"), bundle(good[1], good[2])), wantErr: []string{"unparseable data after 1 PEM blocks"}},
		{name: "garbage tail", data: cat(clean, []byte("garbage\n")), wantErr: []string{"unparseable data after 5 PEM blocks"}},
		{name: "truncated tail", data: cat(clean, bundle(bad1)[:100]), wantErr: []string{"PEM block 6 doesn't parse"}},
		{name: "corrupt block in the middle", data: cat(bundle(good[0]), []byte("-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n"), bundle(good[1], good[2])), wantErr: []string{"PEM block 2 doesn't parse"}},
		{name: "too few", data: bundle(good[0], good[1]), wantErr: []string{"only 2 certificates"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkBundle(tc.data, deny, min)
			if len(tc.wantErr) == 0 {
				require.NoError(t, err)
				return
			}
			for _, w := range tc.wantErr {
				require.ErrorContains(t, err, w)
			}
		})
	}
}
