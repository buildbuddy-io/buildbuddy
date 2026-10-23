package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestLookupFamily(t *testing.T) {
	for _, tc := range []struct {
		name, osRelease, want, wantErr string
	}{
		{"quoted", "NAME=\"Ubuntu\"\nID=ubuntu\nVERSION_ID=\"22.04\"\n", "22.04", ""},
		{"unquoted", "ID=ubuntu\nVERSION_ID=24.04\n", "24.04", ""},
		{"single quoted", "# comment\n\nID='ubuntu'\nVERSION_ID='20.04'", "20.04", ""},
		{"not ubuntu", "ID=debian\nVERSION_ID=\"12\"\n", "", "not ubuntu"},
		{"no ID", "VERSION_ID=\"22.04\"\n", "", "not ubuntu"},
		{"no version", "ID=ubuntu\n", "", "no VERSION_ID"},
		{"unknown version", "ID=ubuntu\nVERSION_ID=\"18.04\"\n", "", "unsupported Ubuntu release \"18.04\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, f, err := lookupFamily([]byte(tc.osRelease))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("lookupFamily() error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("lookupFamily() error = %v", err)
			}
			if got != tc.want || f.glibc == "" {
				t.Fatalf("lookupFamily() = %q, %+v, want %q", got, f, tc.want)
			}
		})
	}
}

func TestParseGlibcVersion(t *testing.T) {
	for _, tc := range []struct {
		out, want string
		wantErr   bool
	}{
		{"glibc 2.35\n", "2.35", false},
		{"glibc 2.31", "2.31", false},
		{"", "", true},
		{"musl 1.2.4\n", "", true},
		{"glibc\n", "", true},
		{"glibc 2.35 extra\n", "", true},
	} {
		got, err := parseGlibcVersion(tc.out)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("parseGlibcVersion(%q) = %q, %v, want %q, error %t", tc.out, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestHasLocale(t *testing.T) {
	for _, tc := range []struct {
		out  string
		want bool
	}{
		{"C\nC.utf8\nen_US.utf8\nPOSIX\n", true},
		{"C\nen_US.UTF-8\n", false},
		{"C\nEN_us.UTF8\n", true},
		{"C\nC.utf8\nPOSIX\n", false},
		{"C\nen_US.utf8.bak\n", false},
		{"", false},
	} {
		if got := hasLocale(tc.out, "en_US.utf8"); got != tc.want {
			t.Errorf("hasLocale(%q) = %t, want %t", tc.out, got, tc.want)
		}
	}
}

func TestDistrustedRootsParse(t *testing.T) {
	deny, err := parseDenylist(distrustedRoots)
	if err != nil {
		t.Fatal(err)
	}
	if len(deny) != len(distrustedRoots) || len(deny) == 0 {
		t.Fatalf("parsed %d fingerprints from %d entries", len(deny), len(distrustedRoots))
	}
}

func TestParseFingerprint(t *testing.T) {
	for _, s := range []string{"", "AB:CD", "ZZ" + strings.Repeat(":00", 31), strings.Repeat("00:", 32) + "00"} {
		if _, err := parseFingerprint(s); err == nil {
			t.Errorf("parseFingerprint(%q) succeeded, want error", s)
		}
	}
}

// newCert returns a self-signed certificate's DER bytes.
func newCert(t *testing.T, cn string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

func TestCheckBundle(t *testing.T) {
	var good [][]byte
	for range 5 {
		good = append(good, newCert(t, "good"))
	}
	bad1 := newCert(t, "bad1")
	bad2 := newCert(t, "bad2")
	deny := map[[32]byte]string{
		sha256.Sum256(bad1): "Bad Root One",
		sha256.Sum256(bad2): "Bad Root Two",
	}
	bundle := func(ders ...[]byte) []byte {
		var b bytes.Buffer
		for _, d := range ders {
			b.Write(pemBlock("CERTIFICATE", d))
		}
		return b.Bytes()
	}
	clean := bundle(good...)
	const min = 3

	for _, tc := range []struct {
		name    string
		data    []byte
		min     int
		wantErr []string
	}{
		{name: "clean", data: clean, min: min},
		{name: "clean with blank lines", data: append(bytes.ReplaceAll(clean, []byte("-----\n-----BEGIN"), []byte("-----\n\n-----BEGIN")), '\n', '\n'), min: min},
		{name: "one denied", data: bundle(good[0], bad1, good[1], good[2]), min: min, wantErr: []string{"Bad Root One"}},
		{name: "two denied", data: bundle(bad2, good[0], good[1], bad1), min: min, wantErr: []string{"Bad Root One", "Bad Root Two"}},
		{name: "trusted certificate block", data: append(bytes.Clone(clean), pemBlock("TRUSTED CERTIFICATE", bad1)...), min: min, wantErr: []string{"TRUSTED CERTIFICATE"}},
		{name: "truncated tail", data: append(bytes.Clone(clean), pemBlock("CERTIFICATE", bad1)[:100]...), min: min, wantErr: []string{"unparseable"}},
		{name: "garbage tail", data: append(bytes.Clone(clean), "garbage\n"...), min: min, wantErr: []string{"unparseable"}},
		{name: "corrupt block in the middle", data: append(append(bundle(good[0]), "-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n"...), bundle(good[1], good[2])...), min: min, wantErr: []string{"1 of 4 PEM blocks don't parse"}},
		{name: "empty", data: nil, min: min, wantErr: []string{"only 0 certificates"}},
		{name: "too few", data: bundle(good[0], good[1]), min: min, wantErr: []string{"only 2 certificates"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkBundle(tc.data, deny, tc.min)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("checkBundle() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkBundle() = nil, want error containing %q", tc.wantErr)
			}
			for _, w := range tc.wantErr {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("checkBundle() = %v, want it to contain %q", err, w)
				}
			}
		})
	}
}
