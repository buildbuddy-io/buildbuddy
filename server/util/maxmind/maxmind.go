// Package maxmind allows geo/asn lookups by IP.
//
// Including this package may increase your binary size drastically, because
// maxmind lookups happen via a bundled database.
package maxmind

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/netip"
	"sync"

	"github.com/oschwald/maxminddb-golang/v2"

	_ "embed"
)

// The database is embedded gzipped to keep the binary smaller; it's
// decompressed once when the reader is first opened.
//
//go:embed dbip-asn-lite.mmdb.gz
var dbipASNLiteGz []byte

// GetASNLiteDB returns a reader for the bundled DB-IP ASN Lite database. The
// reader is opened lazily on first use and shared across callers; it is safe
// for concurrent use.
var GetASNLiteDB = sync.OnceValues(func() (*maxminddb.Reader, error) {
	gz, err := gzip.NewReader(bytes.NewReader(dbipASNLiteGz))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	db, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	return maxminddb.OpenBytes(db)
})

// ASN holds the autonomous system information for an IP address.
type ASN struct {
	Number       uint   `maxminddb:"autonomous_system_number"`
	Organization string `maxminddb:"autonomous_system_organization"`
}

// LookupASN returns the autonomous system information for the given IP address.
func LookupASN(ip netip.Addr) (*ASN, error) {
	db, err := GetASNLiteDB()
	if err != nil {
		return nil, err
	}
	asn := &ASN{}
	if err := db.Lookup(ip).Decode(asn); err != nil {
		return nil, err
	}
	return asn, nil
}
