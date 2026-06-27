package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/miekg/dns"
	"golang.org/x/sync/singleflight"
)

// acmeBlobPrefix namespaces the per-challenge blobs within the bucket.
const acmeBlobPrefix = "dns/acme-challenge/"

// Challenges stores ACME DNS-01 `_acme-challenge` TXT records in a blobstore --
// one blob per challenge name -- so every density replica sees the same values
// without a shared database. Reads go through a short single-flighted cache so
// concurrent lookups (and repeated polls by Let's Encrypt) collapse to roughly
// one blobstore read per name per TTL.
//
// Writes are read-modify-write without compare-and-swap, so two concurrent adds
// to the *same* name can lose one value; cert-manager re-presents on its
// self-check failure, so this converges. Adds to *different* names never
// conflict (separate blobs).
type Challenges struct {
	bs  interfaces.Blobstore
	ttl time.Duration
	sf  singleflight.Group

	mu  sync.Mutex
	hot map[string]cachedTXT
}

type cachedTXT struct {
	vals []string
	at   time.Time
}

// NewChallenges returns a blobstore-backed challenge store caching lookups for
// ttl.
func NewChallenges(bs interfaces.Blobstore, ttl time.Duration) *Challenges {
	return &Challenges{bs: bs, ttl: ttl, hot: make(map[string]cachedTXT)}
}

func acmeBlobName(fqdn string) string { return acmeBlobPrefix + dns.CanonicalName(fqdn) }

// TXT returns the current TXT values for an `_acme-challenge` name. Concurrent
// lookups for the same name collapse to a single blobstore read, and the result
// (including "absent", i.e. nil) is cached for ttl.
func (c *Challenges) TXT(ctx context.Context, fqdn string) []string {
	fqdn = dns.CanonicalName(fqdn)

	c.mu.Lock()
	if e, ok := c.hot[fqdn]; ok && time.Since(e.at) < c.ttl {
		c.mu.Unlock()
		return e.vals
	}
	c.mu.Unlock()

	v, _, _ := c.sf.Do(fqdn, func() (any, error) {
		vals, err := c.read(ctx, fqdn)
		if err != nil {
			// Transient read error: serve "absent" but don't poison the cache.
			log.Warningf("ACME challenge read for %q failed: %s", fqdn, err)
			return []string(nil), nil
		}
		c.mu.Lock()
		c.hot[fqdn] = cachedTXT{vals: vals, at: time.Now()}
		c.mu.Unlock()
		return vals, nil
	})
	return v.([]string)
}

// read fetches the values straight from the blobstore (bypassing the cache); an
// absent blob is not an error and yields nil.
func (c *Challenges) read(ctx context.Context, fqdn string) ([]string, error) {
	data, err := c.bs.ReadBlob(ctx, acmeBlobName(fqdn))
	if status.IsNotFoundError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeTXT(data), nil
}

// Add adds vals to the name's TXT set (deduplicated).
func (c *Challenges) Add(ctx context.Context, fqdn string, vals ...string) error {
	fqdn = dns.CanonicalName(fqdn)
	cur, err := c.read(ctx, fqdn)
	if err != nil {
		return err
	}
	if _, err := c.bs.WriteBlob(ctx, acmeBlobName(fqdn), encodeTXT(union(cur, vals))); err != nil {
		return err
	}
	c.evict(fqdn)
	return nil
}

// Delete removes vals from the name's TXT set, deleting the blob entirely when
// no values are given or none remain.
func (c *Challenges) Delete(ctx context.Context, fqdn string, vals ...string) error {
	fqdn = dns.CanonicalName(fqdn)
	defer c.evict(fqdn)
	if len(vals) == 0 {
		return c.bs.DeleteBlob(ctx, acmeBlobName(fqdn))
	}
	cur, err := c.read(ctx, fqdn)
	if err != nil {
		return err
	}
	rem := subtract(cur, vals)
	if len(rem) == 0 {
		return c.bs.DeleteBlob(ctx, acmeBlobName(fqdn))
	}
	_, err = c.bs.WriteBlob(ctx, acmeBlobName(fqdn), encodeTXT(rem))
	return err
}

func (c *Challenges) evict(fqdn string) {
	c.mu.Lock()
	delete(c.hot, fqdn)
	c.mu.Unlock()
}

// encodeTXT/decodeTXT store the value set as newline-separated lines.
func encodeTXT(vals []string) []byte { return []byte(strings.Join(vals, "\n")) }

func decodeTXT(data []byte) []string {
	var out []string
	for line := range strings.SplitSeq(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

func union(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, group := range [][]string{a, b} {
		for _, s := range group {
			if _, ok := seen[s]; !ok {
				seen[s] = struct{}{}
				out = append(out, s)
			}
		}
	}
	return out
}

func subtract(a, remove []string) []string {
	drop := make(map[string]struct{}, len(remove))
	for _, s := range remove {
		drop[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := drop[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}
