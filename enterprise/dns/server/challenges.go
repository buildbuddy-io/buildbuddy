package server

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/miekg/dns"
	"golang.org/x/sync/singleflight"
)

const (
	// acmeBlobPrefix namespaces the per-challenge blobs within the bucket.
	acmeBlobPrefix = "dns/acme-challenge/"

	// acmeCacheMaxEntries caps the in-memory TXT cache. Bounded so that a flood
	// of queries for distinct (possibly nonexistent) _acme-challenge.* names
	// can't grow it without limit; the least-recently-used name is evicted.
	acmeCacheMaxEntries = 1000
)

// Challenges stores ACME DNS-01 `_acme-challenge` TXT records in a blobstore --
// one blob per challenge name -- so every density replica sees the same values
// without a shared database. Reads go through a short single-flighted, TTL'd,
// LRU-bounded cache so concurrent lookups (and repeated polls by Let's Encrypt)
// collapse to roughly one blobstore read per name per TTL. Only present values
// are cached: an "absent" result is never cached, so a query that races ahead
// of the UPDATE that creates a name can't pin a stale negative answer on a
// replica (and a flood of misses can't fill the cache).
//
// The cache is per-replica and uncoordinated, so a value removed (or changed)
// by a write can still be served for up to ttl by a replica that cached it
// beforehand -- whether because a local lookup raced the eviction or because the
// replica simply hasn't read the new state yet. This is benign for ACME: ttl is
// short and challenge cleanup is not time-critical.
//
// Writes are read-modify-write without compare-and-swap, so two concurrent adds
// to the *same* name can lose one value; cert-manager re-presents on its
// self-check failure, so this converges. Adds to *different* names never
// conflict (separate blobs).
type Challenges struct {
	bs  interfaces.Blobstore
	sf  singleflight.Group
	hot lru.LRU[[]string]
}

// NewChallenges returns a blobstore-backed challenge store caching present
// lookups for ttl (bounded to acmeCacheMaxEntries names).
func NewChallenges(bs interfaces.Blobstore, ttl time.Duration) (*Challenges, error) {
	hot, err := lru.New(&lru.Config[[]string]{
		MaxSize:    acmeCacheMaxEntries,
		TTL:        ttl,
		SizeFn:     func([]string) int64 { return 1 },
		ThreadSafe: true,
	})
	if err != nil {
		return nil, err
	}
	return &Challenges{bs: bs, hot: hot}, nil
}

func acmeBlobName(fqdn string) string { return acmeBlobPrefix + dns.CanonicalName(fqdn) }

// TXT returns the current TXT values for an `_acme-challenge` name. Concurrent
// lookups for the same name collapse to a single blobstore read, and a present
// result is cached for ttl. An absent result is not cached.
func (c *Challenges) TXT(ctx context.Context, fqdn string) []string {
	fqdn = dns.CanonicalName(fqdn)

	if vals, ok := c.hot.Get(fqdn); ok {
		return vals
	}

	v, _, _ := c.sf.Do(fqdn, func() (any, error) {
		vals, err := c.read(ctx, fqdn)
		if err != nil {
			// Transient read error: serve "absent" but don't cache it.
			log.Warningf("ACME challenge read for %q failed: %s", fqdn, err)
			return []string(nil), nil
		}
		// Cache only present values (see the type doc for why "absent" isn't).
		if len(vals) > 0 {
			c.hot.Add(fqdn, vals)
		}
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
	merged := slices.Collect(set.Union(set.From(cur...), set.From(vals...)))
	if _, err := c.bs.WriteBlob(ctx, acmeBlobName(fqdn), encodeTXT(merged)); err != nil {
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
	rem := slices.Collect(set.Difference(set.From(cur...), set.From(vals...)))
	if len(rem) == 0 {
		return c.bs.DeleteBlob(ctx, acmeBlobName(fqdn))
	}
	_, err = c.bs.WriteBlob(ctx, acmeBlobName(fqdn), encodeTXT(rem))
	return err
}

func (c *Challenges) evict(fqdn string) { c.hot.Remove(fqdn) }

// encodeTXT/decodeTXT serialize the value set as a JSON array of strings, so
// values round-trip exactly regardless of their contents.
func encodeTXT(vals []string) []byte {
	// Marshaling a []string cannot fail.
	data, _ := json.Marshal(vals)
	return data
}

func decodeTXT(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	var vals []string
	if err := json.Unmarshal(data, &vals); err != nil {
		log.Warningf("ACME challenge blob has invalid JSON (%d bytes): %s", len(data), err)
		return nil
	}
	return vals
}
