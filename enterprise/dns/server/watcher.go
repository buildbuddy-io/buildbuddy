package server

import (
	"context"
	"crypto/sha256"
	"iter"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/miekg/dns"
)

const (
	// zoneFileInvalidAlert is the constant alert name emitted when a single GCS
	// zone-file object fails to parse or is missing its SOA. It is a Prometheus
	// metric label, so it must stay constant -- the offending object name goes
	// in the alert's log message, not the label.
	zoneFileInvalidAlert = "dns_zone_file_invalid"

	// zoneFileSuffix is the object-name suffix that marks a bucket object as a
	// zone file to serve. Objects without it are ignored.
	zoneFileSuffix = ".zone"
)

// zoneStore is the subset of the GCS blobstore the watcher needs: list object
// names and read an object's bytes. *gcs.GCSBlobStore satisfies it. It is an
// interface so the watcher can be exercised with a fake in tests.
type zoneStore interface {
	List(ctx context.Context, prefix string) iter.Seq2[string, error]
	ReadBlob(ctx context.Context, name string) ([]byte, error)
}

// zoneFileState is the last-good parsed state of a single GCS zone-file object:
// the content hash we loaded it at (to skip re-parsing unchanged objects) and
// the records it parsed to.
type zoneFileState struct {
	hash    [sha256.Size]byte
	records []dns.RR
}

// ZoneWatcher polls a GCS bucket for zone-file objects (those whose name ends in
// ".zone"), loading and verifying each object independently, and hot-swaps the
// handler's served records whenever any object changes.
//
// Each object is kept at its last-good version: one that fails to parse or
// lacks an SOA is skipped (and alerted on) without disturbing the other zones.
// The GCS source is never fatal -- a failed poll, or a bucket that is empty or
// all-invalid, just logs and keeps watching. GCS and local --dns.zone_file
// sources are mutually exclusive, so the watcher owns the entire served set.
type ZoneWatcher struct {
	handler  *Handler
	store    zoneStore
	interval time.Duration

	// files holds the last-good state of each GCS object we've successfully
	// loaded, keyed by object name. Accessed only from the single poll
	// goroutine, so it needs no lock.
	files map[string]*zoneFileState
}

// NewZoneWatcher constructs a watcher that serves, via handler, every ".zone"
// object in store's bucket, re-polling every interval.
func NewZoneWatcher(handler *Handler, store zoneStore, interval time.Duration) *ZoneWatcher {
	return &ZoneWatcher{
		handler:  handler,
		store:    store,
		interval: interval,
		files:    make(map[string]*zoneFileState),
	}
}

// Start performs a best-effort initial poll (so the server begins serving GCS
// zones promptly) and then polls on a ticker until ctx is cancelled. It never
// returns an error: the GCS source is non-fatal by design, so a failed or empty
// initial poll comes up serving nothing and retries on the next tick.
func (w *ZoneWatcher) Start(ctx context.Context) {
	w.poll(ctx)
	if len(w.files) == 0 {
		log.Warningf("dns: no valid zone files found in GCS yet; continuing to watch")
	}
	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.poll(ctx)
			}
		}
	}()
}

// poll lists the zone-file objects, reloads any that changed (keeping the
// last-good version of any that fail to read/parse), drops any that
// disappeared, and -- if the set changed -- rebuilds and swaps in the handler's
// records. It never fails fatally; every error is logged (and single-file parse
// failures are alerted) and the current zones are preserved.
//
// Changes are staged and only committed to w.files after the listing completes
// successfully. If listing errors partway through, w.files is left untouched, so
// a partially-read listing can't record objects as "loaded" without ever
// pushing them (which a later unchanged poll would then never re-push).
func (w *ZoneWatcher) poll(ctx context.Context) {
	present := make(map[string]bool)
	updates := make(map[string]*zoneFileState)
	// An empty prefix lists the whole bucket; suffix filtering below selects the
	// zone files.
	for name, err := range w.store.List(ctx, "") {
		if err != nil {
			// Abandon this poll entirely; keep the zones we're already serving.
			log.Warningf("dns: listing zone files from GCS failed, keeping current zones: %s", err)
			return
		}
		if !strings.HasSuffix(name, zoneFileSuffix) {
			continue
		}
		present[name] = true

		data, err := w.store.ReadBlob(ctx, name)
		if err != nil {
			// A transient read failure keeps the last-good version (if any); the
			// object is still "present" so it isn't dropped below.
			log.Warningf("dns: reading zone file %q failed, keeping last-good: %s", name, err)
			continue
		}

		hash := sha256.Sum256(data)
		if cur, ok := w.files[name]; ok && cur.hash == hash {
			continue // unchanged since last-good load
		}

		records, err := parseAndVerifyZone(data, name)
		if err != nil {
			// One bad file must not disturb the others: alert so we can fix it,
			// and keep this object's last-good records (if we had any).
			alert.UnexpectedEvent(zoneFileInvalidAlert, "zone file %q rejected, keeping last-good: %s", name, err)
			continue
		}
		updates[name] = &zoneFileState{hash: hash, records: records}
	}

	// Listing completed successfully: commit the staged reloads and drop any
	// objects that no longer exist in the bucket.
	changed := false
	for name, st := range updates {
		w.files[name] = st
		changed = true
	}
	for name := range w.files {
		if !present[name] {
			delete(w.files, name)
			changed = true
		}
	}

	if !changed {
		return
	}
	w.handler.Update(w.assemble())
	log.Infof("dns: serving %d zone file(s) from GCS", len(w.files))
}

// assemble builds the full record set to serve: every last-good GCS object's
// records, ordered by object name so the resulting snapshot is deterministic.
func (w *ZoneWatcher) assemble() []dns.RR {
	names := make([]string, 0, len(w.files))
	for name := range w.files {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []dns.RR
	for _, name := range names {
		out = append(out, w.files[name].records...)
	}
	return out
}

// parseAndVerifyZone parses a zone file's bytes and requires it to define an SOA
// at its apex. A file without an SOA contributes no apex, so its names would
// route to no zone and be answered REFUSED even though they loaded -- a silent
// failure -- so it is rejected here.
func parseAndVerifyZone(data []byte, name string) ([]dns.RR, error) {
	records, err := ParseZoneBytes(data, name)
	if err != nil {
		return nil, err
	}
	if !hasSOA(records) {
		return nil, status.FailedPreconditionErrorf("zone file %q has no SOA record at its apex", name)
	}
	return records, nil
}

// hasSOA reports whether rrs contains an SOA record.
func hasSOA(rrs []dns.RR) bool {
	for _, rr := range rrs {
		if rr.Header().Rrtype == dns.TypeSOA {
			return true
		}
	}
	return false
}
