package server

import (
	"context"
	"iter"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

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

	// gcsOpTimeout bounds each individual GCS request (the object listing, and
	// each object read) so a slow or hung backend can't wedge a poll -- and, at
	// startup, can't block the server from coming up. On timeout the poll logs
	// and keeps the zones it already serves, retrying on the next tick.
	gcsOpTimeout = 10 * time.Second
)

// zoneStore is the subset of the GCS blobstore the watcher needs: list object
// attributes and read an object's bytes. *gcs.GCSBlobStore satisfies it. It is
// an interface so the watcher can be exercised with a fake in tests.
type zoneStore interface {
	List(ctx context.Context, prefix string) iter.Seq2[gcs.ObjectAttrs, error]
	ReadBlob(ctx context.Context, name string) ([]byte, error)
}

// zoneFileState is what the watcher remembers about a single GCS zone-file
// object between polls.
type zoneFileState struct {
	// generation is the GCS generation the watcher last processed for this
	// object (whether it parsed successfully or was rejected). An object whose
	// current generation matches this is skipped entirely -- not re-read,
	// re-parsed, or re-alerted -- since its content cannot have changed.
	generation int64

	// records is the object's last-good parsed records, still served even if the
	// current generation was rejected. nil if the object has never parsed
	// successfully.
	records []dns.RR
}

// ZoneWatcher polls a GCS bucket for zone-file objects (those whose name ends in
// ".zone"), loading and verifying each object independently, and hot-swaps the
// handler's served records whenever any object changes.
//
// Each object is kept at its last-good version: one that fails to parse or
// lacks an SOA is skipped (and alerted on once per bad version) without
// disturbing the other zones. The GCS source is never fatal -- a failed poll, or
// a bucket that is empty or all-invalid, just logs and keeps watching. GCS and
// local --dns.zone_file sources are mutually exclusive, so the watcher owns the
// entire served set.
type ZoneWatcher struct {
	handler  *Handler
	store    zoneStore
	interval time.Duration

	// files holds what we know about each GCS object, keyed by object name.
	// Accessed only from the single poll goroutine, so it needs no lock.
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
// returns an error: the GCS source is non-fatal by design, and each GCS request
// in the initial poll is bounded by gcsOpTimeout, so a slow or unreachable
// bucket can't block startup -- the server comes up serving whatever loaded (or
// nothing) and the ticker retries.
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

// poll lists the zone-file objects, reloads any whose generation changed
// (keeping the last-good version of any that fail to read/parse), drops any that
// disappeared, and -- if the served set changed -- rebuilds and swaps in the
// handler's records. It never fails fatally; every error is logged (and each
// newly-seen bad version is alerted once) and the current zones are preserved.
//
// Changes are staged and only committed to w.files after the listing completes
// successfully. If listing errors partway through, w.files is left untouched, so
// a partially-read listing can't record objects as "processed" without ever
// serving them (which a later unchanged poll would then never revisit).
func (w *ZoneWatcher) poll(ctx context.Context) {
	present := make(map[string]bool)
	staged := make(map[string]*zoneFileState)
	changed := false

	listCtx, cancelList := context.WithTimeout(ctx, gcsOpTimeout)
	defer cancelList()
	// An empty prefix lists the whole bucket; suffix filtering below selects the
	// zone files.
	for attrs, err := range w.store.List(listCtx, "") {
		if err != nil {
			// Abandon this poll entirely; keep the zones we're already serving.
			log.Warningf("dns: listing zone files from GCS failed, keeping current zones: %s", err)
			return
		}
		name := attrs.Name
		if !strings.HasSuffix(name, zoneFileSuffix) {
			continue
		}
		present[name] = true

		// Skip objects whose version we've already processed: an unchanged
		// generation means unchanged content, so there's nothing to re-read,
		// re-parse, or re-alert. This is what keeps a stuck-bad file from
		// alerting every poll, and avoids re-downloading unchanged zones.
		if cur, ok := w.files[name]; ok && cur.generation == attrs.Generation {
			continue
		}

		readCtx, cancelRead := context.WithTimeout(ctx, gcsOpTimeout)
		data, err := w.store.ReadBlob(readCtx, name)
		cancelRead()
		if err != nil {
			// A transient read failure keeps the last-good version (if any) and,
			// by not recording this generation, retries on the next poll. The
			// object is still "present" so it isn't dropped below.
			log.Warningf("dns: reading zone file %q failed, keeping last-good: %s", name, err)
			continue
		}

		records, err := ParseAndVerifyZone(data, name)
		if err != nil {
			// One bad file must not disturb the others: alert (once for this
			// version, since we record its generation) and keep this object's
			// last-good records, if any.
			alert.UnexpectedEvent(zoneFileInvalidAlert, "zone file %q rejected, keeping last-good: %s", name, err)
			var lastGood []dns.RR
			if cur, ok := w.files[name]; ok {
				lastGood = cur.records
			}
			staged[name] = &zoneFileState{generation: attrs.Generation, records: lastGood}
			continue
		}
		staged[name] = &zoneFileState{generation: attrs.Generation, records: records}
		changed = true
	}

	// Listing completed successfully: commit the staged results and drop any
	// objects that no longer exist in the bucket. Dropping an object only changes
	// the served set if it was actually contributing records.
	for name, st := range staged {
		w.files[name] = st
	}
	for name, st := range w.files {
		if !present[name] {
			if len(st.records) > 0 {
				changed = true
			}
			delete(w.files, name)
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
