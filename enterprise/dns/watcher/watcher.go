// Package watcher polls a blobstore bucket for objects whose name ends in a
// given suffix and reports, on each change, which objects' contents are new or
// changed and which were removed. It tracks each object's GCS generation so
// unchanged objects are never re-downloaded, bounds every GCS request with a
// timeout, and is content-agnostic: callers parse and act on the bytes it hands
// back.
package watcher

import (
	"context"
	"iter"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// opTimeout bounds each individual GCS request (the object listing, and each
// object read) so a slow or hung backend can't wedge a poll -- or, at startup,
// block the caller. On timeout the poll logs and keeps its current state,
// retrying on the next tick.
const opTimeout = 10 * time.Second

// Store is the subset of the GCS blobstore the watcher needs: list object
// attributes and read an object's bytes. *gcs.GCSBlobStore satisfies it; it is
// an interface so the watcher can be exercised with a fake in tests.
type Store interface {
	List(ctx context.Context, prefix string) iter.Seq2[gcs.ObjectAttrs, error]
	ReadBlob(ctx context.Context, name string) ([]byte, error)
}

// Update reports, since the previous poll, the objects whose contents are new
// or changed (keyed by object name) and the names of objects that were removed.
// At least one of the two is non-empty whenever an Update is delivered.
type Update struct {
	Changed map[string][]byte
	Removed []string
}

// Watcher polls a Store for objects ending in a suffix and invokes onUpdate
// whenever their contents or membership change.
type Watcher struct {
	store    Store
	suffix   string
	interval time.Duration
	onUpdate func(Update)

	// generations is the GCS generation of each object we've last read and
	// delivered, keyed by name. An object whose listed generation matches is
	// skipped: not re-read, not re-delivered. Touched only from the poll
	// goroutine (and Poll's caller), so it needs no lock.
	generations map[string]int64
}

// New constructs a Watcher over store for objects whose name ends in suffix,
// polling every interval and calling onUpdate with each change. onUpdate is
// invoked synchronously from the polling goroutine (and from Poll's caller), so
// it should not block for long.
func New(store Store, suffix string, interval time.Duration, onUpdate func(Update)) *Watcher {
	return &Watcher{
		store:       store,
		suffix:      suffix,
		interval:    interval,
		onUpdate:    onUpdate,
		generations: make(map[string]int64),
	}
}

// Start performs a best-effort initial poll synchronously (so the caller can
// observe the initial load before this returns) and then polls every interval
// in a background goroutine until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) {
	w.Poll(ctx)
	if len(w.generations) == 0 {
		log.Warningf("watcher: no %q objects found in bucket yet; continuing to watch", w.suffix)
	}
	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.Poll(ctx)
			}
		}
	}()
}

// Poll performs a single listing pass: it reads every matching object whose
// generation is new or changed, computes which tracked objects disappeared, and
// -- if anything changed -- invokes onUpdate once with the deltas. It never
// fails fatally; listing/read errors are logged and the current state is
// preserved (a failed listing abandons the poll; a failed read retries next
// poll).
//
// Generation state is committed only after the listing completes successfully,
// so a listing that errors partway can't half-advance the tracked generations.
func (w *Watcher) Poll(ctx context.Context) {
	present := make(map[string]bool)
	changed := make(map[string][]byte)
	newGens := make(map[string]int64)

	listCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	// An empty prefix lists the whole bucket; the suffix filter selects objects.
	for attrs, err := range w.store.List(listCtx, "") {
		if err != nil {
			log.Warningf("watcher: listing objects failed, keeping current state: %s", err)
			return
		}
		name := attrs.Name
		if !strings.HasSuffix(name, w.suffix) {
			continue
		}
		present[name] = true
		if gen, ok := w.generations[name]; ok && gen == attrs.Generation {
			continue // unchanged since last delivered
		}

		readCtx, cancelRead := context.WithTimeout(ctx, opTimeout)
		data, err := w.store.ReadBlob(readCtx, name)
		cancelRead()
		if err != nil {
			// Transient: don't record the generation, so the object is retried on
			// the next poll. It's still present, so it isn't reported removed.
			log.Warningf("watcher: reading %q failed, will retry: %s", name, err)
			continue
		}
		changed[name] = data
		newGens[name] = attrs.Generation
	}

	// Listing completed: compute removals against the fully-known present set,
	// then commit the generation state.
	var removed []string
	for name := range w.generations {
		if !present[name] {
			removed = append(removed, name)
		}
	}
	for name, gen := range newGens {
		w.generations[name] = gen
	}
	for _, name := range removed {
		delete(w.generations, name)
	}

	if len(changed) == 0 && len(removed) == 0 {
		return
	}
	w.onUpdate(Update{Changed: changed, Removed: removed})
}
