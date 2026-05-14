package performance

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

type label int

const (
	INDEX_BYTES_READ label = iota
	INDEX_KEYS_SCANNED

	DOC_BYTES_READ
	DOC_KEYS_SCANNED

	QUERY_PARSE_DURATION

	POSTING_LIST_QUERY_DURATION
	POSTING_LIST_COUNT
	POSTING_LIST_DOCIDS_COUNT

	REMOVE_DELETED_DOCS_DURATION
	REMOVE_DELETED_DOCS_COUNT

	TOTAL_SCORING_DURATION
	TOTAL_DOCS_SCORED_COUNT

	TOTAL_SEARCH_DURATION
)

func (l label) String() string {
	switch l {
	case INDEX_BYTES_READ:
		return "INDEX_BYTES_READ"
	case INDEX_KEYS_SCANNED:
		return "INDEX_KEYS_SCANNED"
	case DOC_BYTES_READ:
		return "DOC_BYTES_READ"
	case DOC_KEYS_SCANNED:
		return "DOC_KEYS_SCANNED"
	case QUERY_PARSE_DURATION:
		return "QUERY_PARSE_DURATION"
	case POSTING_LIST_QUERY_DURATION:
		return "POSTING_LIST_QUERY_DURATION"
	case POSTING_LIST_COUNT:
		return "POSTING_LIST_COUNT"
	case POSTING_LIST_DOCIDS_COUNT:
		return "POSTING_LIST_DOCIDS_COUNT"
	case REMOVE_DELETED_DOCS_DURATION:
		return "REMOVE_DELETED_DOCS_DURATION"
	case REMOVE_DELETED_DOCS_COUNT:
		return "REMOVE_DELETED_DOCS_COUNT"
	case TOTAL_SCORING_DURATION:
		return "TOTAL_SCORING_DURATION"
	case TOTAL_DOCS_SCORED_COUNT:
		return "TOTAL_DOCS_SCORED_COUNT"
	case TOTAL_SEARCH_DURATION:
		return "TOTAL_SEARCH_DURATION"
	default:
		return "UNKNOWN_PERFORMANCE_LABEL"
	}
}

// I'd like to track:
//   - how many bytes are read per query
//   - query parse time
//   - posting lists query duration
//   - deletion duration
//   - scoring duration and number of docs scored
//   - total search duration

type Tracker struct {
	mu   *sync.Mutex
	data map[label]int64
}

func NewTracker() *Tracker {
	return &Tracker{
		mu:   &sync.Mutex{},
		data: make(map[label]int64),
	}
}

func (t *Tracker) TrackOnce(key label, value int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.data[key]; ok {
		log.Errorf("Attempted overwrite of %s", key)
	} else {
		t.data[key] = value
	}
}

func (t *Tracker) Add(key label, value int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data[key] += value
}

func (t *Tracker) Keys() []label {
	t.mu.Lock()
	keys := slices.Collect(maps.Keys(t.data))
	t.mu.Unlock()

	slices.Sort(keys)
	return keys
}

func (t *Tracker) Get(key label) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.data[key]
}

func (t *Tracker) PrettyPrint() {
	indexMB := (float64(t.Get(INDEX_BYTES_READ)) / 1e6)
	log.Printf("Retrieved %d posting lists in %s [%2.2f MB]", t.Get(POSTING_LIST_COUNT), time.Duration(t.Get(POSTING_LIST_QUERY_DURATION)), indexMB)
	if t.Get(REMOVE_DELETED_DOCS_COUNT) > 0 {
		log.Printf("Filtered %d deleted docs in %s", t.Get(REMOVE_DELETED_DOCS_COUNT), time.Duration(t.Get(REMOVE_DELETED_DOCS_DURATION)))
	}
	log.Printf("Scored %d docs in %s", t.Get(TOTAL_DOCS_SCORED_COUNT), time.Duration(t.Get(TOTAL_SCORING_DURATION)))
	log.Printf("Completed search in %s", time.Duration(t.Get(TOTAL_SEARCH_DURATION)))
}

const trackerContextKey = "x-perf-tracker"

func WrapContext(ctx context.Context) context.Context {
	if t := TrackerFromContext(ctx); t != nil {
		return ctx
	}
	return context.WithValue(ctx, trackerContextKey, NewTracker())
}

func TrackerFromContext(ctx context.Context) *Tracker {
	if v := ctx.Value(trackerContextKey); v != nil {
		if r, ok := v.(*Tracker); ok {
			return r
		}
	}
	return nil
}
