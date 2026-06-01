package indexprofile

import (
	"container/heap"
	"fmt"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

type Phase string

const (
	PhaseWalk               Phase = "walk_total"
	PhaseReadFile           Phase = "read_file"
	PhaseAddFileToIndex     Phase = "add_file_to_index"
	PhaseValidateFile       Phase = "validate_file"
	PhaseDetectLanguage     Phase = "detect_language"
	PhaseMakeDocument       Phase = "make_document"
	PhaseUpdateDocument     Phase = "update_document"
	PhaseDeleteExisting     Phase = "delete_existing"
	PhaseAddDocument        Phase = "add_document"
	PhaseTokenizerNext      Phase = "tokenizer_next"
	PhasePostingMutation    Phase = "posting_mutation"
	PhaseStoredFieldSet     Phase = "stored_field_set"
	PhaseFlush              Phase = "flush"
	PhaseUpdatePostingList  Phase = "update_posting_list"
	PhasePostingListMarshal Phase = "posting_list_marshal"
	PhasePebbleBatchSet     Phase = "pebble_batch_set"
	PhasePebbleBatchCommit  Phase = "pebble_batch_commit"
)

type Counter string

const (
	CounterPathsVisited           Counter = "paths_visited"
	CounterFilesSeen              Counter = "files_seen"
	CounterFilesRead              Counter = "files_read"
	CounterFilesIndexed           Counter = "files_indexed"
	CounterFilesSkipped           Counter = "files_skipped"
	CounterBytesRead              Counter = "bytes_read"
	CounterDocsAdded              Counter = "docs_added"
	CounterFieldsIndexed          Counter = "fields_indexed"
	CounterTokensIndexed          Counter = "tokens_indexed"
	CounterPostingListLookups     Counter = "posting_list_lookups"
	CounterPostingListsCreated    Counter = "posting_lists_created"
	CounterPostingListsFlushed    Counter = "posting_lists_flushed"
	CounterPostingListKeyBytes    Counter = "posting_list_key_bytes"
	CounterPostingListValueBytes  Counter = "posting_list_value_bytes"
	CounterStoredFieldsSet        Counter = "stored_fields_set"
	CounterPebbleBatchSets        Counter = "pebble_batch_sets"
	CounterPebbleBatchSetBytes    Counter = "pebble_batch_set_bytes"
	CounterPebbleBatchCommits     Counter = "pebble_batch_commits"
	CounterPebbleBatchCommitBytes Counter = "pebble_batch_commit_bytes"
	CounterHiddenPathsSkipped     Counter = "hidden_paths_skipped"
	CounterValidationSkippedFiles Counter = "validation_skipped_files"
	CounterAddFileErrors          Counter = "add_file_errors"
	CounterTFNgramOccurrences     Counter = "tf_ngram_occurrences"
	CounterTFUniquePostings       Counter = "tf_unique_postings"
	CounterTFDuplicateOccurrences Counter = "tf_duplicate_occurrences"
	CounterTFPostingsWithFreqGT1  Counter = "tf_postings_with_freq_gt_1"
	CounterTFRLEBytes             Counter = "tf_rle_bytes_estimate"
)

type phaseStats struct {
	count int64
	total time.Duration
}

type postingListAggregate struct {
	lists      int64
	postings   int64
	keyBytes   int64
	valueBytes int64
}

type postingListBucketKey struct {
	field string
	label string
	order int
}

type postingListLengthKey struct {
	field  string
	length int
}

type postingListTopEntry struct {
	field       string
	ngram       string
	cardinality uint64
	keyBytes    int64
	valueBytes  int64
}

type TermFrequencyStats = types.TermFrequencyStats

// TermFrequencyStatsFromFrequencies summarizes a per-posting frequency slice
// in iteration order. freqs is expected to be in the order the posting list's
// roaring iterator yields doc IDs — the same order used when serializing, so
// that the RLE byte estimate matches what BuilderList.Marshal would write.
func TermFrequencyStatsFromFrequencies(freqs []uint32) TermFrequencyStats {
	stats := TermFrequencyStats{}
	// Per-posting aggregates.
	for _, freq := range freqs {
		tf := uint64(freq)
		stats.Occurrences += int64(tf)
		stats.UniquePostings++
		addTermFrequencyBucket(&stats, tf)
		if freq > 1 {
			stats.DuplicatePostings++
			stats.DuplicateOccurrences += int64(tf - 1)
		}
	}
	// RLE byte estimate: walk runs of identical values.
	for i := 0; i < len(freqs); {
		v := freqs[i]
		j := i + 1
		for j < len(freqs) && freqs[j] == v {
			j++
		}
		stats.RLEBytesEstimate += int64(uvarintLen64(uint64(j-i)) + uvarintLen64(uint64(v)))
		i = j
	}
	return stats
}

func addTermFrequencyBucket(stats *TermFrequencyStats, tf uint64) {
	bucket := bits.Len(uint(tf - 1))
	if bucket >= types.NumTFLog2Buckets {
		bucket = types.NumTFLog2Buckets - 1
	}
	stats.CountsByLog2Bucket[bucket]++
}

// tfLog2BucketLabel returns a "1", "2", "3-4", "5-8", … style label for the
// given bucket index.
func tfLog2BucketLabel(i int) string {
	switch i {
	case 0:
		return "1"
	case 1:
		return "2"
	}
	return fmt.Sprintf("%d-%d", 1<<(i-1)+1, 1<<i)
}

// uvarintLen64 returns the number of bytes that binary.PutUvarint would write
// for v, without actually allocating a buffer. Used here only to estimate the
// on-disk byte cost of the term-frequency tail for profiling.
func uvarintLen64(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

type topPostingListHeap struct {
	entries []postingListTopEntry
	less    func(a, b postingListTopEntry) bool
}

func (h topPostingListHeap) Len() int           { return len(h.entries) }
func (h topPostingListHeap) Less(i, j int) bool { return h.less(h.entries[i], h.entries[j]) }
func (h topPostingListHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *topPostingListHeap) Push(x any) {
	h.entries = append(h.entries, x.(postingListTopEntry))
}

func (h *topPostingListHeap) Pop() any {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[:n-1]
	return x
}

type Profiler struct {
	start time.Time

	mu                        sync.Mutex
	phases                    map[Phase]phaseStats
	counters                  map[Counter]int64
	postingListsByCardinality map[postingListBucketKey]postingListAggregate
	postingListsByNgramLength map[postingListLengthKey]postingListAggregate
	topContentByValueBytes    topPostingListHeap
	termFrequencyByField      map[string]TermFrequencyStats
}

var current atomic.Pointer[Profiler]

func Start() *Profiler {
	p := &Profiler{
		start:                     time.Now(),
		phases:                    make(map[Phase]phaseStats),
		counters:                  make(map[Counter]int64),
		postingListsByCardinality: make(map[postingListBucketKey]postingListAggregate),
		postingListsByNgramLength: make(map[postingListLengthKey]postingListAggregate),
		termFrequencyByField:      make(map[string]TermFrequencyStats),
		topContentByValueBytes: topPostingListHeap{
			less: lessByValueBytes,
		},
	}
	current.Store(p)
	return p
}

func Stop() *Profiler {
	return current.Swap(nil)
}

func Current() *Profiler {
	return current.Load()
}

func Record(phase Phase, duration time.Duration) {
	if p := Current(); p != nil {
		p.Record(phase, duration)
	}
}

func RecordN(phase Phase, count int64, duration time.Duration) {
	if p := Current(); p != nil {
		p.RecordN(phase, count, duration)
	}
}

func Add(counter Counter, value int64) {
	if p := Current(); p != nil {
		p.Add(counter, value)
	}
}

func RecordPostingList(field, ngram string, cardinality uint64, keyBytes, valueBytes int64) {
	if p := Current(); p != nil {
		p.RecordPostingList(field, ngram, cardinality, keyBytes, valueBytes)
	}
}

func RecordTermFrequencyStats(field string, stats TermFrequencyStats) {
	if p := Current(); p != nil {
		p.RecordTermFrequencyStats(field, stats)
	}
}

var noopTimer = func() {}

// Timer starts a stopwatch and returns a function that records the elapsed
// time against phase. When profiling is off, the returned function is a shared
// no-op so callers can use `defer indexprofile.Timer(phase)()` without
// allocating per call.
func Timer(phase Phase) func() {
	p := Current()
	if p == nil {
		return noopTimer
	}
	start := time.Now()
	return func() {
		p.Record(phase, time.Since(start))
	}
}

func (p *Profiler) Record(phase Phase, duration time.Duration) {
	p.RecordN(phase, 1, duration)
}

func (p *Profiler) RecordN(phase Phase, count int64, duration time.Duration) {
	if count == 0 {
		return
	}
	p.mu.Lock()
	stats := p.phases[phase]
	stats.count += count
	stats.total += duration
	p.phases[phase] = stats
	p.mu.Unlock()
}

func (p *Profiler) Add(counter Counter, value int64) {
	p.mu.Lock()
	p.counters[counter] += value
	p.mu.Unlock()
}

func (p *Profiler) Get(counter Counter) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counters[counter]
}

// Now returns time.Now() when profiling is enabled, otherwise the zero Time.
// Safe to call on a nil receiver — paired with Since this lets hot loops skip
// time.Now() when profiling is off without an explicit nil check at each site.
func (p *Profiler) Now() time.Time {
	if p == nil {
		return time.Time{}
	}
	return time.Now()
}

// Since returns time.Since(start) when profiling is enabled, otherwise 0.
// Safe to call on a nil receiver. See Now.
func (p *Profiler) Since(start time.Time) time.Duration {
	if p == nil {
		return 0
	}
	return time.Since(start)
}

func (p *Profiler) RecordPostingList(field, ngram string, cardinality uint64, keyBytes, valueBytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	agg := postingListAggregate{
		lists:      1,
		postings:   int64(cardinality),
		keyBytes:   keyBytes,
		valueBytes: valueBytes,
	}

	bucket := postingListCardinalityBucket(cardinality)
	bucket.field = field
	addPostingListAggregate(p.postingListsByCardinality, bucket, agg)
	addPostingListAggregate(p.postingListsByNgramLength, postingListLengthKey{
		field:  field,
		length: len(ngram),
	}, agg)

	if field == "content" {
		entry := postingListTopEntry{
			field:       field,
			ngram:       ngram,
			cardinality: cardinality,
			keyBytes:    keyBytes,
			valueBytes:  valueBytes,
		}
		maybeAddTopPostingList(&p.topContentByValueBytes, entry)
	}
}

func (p *Profiler) RecordTermFrequencyStats(field string, stats TermFrequencyStats) {
	p.mu.Lock()
	defer p.mu.Unlock()

	current := p.termFrequencyByField[field]
	current.Occurrences += stats.Occurrences
	current.UniquePostings += stats.UniquePostings
	current.DuplicateOccurrences += stats.DuplicateOccurrences
	current.DuplicatePostings += stats.DuplicatePostings
	current.RLEBytesEstimate += stats.RLEBytesEstimate
	for i := range stats.CountsByLog2Bucket {
		current.CountsByLog2Bucket[i] += stats.CountsByLog2Bucket[i]
	}
	p.termFrequencyByField[field] = current

	p.counters[CounterTFNgramOccurrences] += stats.Occurrences
	p.counters[CounterTFUniquePostings] += stats.UniquePostings
	p.counters[CounterTFDuplicateOccurrences] += stats.DuplicateOccurrences
	p.counters[CounterTFPostingsWithFreqGT1] += stats.DuplicatePostings
	p.counters[CounterTFRLEBytes] += stats.RLEBytesEstimate
}

func addPostingListAggregate[K comparable](m map[K]postingListAggregate, key K, value postingListAggregate) {
	agg := m[key]
	agg.lists += value.lists
	agg.postings += value.postings
	agg.keyBytes += value.keyBytes
	agg.valueBytes += value.valueBytes
	m[key] = agg
}

func maybeAddTopPostingList(h *topPostingListHeap, entry postingListTopEntry) {
	const maxTopPostingLists = 100
	if h.Len() < maxTopPostingLists {
		heap.Push(h, entry)
		return
	}
	if h.less(h.entries[0], entry) {
		h.entries[0] = entry
		heap.Fix(h, 0)
	}
}

func (p *Profiler) PrettyPrint() {
	type phaseRow struct {
		phase Phase
		stats phaseStats
	}
	type counterRow struct {
		counter Counter
		value   int64
	}
	type cardinalityRow struct {
		key   postingListBucketKey
		stats postingListAggregate
	}
	type lengthRow struct {
		key   postingListLengthKey
		stats postingListAggregate
	}
	type termFrequencyRow struct {
		field string
		stats TermFrequencyStats
	}

	p.mu.Lock()
	phases := make([]phaseRow, 0, len(p.phases))
	for phase, stats := range p.phases {
		phases = append(phases, phaseRow{phase: phase, stats: stats})
	}
	counters := make([]counterRow, 0, len(p.counters))
	for counter, value := range p.counters {
		counters = append(counters, counterRow{counter: counter, value: value})
	}
	cardinalityRows := make([]cardinalityRow, 0, len(p.postingListsByCardinality))
	for key, stats := range p.postingListsByCardinality {
		cardinalityRows = append(cardinalityRows, cardinalityRow{key: key, stats: stats})
	}
	lengthRows := make([]lengthRow, 0, len(p.postingListsByNgramLength))
	for key, stats := range p.postingListsByNgramLength {
		lengthRows = append(lengthRows, lengthRow{key: key, stats: stats})
	}
	termFrequencyRows := make([]termFrequencyRow, 0, len(p.termFrequencyByField))
	for field, stats := range p.termFrequencyByField {
		termFrequencyRows = append(termFrequencyRows, termFrequencyRow{field: field, stats: stats})
	}
	topByValueBytes := append([]postingListTopEntry(nil), p.topContentByValueBytes.entries...)
	p.mu.Unlock()

	sort.Slice(phases, func(i, j int) bool {
		if phases[i].stats.total == phases[j].stats.total {
			return phases[i].phase < phases[j].phase
		}
		return phases[i].stats.total > phases[j].stats.total
	})
	sort.Slice(counters, func(i, j int) bool {
		return counters[i].counter < counters[j].counter
	})
	sort.Slice(cardinalityRows, func(i, j int) bool {
		if cardinalityRows[i].key.field != cardinalityRows[j].key.field {
			return cardinalityRows[i].key.field < cardinalityRows[j].key.field
		}
		return cardinalityRows[i].key.order < cardinalityRows[j].key.order
	})
	sort.Slice(lengthRows, func(i, j int) bool {
		if lengthRows[i].key.field != lengthRows[j].key.field {
			return lengthRows[i].key.field < lengthRows[j].key.field
		}
		return lengthRows[i].key.length < lengthRows[j].key.length
	})
	sort.Slice(termFrequencyRows, func(i, j int) bool {
		return termFrequencyRows[i].field < termFrequencyRows[j].field
	})
	sortPostingListTop(topByValueBytes, lessByValueBytes)

	log.Printf("Index profile elapsed=%s", time.Since(p.start).Round(time.Millisecond))
	log.Printf("Index profile phases (some rows are inclusive parent phases):")
	for _, row := range phases {
		avg := time.Duration(0)
		if row.stats.count > 0 {
			avg = row.stats.total / time.Duration(row.stats.count)
		}
		log.Printf("  %-24s count=%-10d total=%-12s avg=%s", row.phase, row.stats.count, formatDuration(row.stats.total), formatDuration(avg))
	}

	log.Printf("Index profile counters:")
	for _, row := range counters {
		log.Printf("  %-32s %d", row.counter, row.value)
	}

	if len(cardinalityRows) > 0 {
		log.Printf("Index profile posting list cardinality buckets:")
		for _, row := range cardinalityRows {
			log.Printf("  field=%-12q bucket=%-12s lists=%-10d postings=%-12d key_bytes=%-12d value_bytes=%d",
				row.key.field, row.key.label, row.stats.lists, row.stats.postings, row.stats.keyBytes, row.stats.valueBytes)
		}
	}
	if len(lengthRows) > 0 {
		log.Printf("Index profile posting list ngram length buckets:")
		for _, row := range lengthRows {
			log.Printf("  field=%-12q length=%-3d lists=%-10d postings=%-12d key_bytes=%-12d value_bytes=%d",
				row.key.field, row.key.length, row.stats.lists, row.stats.postings, row.stats.keyBytes, row.stats.valueBytes)
		}
	}
	if len(termFrequencyRows) > 0 {
		log.Printf("Index profile term frequency storage estimate:")
		for _, row := range termFrequencyRows {
			s := row.stats
			duplicatePercent := float64(0)
			if s.UniquePostings > 0 {
				duplicatePercent = 100 * float64(s.DuplicatePostings) / float64(s.UniquePostings)
			}
			log.Printf("  field=%-12q occurrences=%-12d unique_postings=%-12d duplicate_occurrences=%-12d postings_with_tf_gt_1=%-12d (%.2f%%) rle_bytes_estimate=%d",
				row.field, s.Occurrences, s.UniquePostings, s.DuplicateOccurrences, s.DuplicatePostings, duplicatePercent, s.RLEBytesEstimate)
			var sb strings.Builder
			sb.WriteString("    tf_buckets:")
			for i, count := range s.CountsByLog2Bucket {
				if count == 0 {
					continue
				}
				fmt.Fprintf(&sb, " %s=%d", tfLog2BucketLabel(i), count)
			}
			log.Print(sb.String())
		}
	}
	printTopPostingLists("Index profile top content posting lists by value bytes:", topByValueBytes)
}

func formatDuration(d time.Duration) time.Duration {
	if d == 0 {
		return 0
	}
	if d < time.Microsecond {
		return d.Round(time.Nanosecond)
	}
	return d.Round(time.Microsecond)
}

func postingListCardinalityBucket(cardinality uint64) postingListBucketKey {
	switch {
	case cardinality == 0:
		return postingListBucketKey{label: "0", order: 0}
	case cardinality == 1:
		return postingListBucketKey{label: "1", order: 1}
	case cardinality == 2:
		return postingListBucketKey{label: "2", order: 2}
	case cardinality <= 4:
		return postingListBucketKey{label: "3-4", order: 3}
	case cardinality <= 8:
		return postingListBucketKey{label: "5-8", order: 4}
	case cardinality <= 16:
		return postingListBucketKey{label: "9-16", order: 5}
	case cardinality <= 32:
		return postingListBucketKey{label: "17-32", order: 6}
	case cardinality <= 64:
		return postingListBucketKey{label: "33-64", order: 7}
	case cardinality <= 128:
		return postingListBucketKey{label: "65-128", order: 8}
	case cardinality <= 256:
		return postingListBucketKey{label: "129-256", order: 9}
	case cardinality <= 512:
		return postingListBucketKey{label: "257-512", order: 10}
	case cardinality <= 1024:
		return postingListBucketKey{label: "513-1K", order: 11}
	case cardinality <= 2048:
		return postingListBucketKey{label: "1K-2K", order: 12}
	case cardinality <= 4096:
		return postingListBucketKey{label: "2K-4K", order: 13}
	case cardinality <= 8192:
		return postingListBucketKey{label: "4K-8K", order: 14}
	case cardinality <= 16384:
		return postingListBucketKey{label: "8K-16K", order: 15}
	case cardinality <= 32768:
		return postingListBucketKey{label: "16K-32K", order: 16}
	case cardinality <= 65536:
		return postingListBucketKey{label: "32K-64K", order: 17}
	default:
		return postingListBucketKey{label: "64K+", order: 18}
	}
}

func lessByValueBytes(a, b postingListTopEntry) bool {
	if a.valueBytes != b.valueBytes {
		return a.valueBytes < b.valueBytes
	}
	if a.cardinality != b.cardinality {
		return a.cardinality < b.cardinality
	}
	return a.ngram > b.ngram
}

func sortPostingListTop(entries []postingListTopEntry, less func(a, b postingListTopEntry) bool) {
	sort.Slice(entries, func(i, j int) bool {
		return less(entries[j], entries[i])
	})
}

func printTopPostingLists(header string, entries []postingListTopEntry) {
	if len(entries) == 0 {
		return
	}
	log.Printf("%s", header)
	for i, entry := range entries {
		log.Printf("  #%03d ngram=%-12s docs=%-10d key_bytes=%-8d value_bytes=%d",
			i+1, formatNgram(entry.ngram), entry.cardinality, entry.keyBytes, entry.valueBytes)
	}
}

func formatNgram(ngram string) string {
	const maxNgramPrintBytes = 24
	if len(ngram) > maxNgramPrintBytes {
		ngram = ngram[:maxNgramPrintBytes] + "..."
	}
	return strconv.QuoteToASCII(ngram)
}
