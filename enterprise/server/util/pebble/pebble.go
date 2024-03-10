package pebble

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
)

var (
	warnAboutLeaks = flag.Bool("cache.pebble.warn_about_leaks", true, "If set, warn about leaked DB handles")
)

var NoSync = pebble.NoSync
var Sync = pebble.Sync
var ErrNotFound = pebble.ErrNotFound

var NewCache = pebble.NewCache
var WithFlushedWAL = pebble.WithFlushedWAL
var Peek = pebble.Peek
var DefaultFS = vfs.Default

type Options = pebble.Options
type IterOptions = pebble.IterOptions
type Snapshot = pebble.Snapshot
type Metrics = pebble.Metrics
type EventListener = pebble.EventListener
type WriteStallBeginInfo = pebble.WriteStallBeginInfo
type DiskSlowInfo = pebble.DiskSlowInfo
type DBDesc = pebble.DBDesc

type FS = vfs.FS

type Iterator interface {
	io.Closer

	// First moves the iterator the the first key/value pair. Returns true if the
	// iterator is pointing at a valid entry and false otherwise.
	First() bool

	// Valid returns true if the iterator is positioned at a valid key/value pair
	// and false otherwise.
	Valid() bool

	// Next moves the iterator to the next key/value pair. Returns true if the
	// iterator is pointing at a valid entry and false otherwise.
	Next() bool

	// SeekGE moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key. Returns true if the iterator is pointing at
	// a valid entry and false otherwise.
	SeekGE(key []byte) bool

	// SeekLT moves the iterator to the last key/value pair whose key is less than
	// the given key. Returns true if the iterator is pointing at a valid entry and
	// false otherwise.
	SeekLT(key []byte) bool

	// Key returns the key of the current key/value pair, or nil if done. The
	// caller should not modify the contents of the returned slice, and its
	// contents may change on the next call to Next.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done. The
	// caller should not modify the contents of the returned slice, and its
	// contents may change on the next call to Next.
	Value() []byte
}

type Reader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but it is
	// safe to modify the contents of the argument after Get returns. The
	// returned slice will remain valid until the returned Closer is closed. On
	// success, the caller MUST call closer.Close() or a memory leak will occur.
	Get(key []byte) (value []byte, closer io.Closer, err error)

	// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
	// return false). The iterator can be positioned via a call to SeekGE,
	// SeekLT, First or Last.
	NewIter(o *pebble.IterOptions) Iterator
}

type Writer interface {
	// Apply the operations contained in the batch to the DB.
	//
	// It is safe to modify the contents of the arguments after Apply returns.
	Apply(batch Batch, o *pebble.WriteOptions) error

	// Set sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	//
	// It is safe to modify the contents of the arguments after Set returns.
	Set(key, value []byte, o *pebble.WriteOptions) error

	// Delete deletes the value for the given key. Deletes are blind all will
	// succeed even if the given key does not exist.
	//
	// It is safe to modify the contents of the arguments after Delete returns.
	Delete(key []byte, o *pebble.WriteOptions) error

	// DeleteRange deletes all of the point keys (and values) in the range
	// [start,end) (inclusive on start, exclusive on end). DeleteRange does NOT
	// delete overlapping range keys (eg, keys set via RangeKeySet).
	//
	// It is safe to modify the contents of the arguments after DeleteRange
	// returns.
	DeleteRange(start, end []byte, o *pebble.WriteOptions) error

	// LogData adds the specified to the batch. The data will be written to the
	// WAL, but not added to memtables or sstables. Log data is never indexed,
	// which makes it useful for testing WAL performance.
	//
	// It is safe to modify the contents of the argument after LogData returns.
	LogData(data []byte, opts *pebble.WriteOptions) error
}

type Batch interface {
	io.Closer
	Reader
	Writer

	// Commit applies the batch to its parent writer.
	Commit(o *pebble.WriteOptions) error

	// Count returns the count of memtable-modifying operations in this batch. All
	// operations with the except of LogData increment this count.
	Count() uint32

	// Empty returns true if the batch is empty, and false otherwise.
	Empty() bool

	// Returns the current size of the batch.
	Len() int

	Reader() pebble.BatchReader

	Reset()
}

// IPebbleDB is an interface the covers the methods on a pebble.DB used by our
// code. An interface is required so that the DB leaser can return a pebble.DB
// embedded in a struct that implements this interface.
type IPebbleDB interface {
	Reader
	Writer
	io.Closer

	EstimateDiskUsage(start, end []byte) (uint64, error)
	Flush() error
	Metrics() *pebble.Metrics
	NewBatch() Batch
	NewIndexedBatch() Batch
	NewSnapshot() *pebble.Snapshot

	// Compact the specified range of keys in the database.
	Compact(start, end []byte, parallelize bool) error

	// Checkpoint constructs a snapshot of the DB instance in the specified
	// directory. The WAL, MANIFEST, OPTIONS, and sstables will be copied into the
	// snapshot. Hard links will be used when possible. Beware of the significant
	// space overhead for a checkpoint if hard links are disabled. Also beware that
	// even if hard links are used, the space overhead for the checkpoint will
	// increase over time as the DB performs compactions.
	Checkpoint(destDir string, opts ...pebble.CheckpointOption) error
}

type instrumentedIter struct {
	db *instrumentedDB

	iter *pebble.Iterator
}

func (i *instrumentedIter) Close() error {
	return i.iter.Close()
}

func (i *instrumentedIter) First() bool {
	t := i.db.iterFirstMetrics.Track()
	defer t.Done()
	return i.iter.First()
}

func (i *instrumentedIter) Valid() bool {
	return i.iter.Valid()
}

func (i *instrumentedIter) Next() bool {
	t := i.db.iterNextMetrics.Track()
	defer t.Done()
	return i.iter.Next()
}

func (i *instrumentedIter) SeekGE(key []byte) bool {
	t := i.db.iterSeekGEMetrics.Track()
	defer t.Done()
	return i.iter.SeekGE(key)
}

func (i *instrumentedIter) SeekLT(key []byte) bool {
	t := i.db.iterSeekLTMetrics.Track()
	defer t.Done()
	return i.iter.SeekLT(key)
}

func (i *instrumentedIter) Key() []byte {
	return i.iter.Key()
}

func (i *instrumentedIter) Value() []byte {
	return i.iter.Value()
}

type instrumentedBatch struct {
	pebble.Writer

	batch *pebble.Batch
	db    *instrumentedDB
}

func (ib *instrumentedBatch) Close() error {
	return ib.batch.Close()
}

func (ib *instrumentedBatch) Get(key []byte) (value []byte, closer io.Closer, err error) {
	t := ib.db.batchGetMetrics.Track()
	defer t.Done()
	return ib.batch.Get(key)
}

func (ib *instrumentedBatch) Commit(o *pebble.WriteOptions) error {
	t := ib.db.batchCommitMetrics.Track()
	defer t.Done()
	return ib.batch.Commit(o)
}

func (ib *instrumentedBatch) Count() uint32 {
	return ib.batch.Count()
}

func (ib *instrumentedBatch) Empty() bool {
	return ib.batch.Empty()
}

func (ib *instrumentedBatch) Len() int {
	return ib.batch.Len()
}

func (ib *instrumentedBatch) NewIter(o *pebble.IterOptions) Iterator {
	iter := ib.batch.NewIter(o)
	return &instrumentedIter{ib.db, iter}
}

func (ib *instrumentedBatch) Apply(batch Batch, opts *pebble.WriteOptions) error {
	return ib.batch.Apply(batch.(*instrumentedBatch).batch, opts)
}

func (ib *instrumentedBatch) Reader() pebble.BatchReader {
	return ib.batch.Reader()
}
func (ib *instrumentedBatch) Reset() {
	ib.batch.Reset()
}

type opMetrics struct {
	count prometheus.Counter
	hist  prometheus.Observer
}

type opTracker struct {
	hist  prometheus.Observer
	start time.Time
}

func (ot *opTracker) Done() {
	ot.hist.Observe(float64(time.Since(ot.start).Microseconds()))
}

func (om *opMetrics) Track() opTracker {
	om.count.Inc()
	return opTracker{hist: om.hist, start: time.Now()}
}

type instrumentedDB struct {
	db *pebble.DB

	iterFirstMetrics  *opMetrics
	iterNextMetrics   *opMetrics
	iterSeekGEMetrics *opMetrics
	iterSeekLTMetrics *opMetrics

	dbApplyMetrics       *opMetrics
	dbGetMetrics         *opMetrics
	dbSetMetrics         *opMetrics
	dbDeleteMetrics      *opMetrics
	dbDeleteRangeMetrics *opMetrics
	dbLogDataMetrics     *opMetrics
	dbFlushMetrics       *opMetrics
	dbCheckpointMetrics  *opMetrics

	batchGetMetrics    *opMetrics
	batchCommitMetrics *opMetrics
}

func (idb *instrumentedDB) Get(key []byte) (value []byte, closer io.Closer, err error) {
	t := idb.dbGetMetrics.Track()
	defer t.Done()
	return idb.db.Get(key)
}

func (idb *instrumentedDB) Set(key, value []byte, o *pebble.WriteOptions) error {
	t := idb.dbSetMetrics.Track()
	defer t.Done()
	return idb.db.Set(key, value, o)
}

func (idb *instrumentedDB) Delete(key []byte, o *pebble.WriteOptions) error {
	t := idb.dbDeleteMetrics.Track()
	defer t.Done()
	return idb.db.Delete(key, o)
}

func (idb *instrumentedDB) DeleteRange(start, end []byte, o *pebble.WriteOptions) error {
	t := idb.dbDeleteRangeMetrics.Track()
	defer t.Done()
	return idb.db.DeleteRange(start, end, o)
}

func (idb *instrumentedDB) LogData(data []byte, opts *pebble.WriteOptions) error {
	t := idb.dbLogDataMetrics.Track()
	defer t.Done()
	return idb.db.LogData(data, opts)
}

func (idb *instrumentedDB) Close() error {
	return idb.db.Close()
}

func (idb *instrumentedDB) EstimateDiskUsage(start, end []byte) (uint64, error) {
	return idb.db.EstimateDiskUsage(start, end)
}

func (idb *instrumentedDB) Flush() error {
	t := idb.dbFlushMetrics.Track()
	defer t.Done()
	return idb.db.Flush()
}

func (idb *instrumentedDB) Metrics() *pebble.Metrics {
	return idb.db.Metrics()
}

func (idb *instrumentedDB) NewSnapshot() *pebble.Snapshot {
	return idb.db.NewSnapshot()
}

func (idb *instrumentedDB) Compact(start, end []byte, parallelize bool) error {
	return idb.db.Compact(start, end, parallelize)
}

func (idb *instrumentedDB) Checkpoint(destDir string, opts ...pebble.CheckpointOption) error {
	t := idb.dbCheckpointMetrics.Track()
	defer t.Done()
	return idb.db.Checkpoint(destDir, opts...)
}

func (idb *instrumentedDB) Apply(batch Batch, opts *pebble.WriteOptions) error {
	t := idb.dbApplyMetrics.Track()
	defer t.Done()
	return idb.db.Apply(batch.(*instrumentedBatch).batch, opts)
}

func (idb *instrumentedDB) NewIter(o *pebble.IterOptions) Iterator {
	iter := idb.db.NewIter(o)
	return &instrumentedIter{idb, iter}
}

func (idb *instrumentedDB) NewBatch() Batch {
	batch := idb.db.NewBatch()
	return &instrumentedBatch{batch, batch, idb}
}

func (idb *instrumentedDB) NewIndexedBatch() Batch {
	batch := idb.db.NewIndexedBatch()
	return &instrumentedBatch{batch, batch, idb}
}

func Open(dbDir string, id string, options *pebble.Options) (IPebbleDB, error) {
	db, err := pebble.Open(dbDir, options)
	if err != nil {
		return nil, err
	}

	opMetrics := func(op string) *opMetrics {
		metricsLabels := prometheus.Labels{
			metrics.PebbleOperation: op,
			metrics.PebbleID:        id,
		}
		return &opMetrics{
			count: metrics.PebbleCachePebbleOpCount.With(metricsLabels),
			hist:  metrics.PebbleCachePebbleOpLatencyUsec.With(metricsLabels),
		}
	}
	idb := &instrumentedDB{
		db:                   db,
		iterFirstMetrics:     opMetrics("iter_first"),
		iterNextMetrics:      opMetrics("iter_next"),
		iterSeekGEMetrics:    opMetrics("iter_seek_ge"),
		iterSeekLTMetrics:    opMetrics("iter_seek_lt"),
		dbApplyMetrics:       opMetrics("apply"),
		dbGetMetrics:         opMetrics("get"),
		dbSetMetrics:         opMetrics("set"),
		dbDeleteMetrics:      opMetrics("delete"),
		dbDeleteRangeMetrics: opMetrics("delete_range"),
		dbLogDataMetrics:     opMetrics("log_data"),
		dbFlushMetrics:       opMetrics("flush"),
		dbCheckpointMetrics:  opMetrics("checkpoint"),
		batchGetMetrics:      opMetrics("batch_get"),
		batchCommitMetrics:   opMetrics("batch_commit"),
	}
	return idb, err
}

// Leaser is an interface implemented by the leaser that allows clients to get
// a leased pebble DB or close the leaser.
type Leaser interface {
	DB() (IPebbleDB, error)
	Close()
}

type leaser struct {
	db       IPebbleDB
	waiters  sync.WaitGroup
	closedMu sync.Mutex // PROTECTS(closed)
	closed   bool
}

// NewDBLeaser returns a new DB leaser that wraps db and serializes access to
// it.
//
// Clients can call the leaser's DB() method to get a handle to the wrapped
// pebble DB. Clients are then required call *Close* on this handle when done
// to prevent leaks. The DB cannot be closed until all handles are returned.
//
// Once the DB leaser has been closed with Close(), no new handles can be
// acquired, instead an error is returned.
func NewDBLeaser(db IPebbleDB) Leaser {
	return &leaser{
		db:       db,
		waiters:  sync.WaitGroup{},
		closedMu: sync.Mutex{},
		closed:   false,
	}
}

func (l *leaser) Close() {
	l.closedMu.Lock()
	defer l.closedMu.Unlock()
	if l.closed {
		return
	}
	l.closed = true

	// wait for all db users to finish up.
	l.waiters.Wait()
}

func (l *leaser) DB() (IPebbleDB, error) {
	l.closedMu.Lock()
	defer l.closedMu.Unlock()
	if l.closed {
		return nil, status.FailedPreconditionError("db is closed")
	}

	handle := &refCountedDB{
		l.db,
		newRefCounter(&l.waiters),
	}

	if *warnAboutLeaks {
		location := "unknown"
		if _, file, no, ok := runtime.Caller(1); ok {
			location = fmt.Sprintf("%s:%d", file, no)
		}
		runtime.SetFinalizer(handle, func(h *refCountedDB) {
			if !h.refCounter.closed {
				alert.UnexpectedEvent("pebble_db_handle_leak", "DB() handle leak at %s!", location)
			}
		})
	}
	return handle, nil
}

type refCounter struct {
	wg     *sync.WaitGroup
	closed bool
}

func newRefCounter(wg *sync.WaitGroup) *refCounter {
	wg.Add(1)
	return &refCounter{
		wg:     wg,
		closed: false,
	}
}

func (r *refCounter) Close() error {
	if !r.closed {
		r.closed = true
		r.wg.Add(-1)
	}
	return nil
}

type refCountedDB struct {
	IPebbleDB
	*refCounter
}

func (r *refCountedDB) Close() error {
	// Just close the refcounter, not the DB.
	return r.refCounter.Close()
}

type fnReadCloser struct {
	io.ReadCloser
	closeFn func() error
}

func ReadCloserWithFunc(rc io.ReadCloser, closeFn func() error) io.ReadCloser {
	return &fnReadCloser{rc, closeFn}
}
func (f fnReadCloser) Close() error {
	err := f.ReadCloser.Close()
	closeFnErr := f.closeFn()

	if err == nil && closeFnErr != nil {
		return closeFnErr
	}
	return err
}

type writeCloser struct {
	interfaces.MetadataWriteCloser
	commitFn     func(n int64) error
	bytesWritten int64
	closeFn      func() error
}

func CommittedWriterWithFunc(wcm interfaces.MetadataWriteCloser, commitFn func(n int64) error, closeFn func() error) interfaces.CommittedMetadataWriteCloser {
	return &writeCloser{wcm, commitFn, 0, closeFn}
}

func (dc *writeCloser) Commit() error {
	if err := dc.MetadataWriteCloser.Close(); err != nil {
		return err
	}
	return dc.commitFn(dc.bytesWritten)
}

func (dc *writeCloser) Close() error {
	return dc.closeFn()
}

func (dc *writeCloser) Write(p []byte) (int, error) {
	n, err := dc.MetadataWriteCloser.Write(p)
	if err != nil {
		return 0, err
	}
	dc.bytesWritten += int64(n)
	return n, nil
}

func GetCopy(b Reader, key []byte) ([]byte, error) {
	buf, closer, err := b.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, status.NotFoundErrorf("key %q not found", key)
		}
		return nil, err
	}
	defer closer.Close()
	if len(buf) == 0 {
		return nil, status.NotFoundErrorf("key %q not found (empty value)", key)
	}

	// We need to copy the value before closer is closed.
	val := make([]byte, len(buf))
	copy(val, buf)
	return val, nil
}

func GetProto(b Reader, key []byte, pb proto.Message) error {
	buf, closer, err := b.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return status.NotFoundErrorf("key %q not found", key)
		}
		return err
	}
	defer closer.Close()
	if len(buf) == 0 {
		return status.NotFoundErrorf("key %q not found (empty value)", key)
	}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return status.InternalErrorf("error parsing value for %q: %s", key, err)
	}
	return nil
}

func LookupProto(iter Iterator, key []byte, pb proto.Message) error {
	if !iter.SeekGE(key) || !bytes.Equal(iter.Key(), key) {
		return status.NotFoundErrorf("key %q not found", key)
	}
	if err := proto.Unmarshal(iter.Value(), pb); err != nil {
		return status.InternalErrorf("error parsing value for %q: %s", key, err)
	}
	return nil
}
