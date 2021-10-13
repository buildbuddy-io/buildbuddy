package exampled

import (
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

// pebbledb is a wrapper to ensure lookup() and close() can be concurrently
// invoked. IOnDiskStateMachine.Update() and close() will never be concurrently
// invoked.
type pebbledb struct {
	mu     sync.RWMutex
	db     *pebble.DB
	ro     *pebble.IterOptions
	wo     *pebble.WriteOptions
	syncwo *pebble.WriteOptions
	closed bool
}

func (r *pebbledb) lookup(query []byte) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, status.FailedPreconditionError("db already closed")
	}

	buf, closer, err := r.db.Get(query)
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	if len(buf) == 0 {
		return nil, nil
	}

	// We need to copy the value from pebble before
	// closer is closed.
	val := make([]byte, len(buf))
	copy(val, buf)
	return val, nil
}

func (r *pebbledb) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.db != nil {
		r.db.Close()
	}
}

// createDB creates a PebbleDB DB in the specified directory.
func createDB(dbdir string) (*pebbledb, error) {
	if err := os.MkdirAll(dbdir, 0755); err != nil {
		return nil, err
	}
	db, err := pebble.Open(dbdir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &pebbledb{
		db: db,
	}, nil
}

// DiskKV is a state machine that implements the IOnDiskStateMachine interface.
// DiskKV stores key-value pairs in the underlying PebbleDB key-value store. As
// it is used as an example, it is implemented using the most basic features
// common in most key-value stores. This is NOT a benchmark program.
type DiskKV struct {
	pebbleDB    *pebble.DB
	clusterID   uint64
	nodeID      uint64
	lastApplied uint64
	closed      bool
	aborted     bool
}

// NewDiskKV creates a new disk kv test state machine.
func NewDiskKV(clusterID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	d := &DiskKV{
		clusterID: clusterID,
		nodeID:    nodeID,
	}
	return d
}

func getLastIndex(db *pebbledb) (uint64, error) {
	val, err := db.lookup([]byte(appliedIndexKey))
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(val), nil
}

// Open opens the state machine and return the index of the last Raft Log entry
// already updated into the state machine.
func (d *DiskKV) Open(stopc <-chan struct{}) (uint64, error) {
	db, err := createDB(dbdir)
	if err != nil {
		return 0, err
	}
	appliedIndex, err := getLastIndex(db)
	if err != nil {
		return 0, err
	}
	d.lastApplied = appliedIndex
	return appliedIndex, nil
}

// Lookup queries the state machine.
func (d *DiskKV) Lookup(key interface{}) (interface{}, error) {
	kb, err := key.([]byte)
	if err != nil {
		return nil, status.FailedPreconditionError("Cannot convert key to []byte")
	}
	val, err := d.pebbleDB.lookup(kb)
	if status.IsNotFoundError(err) {
		return nil, nil
	}
	return val, err
}

// Update updates the state machine. In this example, all updates are put into
// a PebbleDB write batch and then atomically written to the DB together with
// the index of the last Raft Log entry. For simplicity, we always Sync the
// writes (db.wo.Sync=True). To get higher throughput, you can implement the
// Sync() method below and choose not to synchronize for every Update(). Sync()
// will periodically called by Dragonboat to synchronize the state.
func (d *DiskKV) Update(ents []sm.Entry) ([]sm.Entry, error) {
	if d.aborted {
		return nil, status.FailedPreconditionError("update() called after abort set to true")
	}
	if d.closed {
		return nil, status.FailedPreconditionError("update() called after close")
	}
	wb := d.pebbleDB.NewBatch()
	defer wb.Close()
	for idx, e := range ents {
		dataKV := &KVData{}
		if err := json.Unmarshal(e.Cmd, dataKV); err != nil {
			panic(err)
		}
		wb.Set([]byte(dataKV.Key), []byte(dataKV.Val), db.wo)
		ents[idx].Result = sm.Result{Value: uint64(len(ents[idx].Cmd))}
	}
	// save the applied index to the DB.
	appliedIndex := make([]byte, 8)
	binary.LittleEndian.PutUint64(appliedIndex, ents[len(ents)-1].Index)
	wb.Set([]byte(appliedIndexKey), appliedIndex, db.wo)
	if err := d.pebbleDB.Apply(wb, &pebble.WriteOptions{Sync: true}); err != nil {
		return nil, err
	}
	if d.lastApplied >= ents[len(ents)-1].Index {
		return nil, status.FailedPrecondition("lastApplied not moving forward")
	}
	d.lastApplied = ents[len(ents)-1].Index
	return ents, nil
}

// Sync synchronizes all in-core state of the state machine. Since the Update
// method in this example already does that every time when it is invoked, the
// Sync method here is a NoOP.
func (d *DiskKV) Sync() error {
	return nil
}

type diskKVCtx struct {
	db       *pebbledb
	snapshot *pebble.Snapshot
}

// PrepareSnapshot prepares snapshotting. PrepareSnapshot is responsible to
// capture a state identifier that identifies a point in time state of the
// underlying data. In this example, we use Pebble's snapshot feature to
// achieve that.
func (d *DiskKV) PrepareSnapshot() (interface{}, error) {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	db := (*pebbledb)(atomic.LoadPointer(&d.db))
	return &diskKVCtx{
		db:       db,
		snapshot: db.db.NewSnapshot(),
	}, nil
}

func iteratorIsValid(iter *pebble.Iterator) bool {
	return iter.Valid()
}

// saveToWriter saves all existing key-value pairs to the provided writer.
// As an example, we use the most straight forward way to implement this.
func (d *DiskKV) saveToWriter(db *pebbledb, ss *pebble.Snapshot, w io.Writer) error {
	iter := ss.NewIter(db.ro)
	defer iter.Close()
	values := make([]*KVData, 0)
	for iter.First(); iteratorIsValid(iter); iter.Next() {
		kv := &KVData{
			Key: string(iter.Key()),
			Val: string(iter.Value()),
		}
		values = append(values, kv)
	}
	count := uint64(len(values))
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, count)
	if _, err := w.Write(sz); err != nil {
		return err
	}
	for _, dataKv := range values {
		data, err := json.Marshal(dataKv)
		if err != nil {
			panic(err)
		}
		binary.LittleEndian.PutUint64(sz, uint64(len(data)))
		if _, err := w.Write(sz); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// SaveSnapshot saves the state machine state identified by the state
// identifier provided by the input ctx parameter. Note that SaveSnapshot
// is not suppose to save the latest state.
func (d *DiskKV) SaveSnapshot(ctx interface{},
	w io.Writer, done <-chan struct{}) error {
	if d.closed {
		panic("prepare snapshot called after Close()")
	}
	if d.aborted {
		panic("prepare snapshot called after abort")
	}
	ctxdata := ctx.(*diskKVCtx)
	db := ctxdata.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	ss := ctxdata.snapshot
	defer ss.Close()
	return d.saveToWriter(db, ss, w)
}

// RecoverFromSnapshot recovers the state machine state from snapshot. The
// snapshot is recovered into a new DB first and then atomically swapped with
// the existing DB to complete the recovery.
func (d *DiskKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	if d.closed {
		panic("recover from snapshot called after Close()")
	}
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	dbdir := getNewRandomDBDirName(dir)
	oldDirName, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}
	db, err := createDB(dbdir)
	if err != nil {
		return err
	}
	sz := make([]byte, 8)
	if _, err := io.ReadFull(r, sz); err != nil {
		return err
	}
	total := binary.LittleEndian.Uint64(sz)
	wb := db.db.NewBatch()
	defer wb.Close()
	for i := uint64(0); i < total; i++ {
		if _, err := io.ReadFull(r, sz); err != nil {
			return err
		}
		toRead := binary.LittleEndian.Uint64(sz)
		data := make([]byte, toRead)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		dataKv := &KVData{}
		if err := json.Unmarshal(data, dataKv); err != nil {
			panic(err)
		}
		wb.Set([]byte(dataKv.Key), []byte(dataKv.Val), db.wo)
	}
	if err := db.db.Apply(wb, db.syncwo); err != nil {
		return err
	}
	if err := saveCurrentDBDirName(dir, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(dir); err != nil {
		return err
	}
	newLastApplied, err := d.queryAppliedIndex(db)
	if err != nil {
		panic(err)
	}
	// when d.lastApplied == newLastApplied, it probably means there were some
	// dummy entries or membership change entries as part of the new snapshot
	// that never reached the SM and thus never moved the last applied index
	// in the SM snapshot.
	if d.lastApplied > newLastApplied {
		panic("last applied not moving forward")
	}
	d.lastApplied = newLastApplied
	old := (*pebbledb)(atomic.SwapPointer(&d.db, unsafe.Pointer(db)))
	if old != nil {
		old.close()
	}
	parent := filepath.Dir(oldDirName)
	if err := os.RemoveAll(oldDirName); err != nil {
		return err
	}
	return syncDir(parent)
}

// Close closes the state machine.
func (d *DiskKV) Close() error {
	db := (*pebbledb)(atomic.SwapPointer(&d.db, unsafe.Pointer(nil)))
	if db != nil {
		d.closed = true
		db.close()
	} else {
		if d.closed {
			panic("close called twice")
		}
	}
	return nil
}
