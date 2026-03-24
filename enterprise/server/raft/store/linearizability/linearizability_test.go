package linearizability_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	opWrite = iota
	opRead
)

// opInput represents the input to a linearizable operation.
type opInput struct {
	op    int
	key   string
	value string
}

// opOutput represents the output from a linearizable operation.
type opOutput struct {
	value string
	err   string
}

// kvModel defines a linearizable key-value register model.
// Each key is checked independently via PartitionEvent.
//
// Uses NondeterministicModel because a write that returns an error
// may or may not have been applied by Raft (e.g., the proposer
// timed out but the proposal was already committed). The model
// forks into both possible states and porcupine explores both
// branches.
var kvModel = (&porcupine.NondeterministicModel{
	PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
		// Two-pass: index call events by Id to find the key,
		// then assign return events to the same partition.
		byID := make(map[int]string)
		for _, e := range history {
			if e.Kind == porcupine.CallEvent {
				byID[e.Id] = e.Value.(opInput).key
			}
		}
		m := make(map[string][]porcupine.Event)
		for _, e := range history {
			key := byID[e.Id]
			m[key] = append(m[key], e)
		}
		var result [][]porcupine.Event
		for _, v := range m {
			result = append(result, v)
		}
		return result
	},
	Init: func() []interface{} { return []interface{}{""} },
	Step: func(state, input, output interface{}) []interface{} {
		inp := input.(opInput)
		out := output.(opOutput)
		switch inp.op {
		case opWrite:
			if out.err != "" {
				// Indeterminate write: may or may not
				// have taken effect.
				return []interface{}{state, inp.value}
			}
			return []interface{}{inp.value}
		case opRead:
			if out.err != "" {
				// Failed read: could have seen any
				// state.
				return []interface{}{state}
			}
			if out.value == state.(string) {
				return []interface{}{state}
			}
			return nil // illegal
		}
		return nil
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(opInput)
		out := output.(opOutput)
		if inp.op == opWrite {
			if out.err != "" {
				return fmt.Sprintf("set(%s) err=%s", inp.key, out.err)
			}
			return fmt.Sprintf("set(%s, %s)", inp.key, inp.value)
		}
		if out.err != "" {
			return fmt.Sprintf("get(%s) err=%s", inp.key, out.err)
		}
		return fmt.Sprintf("get(%s) -> %s", inp.key, out.value)
	},
	DescribeState: func(state interface{}) string {
		return fmt.Sprintf("%q", state)
	},
}).ToModel()

// eventLog is a thread-safe log of porcupine events.
type eventLog struct {
	mu     sync.Mutex
	events []porcupine.Event
	nextID atomic.Int64
}

func (el *eventLog) logCall(clientID int, inp opInput) int64 {
	id := el.nextID.Add(1) - 1
	el.mu.Lock()
	defer el.mu.Unlock()
	el.events = append(el.events, porcupine.Event{
		Kind:     porcupine.CallEvent,
		Value:    inp,
		Id:       int(id),
		ClientId: clientID,
	})
	return id
}

func (el *eventLog) logReturn(clientID int, id int64, out opOutput) {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.events = append(el.events, porcupine.Event{
		Kind:     porcupine.ReturnEvent,
		Value:    out,
		Id:       int(id),
		ClientId: clientID,
	})
}

func (el *eventLog) getEvents() []porcupine.Event {
	el.mu.Lock()
	defer el.mu.Unlock()
	out := make([]porcupine.Event, len(el.events))
	copy(out, el.events)
	return out
}

// testKey holds a pre-generated filestore key and its associated file
// record, used to write data in the same format as writeRecord in
// store_test.go.
type testKey struct {
	pebbleKey []byte
	fr        *sgpb.FileRecord
	name      string // short name for porcupine
}

// generateTestKeys creates numKeys file-record-style keys in the
// "default" partition.
func generateTestKeys(t *testing.T, numKeys int) []*testKey {
	fs := filestore.New()
	keys := make([]*testKey, numKeys)
	for i := 0; i < numKeys; i++ {
		r, _ := testdigest.RandomCASResourceBuf(t, 100)
		fr := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   r.GetCacheType(),
				PartitionId: "default",
			},
			Digest:         r.GetDigest(),
			DigestFunction: r.GetDigestFunction(),
		}
		pk, err := fs.PebbleKey(fr)
		require.NoError(t, err)
		keyBytes, err := pk.Bytes(filestore.Version5)
		require.NoError(t, err)
		keys[i] = &testKey{
			pebbleKey: keyBytes,
			fr:        fr,
			name:      fmt.Sprintf("key-%d", i),
		}
	}
	return keys
}

// writeFileRecord writes a value to the given key.
func writeFileRecord(ctx context.Context, store *testutil.TestingStore, tk *testKey, value string) error {
	fs := filestore.New()
	valBytes := []byte(value)
	writeCloser := fs.InlineWriter(ctx, int64(len(valBytes)))
	if _, err := writeCloser.Write(valBytes); err != nil {
		return err
	}
	now := time.Now()
	md := &sgpb.FileMetadata{
		FileRecord:      tk.fr,
		StorageMetadata: writeCloser.Metadata(),
		StoredSizeBytes: int64(len(valBytes)),
		LastModifyUsec:  now.UnixMicro(),
		LastAccessUsec:  now.UnixMicro(),
	}
	protoBytes, err := proto.Marshal(md)
	if err != nil {
		return err
	}
	writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   tk.pebbleKey,
			Value: protoBytes,
		},
	}).ToProto()
	if err != nil {
		return err
	}
	writeRsp, err := store.Sender().SyncPropose(ctx, tk.pebbleKey, writeReq)
	if err != nil {
		return err
	}
	return rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
}

// readFileRecord reads the value from the given key using
// DirectReadRequest.
func readFileRecord(ctx context.Context, store *testutil.TestingStore, tk *testKey) (string, error) {
	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: tk.pebbleKey,
	}).ToProto()
	if err != nil {
		return "", err
	}
	readRsp, err := store.Sender().SyncRead(ctx, tk.pebbleKey, readReq)
	if err != nil {
		return "", err
	}
	batchRsp := rbuilder.NewBatchResponseFromProto(readRsp)
	if err := batchRsp.AnyError(); err != nil {
		return "", err
	}
	resp, err := batchRsp.DirectReadResponse(0)
	if err != nil {
		return "", err
	}
	// Parse FileMetadata to extract the inline data.
	md := &sgpb.FileMetadata{}
	if err := proto.Unmarshal(resp.GetKv().GetValue(), md); err != nil {
		return "", err
	}
	fs := filestore.New()
	rc, err := fs.InlineReader(md.GetStorageMetadata().GetInlineMetadata(), 0, 0)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	buf := make([]byte, md.GetStoredSizeBytes())
	n, err := rc.Read(buf)
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(buf[:n]), nil
}

// opTimeout is the maximum time to wait for a single read/write
// operation before treating it as failed.
const opTimeout = 10 * time.Second

// defaultTestDuration is the default duration for linearizability
// tests. Override with TEST_DURATION env var (e.g. "5m", "30s").
const defaultTestDuration = 3 * time.Minute

func getTestDuration() time.Duration {
	if v := os.Getenv("TEST_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return defaultTestDuration
}

// worker runs read/write operations against the cluster and logs
// events for porcupine.
func worker(ctx context.Context, clientID int, stores []*testutil.TestingStore, keys []*testKey, elog *eventLog, writeCount *atomic.Int64) {
	rng := rand.New(rand.NewSource(int64(clientID) + time.Now().UnixNano()))
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		store := stores[rng.Intn(len(stores))]
		tk := keys[rng.Intn(len(keys))]

		opCtx, cancel := context.WithTimeout(ctx, opTimeout)

		if rng.Float64() < 0.5 {
			val := fmt.Sprintf("v%d", writeCount.Add(1))
			inp := opInput{op: opWrite, key: tk.name, value: val}
			id := elog.logCall(clientID, inp)

			err := writeFileRecord(opCtx, store, tk, val)
			cancel()
			if err != nil {
				elog.logReturn(clientID, id, opOutput{err: err.Error()})
			} else {
				elog.logReturn(clientID, id, opOutput{})
			}
		} else {
			inp := opInput{op: opRead, key: tk.name}
			id := elog.logCall(clientID, inp)

			val, err := readFileRecord(opCtx, store, tk)
			cancel()
			if err != nil {
				elog.logReturn(clientID, id, opOutput{err: err.Error()})
			} else {
				elog.logReturn(clientID, id, opOutput{value: val})
			}
		}
	}
}

// killNemesis kills a single non-first store after a delay to simulate
// a node failure during operations.
func killNemesis(ctx context.Context, stores []*testutil.TestingStore, mu *sync.RWMutex, delay time.Duration, idx int) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}
	mu.Lock()
	defer mu.Unlock()
	log.Infof("killNemesis: killing store %s (index %d)", stores[idx].NHID(), idx)
	stores[idx].Stop()
}

// checkLinearizability runs porcupine on the collected events and
// writes a visualization on failure.
func checkLinearizability(t *testing.T, elog *eventLog, vizPath string) {
	events := elog.getEvents()
	log.Infof("Checking linearizability: %d events", len(events))

	result, info := porcupine.CheckEventsVerbose(kvModel, events, 60*time.Second)
	if result != porcupine.Ok {
		if err := porcupine.VisualizePath(kvModel, info, vizPath); err != nil {
			t.Logf("Failed to write visualization: %v", err)
		} else {
			t.Logf("Visualization written to %s", vizPath)
		}
	}
	require.Equal(t, porcupine.Ok, result, "linearizability violation detected")
}

// TestLinearizabilityUnderSplits verifies that read/write operations
// remain linearizable while ranges are being split.
func TestLinearizabilityUnderSplits(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.target_range_size_bytes", 8000)
	flags.Set(t, "cache.raft.min_replicas_per_range", 3)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "cache.raft.enable_txn_cleanup", true)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.op_timeout", 5*time.Second)

	sf := testutil.NewStoreFactoryWithRootDir(t, t.TempDir())
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	ctx := context.Background()

	stores := []*testutil.TestingStore{s1, s2, s3}
	sf.StartShard(t, ctx, stores...)

	testutil.WaitForRangeLease(t, ctx, stores, 1)
	testutil.WaitForRangeLease(t, ctx, stores, 2)

	const numKeys = 10
	const numWorkers = 5
	testDuration := getTestDuration()

	keys := generateTestKeys(t, numKeys)

	// Seed data so the range has enough data for pebble's
	// EstimateDiskUsage to trigger splits. Sleep to let the
	// cluster fully initialize (gossip, shard readiness).
	leaseHolder := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	time.Sleep(10 * time.Second)
	for i := 0; i < 20; i++ {
		testutil.WriteRecord(ctx, t, leaseHolder, "default", 1000)
	}
	leaseHolder.DB().Flush()
	log.Infof("Seeded 20 records on %s", leaseHolder.NHID())

	elog := &eventLog{}
	var writeCount atomic.Int64

	workerCtx, workerCancel := context.WithTimeout(ctx, testDuration)
	defer workerCancel()

	var wg sync.WaitGroup

	// Periodically flush all stores so pebble's EstimateDiskUsage
	// reflects written data. The driver will detect ranges over the
	// target size and trigger splits automatically.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				for _, s := range stores {
					s.DB().Flush()
				}
			}
		}
	}()

	// Start workers.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(workerCtx, id, stores, keys, elog, &writeCount)
		}(i)
	}

	wg.Wait()

	log.Infof("Collected %d events (%d writes)", len(elog.getEvents()), writeCount.Load())

	// Log split results.
	splitCount := 0
	for rangeID := uint64(3); rangeID <= 20; rangeID++ {
		for _, s := range stores {
			rd := s.GetRange(rangeID)
			if rd != nil {
				splitCount++
				log.Infof("Split detected: range %d [%q, %q)", rangeID, rd.GetStart(), rd.GetEnd())
				break
			}
		}
	}
	log.Infof("Total splits: %d", splitCount)

	checkLinearizability(t, elog, "/tmp/linearizability-splits.html")
}

// TestLinearizabilityUnderUpReplication verifies linearizability while
// a node is killed and the driver up-replicates to restore the
// replica count. Uses 4 nodes: shard starts on s1-s3, s2 is killed,
// and the driver adds a replica on s4.
func TestLinearizabilityUnderUpReplication(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.target_range_size_bytes", 0)
	flags.Set(t, "cache.raft.min_replicas_per_range", 3)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "cache.raft.enable_txn_cleanup", true)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.op_timeout", 5*time.Second)

	sf := testutil.NewStoreFactoryWithRootDir(t, t.TempDir())
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	s4 := sf.NewStore(t)
	ctx := context.Background()

	// Start shard on s1-s3 only. s4 is available via gossip for
	// up-replication after s2 is killed.
	initialStores := []*testutil.TestingStore{s1, s2, s3}
	allStores := []*testutil.TestingStore{s1, s2, s3, s4}
	sf.StartShard(t, ctx, initialStores...)

	testutil.WaitForRangeLease(t, ctx, initialStores, 1)
	testutil.WaitForRangeLease(t, ctx, initialStores, 2)

	const numKeys = 10
	const numWorkers = 5
	testDuration := getTestDuration()

	keys := generateTestKeys(t, numKeys)

	elog := &eventLog{}
	var writeCount atomic.Int64
	var storesMu sync.RWMutex

	workerCtx, workerCancel := context.WithTimeout(ctx, testDuration)
	defer workerCancel()

	var wg sync.WaitGroup

	// Kill s2 after 10s. The driver will detect under-replication
	// (2/3 replicas alive) and add a replica on s4.
	wg.Add(1)
	go func() {
		defer wg.Done()
		killNemesis(workerCtx, allStores, &storesMu, 10*time.Second, 2)
	}()

	// Start workers on all 4 stores. Workers on s2 will start
	// getting errors after the kill; workers on s4 will get errors
	// until it receives a replica.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(workerCtx, id, allStores, keys, elog, &writeCount)
		}(i)
	}

	wg.Wait()

	log.Infof("Collected %d events (%d writes)", len(elog.getEvents()), writeCount.Load())
	checkLinearizability(t, elog, "/tmp/linearizability-up-replication.html")
}
