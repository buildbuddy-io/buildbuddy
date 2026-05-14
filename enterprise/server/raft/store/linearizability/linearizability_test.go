package linearizability_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
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
	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

func init() {
	// Shrink dragonboat transport buffers to make replication lag
	// more likely.
	dragonboat.ApplyMonkeySettings()
}

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
	Init: func() []any { return []any{""} },
	Step: func(state, input, output any) []any {
		inp := input.(opInput)
		out := output.(opOutput)
		switch inp.op {
		case opWrite:
			if out.err != "" {
				// Indeterminate write: may or may not
				// have taken effect.
				return []any{state, inp.value}
			}
			return []any{inp.value}
		case opRead:
			if out.err != "" {
				// Failed read: could have seen any
				// state.
				return []any{state}
			}
			if out.value == state.(string) {
				return []any{state}
			}
			return nil // illegal
		}
		return nil
	},
	DescribeOperation: func(input, output any) string {
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
	DescribeState: func(state any) string {
		return fmt.Sprintf("%q", state)
	},
}).ToModel()

// eventLog is a thread-safe log of porcupine events.
type eventLog struct {
	mu     sync.Mutex
	events []porcupine.Event
	nextID int
}

func (el *eventLog) logCall(clientID int, inp opInput) int {
	el.mu.Lock()
	defer el.mu.Unlock()
	id := el.nextID
	el.nextID++
	el.events = append(el.events, porcupine.Event{
		Kind:     porcupine.CallEvent,
		Value:    inp,
		Id:       id,
		ClientId: clientID,
	})
	return id
}

func (el *eventLog) logReturn(clientID int, id int, value string, err error) {
	out := opOutput{value: value}
	if err != nil {
		out.err = err.Error()
	}
	el.mu.Lock()
	defer el.mu.Unlock()
	el.events = append(el.events, porcupine.Event{
		Kind:     porcupine.ReturnEvent,
		Value:    out,
		Id:       id,
		ClientId: clientID,
	})
}

func (el *eventLog) numEvents() int {
	el.mu.Lock()
	defer el.mu.Unlock()
	return len(el.events)
}

func (el *eventLog) getEvents() []porcupine.Event {
	el.mu.Lock()
	defer el.mu.Unlock()
	return slices.Clone(el.events)
}

// testKey holds a pre-generated filestore key and its associated file
// record.
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

func writeFileRecord(ctx context.Context, store *testutil.TestingStore, tk *testKey, value string) error {
	valBytes := []byte(value)
	now := time.Now()
	md := &sgpb.FileMetadata{
		FileRecord: tk.fr,
		StorageMetadata: &sgpb.StorageMetadata{
			InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
				Data:          valBytes,
				CreatedAtNsec: now.UnixNano(),
			},
		},
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
	md := &sgpb.FileMetadata{}
	if err := proto.Unmarshal(resp.GetKv().GetValue(), md); err != nil {
		return "", err
	}
	return string(md.GetStorageMetadata().GetInlineMetadata().GetData()), nil
}

// opTimeout is the maximum time to wait for a single read/write
// operation before treating it as failed.
const opTimeout = 10 * time.Second

// defaultTestDuration is the default duration for linearizability
// tests. Override with TEST_DURATION env var (e.g. "5m", "30s").
const defaultTestDuration = 3 * time.Minute

func getTestDuration(t *testing.T) time.Duration {
	v := os.Getenv("TEST_DURATION")
	if v == "" {
		t.Logf("TEST_DURATION not set, using default %v", defaultTestDuration)
		return defaultTestDuration
	}
	d, err := time.ParseDuration(v)
	require.NoError(t, err, "invalid TEST_DURATION %q", v)
	t.Logf("TEST_DURATION=%s (parsed as %v)", v, d)
	return d
}

// worker runs read/write operations against the cluster and logs
// events for porcupine.
func worker(ctx context.Context, clientID int, stores []*testutil.TestingStore, keys []*testKey, elog *eventLog, writeCount *atomic.Int64) {
	rng := rand.New(rand.NewSource(int64(clientID) + time.Now().UnixNano()))
	for ctx.Err() == nil {
		store := stores[rng.Intn(len(stores))]
		tk := keys[rng.Intn(len(keys))]

		opCtx, cancel := context.WithTimeout(ctx, opTimeout)

		if rng.Float64() < 0.5 {
			val := fmt.Sprintf("v%d", writeCount.Add(1))
			inp := opInput{op: opWrite, key: tk.name, value: val}
			id := elog.logCall(clientID, inp)
			err := writeFileRecord(opCtx, store, tk, val)
			cancel()
			elog.logReturn(clientID, id, "", err)
		} else {
			inp := opInput{op: opRead, key: tk.name}
			id := elog.logCall(clientID, inp)
			val, err := readFileRecord(opCtx, store, tk)
			cancel()
			elog.logReturn(clientID, id, val, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// killRestartLoop repeatedly kills and restarts a random store.
// Must be called from the test goroutine since RecreateStore uses
// require.
func killRestartLoop(ctx context.Context, t *testing.T, sf *testutil.StoreFactory, stores []*testutil.TestingStore) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Wait before first kill to let the cluster stabilize.
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}
	for ctx.Err() == nil {
		victim := stores[rng.Intn(len(stores))]
		log.Infof("killRestartLoop: killing store %s", victim.NHID())
		victim.Stop()

		// Keep the node dead for a random duration. With
		// dead_store_timeout=1m, longer downtimes may trigger
		// up-replication to another available node.
		downtime := 10*time.Second + time.Duration(rng.Intn(110))*time.Second
		log.Infof("killRestartLoop: store %s will be down for %s", victim.NHID(), downtime)
		select {
		case <-ctx.Done():
			sf.RecreateStore(t, victim)
			return
		case <-time.After(downtime):
		}

		sf.RecreateStore(t, victim)
		log.Infof("killRestartLoop: restarted store %s", victim.NHID())

		// Let the node rejoin before the next kill.
		uptime := 5*time.Second + time.Duration(rng.Intn(30))*time.Second
		select {
		case <-ctx.Done():
			return
		case <-time.After(uptime):
		}
	}
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

// partitionLoop repeatedly partitions and restores a random store's
// Raft transport. While partitioned, all Raft replicas on that node
// fall behind (gossip and gRPC remain active). Safe to call from any
// goroutine since it doesn't use require.
func partitionLoop(ctx context.Context, stores []*testutil.TestingStore) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Wait before first partition to let the cluster stabilize.
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}
	for ctx.Err() == nil {
		victim := stores[rng.Intn(len(stores))]
		nh := victim.NodeHost()

		duration := 5*time.Second + time.Duration(rng.Intn(10))*time.Second
		log.Infof("partitionLoop: partitioning %s for %s", victim.NHID(), duration)
		nh.PartitionNode()

		select {
		case <-ctx.Done():
			nh.RestorePartitionedNode()
			return
		case <-time.After(duration):
		}

		log.Infof("partitionLoop: restoring %s", victim.NHID())
		nh.RestorePartitionedNode()

		uptime := 5*time.Second + time.Duration(rng.Intn(10))*time.Second
		select {
		case <-ctx.Done():
			return
		case <-time.After(uptime):
		}
	}
}

// TestLinearizabilityUnderSplits verifies that read/write operations
// remain linearizable while ranges are being split and nodes are
// periodically partitioned. The partition creates asymmetric catch-up:
// new shards from splits have short logs while old shards have large
// backlogs, so the new shard's replica may become leaseholder while
// the old shard's replica still has stale data (overlapping-range bug).
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
	testDuration := getTestDuration(t)

	keys := generateTestKeys(t, numKeys)

	// Seed data so the range has enough data for pebble's
	// EstimateDiskUsage to trigger splits. More seed data means a
	// larger backlog on partitioned nodes, increasing the catch-up
	// asymmetry between old and new shards after restore.
	leaseHolder := testutil.GetStoreWithRangeLease(t, ctx, stores, 2)
	for i := 0; i < 40; i++ {
		testutil.WriteRecord(ctx, t, leaseHolder, "default", 1000)
	}
	leaseHolder.DB().Flush()
	log.Infof("Seeded 40 records on %s", leaseHolder.NHID())

	elog := &eventLog{}
	var writeCount atomic.Int64

	workerCtx, workerCancel := context.WithTimeout(ctx, testDuration)
	defer workerCancel()

	var wg sync.WaitGroup

	// Periodically flush all stores so pebble's EstimateDiskUsage
	// reflects written data. The driver will detect ranges over the
	// target size and trigger splits automatically.
	wg.Go(func() {
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
	})

	// Periodically log event count.
	wg.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				log.Infof("Event log size: %d events", elog.numEvents())
			}
		}
	})

	// Start workers.
	for i := range numWorkers {
		wg.Go(func() {
			worker(workerCtx, i, stores, keys, elog, &writeCount)
		})
	}

	// Partition nemesis: periodically partition a random node's
	// Raft transport so its replicas fall behind during splits.
	wg.Go(func() {
		partitionLoop(workerCtx, stores)
	})

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

// TestLinearizabilityUnderKillRestart verifies linearizability while
// nodes are repeatedly killed and restarted. Uses 4 nodes: shard
// starts on s1-s3, and the nemesis kills/restarts any node at
// random intervals. If a node stays dead long enough, the driver
// may up-replicate to another available node.
func TestLinearizabilityUnderKillRestart(t *testing.T) {
	flags.Set(t, "cache.raft.entries_between_usage_checks", 1)
	flags.Set(t, "cache.raft.target_range_size_bytes", 0)
	flags.Set(t, "cache.raft.min_replicas_per_range", 3)
	flags.Set(t, "cache.raft.min_meta_range_replicas", 3)
	flags.Set(t, "cache.raft.enable_txn_cleanup", true)
	flags.Set(t, "cache.raft.zombie_node_scan_interval", 0)
	flags.Set(t, "cache.raft.dead_store_timeout", 1*time.Minute)
	flags.Set(t, "cache.raft.suspect_store_duration", 10*time.Second)
	flags.Set(t, "cache.raft.replica_scan_interval", 5*time.Second)
	flags.Set(t, "cache.raft.op_timeout", 5*time.Second)

	sf := testutil.NewStoreFactoryWithRootDir(t, t.TempDir())
	s1 := sf.NewStore(t)
	s2 := sf.NewStore(t)
	s3 := sf.NewStore(t)
	s4 := sf.NewStore(t)
	ctx := context.Background()

	initialStores := []*testutil.TestingStore{s1, s2, s3}
	allStores := []*testutil.TestingStore{s1, s2, s3, s4}
	sf.StartShard(t, ctx, initialStores...)

	testutil.WaitForRangeLease(t, ctx, initialStores, 1)
	testutil.WaitForRangeLease(t, ctx, initialStores, 2)

	const numKeys = 10
	const numWorkers = 5
	testDuration := getTestDuration(t)

	keys := generateTestKeys(t, numKeys)

	elog := &eventLog{}
	var writeCount atomic.Int64

	workerCtx, workerCancel := context.WithTimeout(ctx, testDuration)
	defer workerCancel()

	var wg sync.WaitGroup

	// Periodically log event count.
	wg.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				log.Infof("Event log size: %d events", elog.numEvents())
			}
		}
	})

	// Start workers on all 4 stores. Workers hitting a dead or
	// replica-less store will get errors (treated as indeterminate).
	for i := range numWorkers {
		wg.Go(func() {
			worker(workerCtx, i, allStores, keys, elog, &writeCount)
		})
	}

	// Run kill/restart loop on the test goroutine. Kills one
	// random node at a time to maintain quorum.
	killRestartLoop(workerCtx, t, sf, allStores)

	wg.Wait()

	log.Infof("Collected %d events (%d writes)", len(elog.getEvents()), writeCount.Load())
	checkLinearizability(t, elog, "/tmp/linearizability-kill-restart.html")
}
