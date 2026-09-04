package pebble_cache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockmap"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	cpebble "github.com/cockroachdb/pebble"
)

// TestGetSizeOnLocalDiskFromView asserts that the Eviction-view size
// computation the sampler uses matches getSizeOnLocalDisk on the full
// message — including the substitution of len(serialized value) for
// md.SizeVT() — for every storage type and both includeMetadata modes.
func TestGetSizeOnLocalDiskFromView(t *testing.T) {
	key := []byte("PTFOO/GR00123/abcdef1234567890")
	for name, md := range map[string]*sgpb.FileMetadata{
		"disk": {
			StorageMetadata: &sgpb.StorageMetadata{
				FileMetadata: &sgpb.StorageMetadata_FileMetadata{Filename: "/data/blobs/foo"},
			},
			StoredSizeBytes: 123_456,
			LastAccessUsec:  1_700_000_000_000_000,
		},
		"inline": {
			StorageMetadata: &sgpb.StorageMetadata{
				InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
					Data:          make([]byte, 1024),
					CreatedAtNsec: 1_700_000_000_000_000_000,
				},
			},
			StoredSizeBytes: 1024,
			LastAccessUsec:  1_700_000_000_000_000,
		},
		"gcs": {
			StorageMetadata: &sgpb.StorageMetadata{
				GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
					BlobName:           "blobs/foo",
					LastCustomTimeUsec: 1_700_000_000_000_000,
				},
			},
			StoredSizeBytes: 123_456,
			LastAccessUsec:  1_700_000_000_000_000,
		},
		"no-storage-metadata": {
			StoredSizeBytes: 7,
			LastAccessUsec:  1_700_000_000_000_000,
		},
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			buf, err := md.MarshalVT()
			require.NoError(t, err)
			var v sgpb.FileMetadataEvictionView
			require.NoError(t, v.UnmarshalWire(buf))
			for _, includeMetadata := range []bool{true, false} {
				want := getSizeOnLocalDisk(key, md, includeMetadata)
				got := getSizeOnLocalDiskFromView(len(key), len(buf), &v, includeMetadata)
				require.Equal(t, want, got, "includeMetadata=%v", includeMetadata)
			}
		})
	}
}

type benchVersionGetter struct{}

func (benchVersionGetter) minDatabaseVersion() filestore.PebbleKeyVersion {
	return filestore.Version5
}

const (
	// benchMinEvictionAge sits between the two atime populations newBenchEvictor
	// writes (one day old vs. fresh), so eligibleEvery alone determines which
	// rows the sampler may emit.
	benchMinEvictionAge = time.Hour

	numKeys          = 16384
	benchPartitionID = "benchpart"
)

func fileMetadata(i int, eligible bool) *sgpb.FileMetadata {
	atime := time.Now()
	if eligible {
		atime = atime.Add(-24 * time.Hour)
	}
	md := &sgpb.FileMetadata{
		FileRecord: &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   rspb.CacheType_CAS,
				PartitionId: benchPartitionID,
				GroupId:     "GR0123456789012345678901234567890123456789",
			},
			Digest: &repb.Digest{
				// Real digests are uniformly distributed; the sampler's
				// random seeks depend on that (a skewed keyspace makes
				// SeekGE miss every key and the sampler spin).
				Hash:      fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "%d", i))),
				SizeBytes: 123_456,
			},
			DigestFunction: repb.DigestFunction_SHA256,
		},
		StoredSizeBytes: 123_456,
		LastAccessUsec:  atime.UnixMicro(),
		LastModifyUsec:  atime.Add(-24 * time.Hour).UnixMicro(),
	}
	switch i % 3 {
	case 0:
		md.StorageMetadata = &sgpb.StorageMetadata{
			FileMetadata: &sgpb.StorageMetadata_FileMetadata{Filename: "/data/blobs/foo"},
		}
	case 1:
		md.StorageMetadata = &sgpb.StorageMetadata{
			InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{Data: make([]byte, 1024), CreatedAtNsec: 1},
		}
	case 2:
		md.StorageMetadata = &sgpb.StorageMetadata{
			GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{BlobName: "blobs/foo", LastCustomTimeUsec: 1},
		}
	}
	return md
}

// newBenchEvictor builds a real partitionEvictor over an in-memory pebble DB
// populated with numKeys metadata rows (mixed disk/inline/gcs storage). Every
// eligibleEvery-th row has an atime one day old — eligible for eviction; the
// rest are fresh — filtered by benchMinEvictionAge (eligibleEvery=1: all
// eligible, eligibleEvery=0: none).
func newBenchEvictor(b *testing.B, eligibleEvery int) (*partitionEvictor, pebble.IPebbleDB) {
	b.Helper()
	db, err := pebble.Open("", "bench", &cpebble.Options{FS: vfs.NewMem()})
	require.NoError(b, err)
	b.Cleanup(func() { db.Close() })
	fileStorer := filestore.New()
	for i := range numKeys {
		md := fileMetadata(i, eligibleEvery > 0 && i%eligibleEvery == 0)
		key, err := fileStorer.PebbleKey(md.GetFileRecord())
		require.NoError(b, err)
		keyBytes, err := key.Bytes(filestore.Version5)
		require.NoError(b, err)
		buf, err := md.MarshalVT()
		require.NoError(b, err)
		require.NoError(b, db.Set(keyBytes, buf, cpebble.NoSync))
	}
	part := disk.Partition{
		ID:                benchPartitionID,
		MaxSizeBytes:      100,
		MinEvictionAge:    new(benchMinEvictionAge),
		EvictionThreshold: new(JanitorCutoffThreshold),
	}
	evictor, err := newPartitionEvictor(
		context.Background(),
		part,
		fileStorer,
		"", /*=blobDir*/
		pebble.NewDBLeaser(db),
		lockmap.New[string](),
		benchVersionGetter{},
		clockwork.NewRealClock(),
		"bench",
		true,          /*=includeMetadataSize*/
		1000,          /*=sampleBufferSize*/
		10000,         /*=samplesPerBatch*/
		5*time.Minute, /*=samplerIterRefreshPeriod*/
		20,            /*=deleteBufferSize*/
		1,             /*=numDeleteWorkers*/
	)
	require.NoError(b, err)
	return evictor, db
}

// BenchmarkSampleGenerator runs the real sampler goroutine end to end —
// iterator refresh, random seeks, timers, and channel sends included.
//
// "eligible" (all entries old) and "sparse-eligible" (1 in 100 old, the
// regime of a partition whose churn outpaces min_eviction_age) count
// delivered samples: b.N samples received, so sparse ns/op includes the cost
// of scanning past the ~99 filtered keys between samples. "too-recent" (no
// entries old) never delivers a sample, so it counts scanned keys instead,
// observed via the age_too_small counter.
func BenchmarkSampleGenerator(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	for _, tc := range []struct {
		name          string
		eligibleEvery int
	}{
		{"eligible", 1},
		{"sparse-eligible", 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e, _ := newBenchEvictor(b, tc.eligibleEvery)
			quit := make(chan struct{})
			go e.startSampleGenerator(quit)
			// Receive one sample before timing so generator goroutine
			// startup isn't charged to the measured window.
			if _, ok := <-e.samples; !ok {
				b.Fatal("samples channel closed early")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := <-e.samples; !ok {
					b.Fatal("samples channel closed early")
				}
			}
			b.StopTimer()
			close(quit)
			// samples is closed when startSampleGenerator exits, so wait for
			// that before returning and closing the DB.
			for range e.samples {
			}
		})
	}
	b.Run("too-recent", func(b *testing.B) {
		e, _ := newBenchEvictor(b, 0 /*=eligibleEvery*/)
		scanned := metrics.PebbleCacheEvictionSamples.WithLabelValues(benchPartitionID, "bench", "age_too_small")
		base := testutil.ToFloat64(scanned)
		quit := make(chan struct{})
		go e.startSampleGenerator(quit)
		b.ResetTimer()
		// b.N counts keys scanned; poll the counter since no samples are
		// ever delivered in this regime.
		for testutil.ToFloat64(scanned)-base < float64(b.N) {
			time.Sleep(time.Millisecond)
		}
		b.StopTimer()
		close(quit)
		for range e.samples {
		}
	})
}
