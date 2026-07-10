package pebble_cache

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	bbpebble "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	cpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Wire-level field numbers used to hand-construct adversarial encodings,
// derived from the descriptors so they track storage.proto.
var (
	fmFields  = (&sgpb.FileMetadata{}).ProtoReflect().Descriptor().Fields()
	smFields  = (&sgpb.StorageMetadata{}).ProtoReflect().Descriptor().Fields()
	gcsFields = (&sgpb.StorageMetadata_GCSMetadata{}).ProtoReflect().Descriptor().Fields()

	fmStorageMetadataNum     = protowire.Number(fmFields.ByName("storage_metadata").Number())
	fmStoredSizeBytesNum     = protowire.Number(fmFields.ByName("stored_size_bytes").Number())
	smGCSMetadataNum         = protowire.Number(smFields.ByName("gcs_metadata").Number())
	gcsLastCustomTimeUsecNum = protowire.Number(gcsFields.ByName("last_custom_time_usec").Number())
)

// checkViewAgainstFullUnmarshal asserts the generated FindMissing view's
// contract: any buffer UnmarshalVT accepts must UnmarshalWire successfully
// (the generated decode mirrors UnmarshalVT, so there is no leniency
// divergence) with equal values for the view's fields.
func checkViewAgainstFullUnmarshal(t testing.TB, b []byte) {
	md := &sgpb.FileMetadata{}
	if err := md.UnmarshalVT(b); err != nil {
		var v sgpb.FileMetadataFindMissingView
		_ = v.UnmarshalWire(b) // must not panic
		return
	}
	var v sgpb.FileMetadataFindMissingView
	require.NoError(t, v.UnmarshalWire(b), "full unmarshal succeeded but view parse failed")
	sm := md.GetStorageMetadata()
	gcs := sm.GetGcsMetadata()
	require.Equal(t, md.GetStoredSizeBytes(), v.StoredSizeBytes, "stored_size_bytes")
	require.Equal(t, md.GetLastAccessUsec(), v.LastAccessUsec, "last_access_usec")
	require.Equal(t, sm != nil, v.HasStorageMetadata, "storage_metadata presence")
	require.Equal(t, gcs != nil, v.StorageMetadata.HasGcsMetadata, "gcs_metadata presence")
	require.Equal(t, gcs.GetLastCustomTimeUsec(), v.StorageMetadata.GcsMetadata.LastCustomTimeUsec, "last_custom_time_usec")
}

func diskFileMetadata() *sgpb.FileMetadata {
	return &sgpb.FileMetadata{
		FileRecord: &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:          rspb.CacheType_CAS,
				RemoteInstanceName: "some/instance/name",
				PartitionId:        "PTdefaultpartition",
				GroupId:            "GR0123456789012345678901234567890123456789",
			},
			Digest: &repb.Digest{
				Hash:      strings.Repeat("a1", 32),
				SizeBytes: 123_456,
			},
			DigestFunction: repb.DigestFunction_SHA256,
		},
		StorageMetadata: &sgpb.StorageMetadata{
			FileMetadata: &sgpb.StorageMetadata_FileMetadata{
				Filename: "/data/cache/blobs/ranged/a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
			},
		},
		StoredSizeBytes: 123_456,
		LastAccessUsec:  1_700_000_000_000_000,
		LastModifyUsec:  1_690_000_000_000_000,
	}
}

func inlineFileMetadata(dataLen int) *sgpb.FileMetadata {
	md := diskFileMetadata()
	md.StorageMetadata = &sgpb.StorageMetadata{
		InlineMetadata: &sgpb.StorageMetadata_InlineMetadata{
			Data:          []byte(strings.Repeat("z", dataLen)),
			CreatedAtNsec: 1_700_000_000_000_000_000,
		},
	}
	return md
}

func gcsFileMetadata() *sgpb.FileMetadata {
	md := diskFileMetadata()
	md.StorageMetadata = &sgpb.StorageMetadata{
		GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{
			BlobName:           "blobs/a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
			LastCustomTimeUsec: 1_700_000_000_000_000,
		},
	}
	md.EncryptionMetadata = &sgpb.EncryptionMetadata{
		EncryptionKeyId: "EK123",
		Version:         2,
	}
	return md
}

func mustMarshal(t testing.TB, md *sgpb.FileMetadata) []byte {
	b, err := md.MarshalVT()
	require.NoError(t, err)
	return b
}

// wireTestCases returns test buffers covering both ordinary marshaled
// messages and adversarial wire encodings (duplicate fields, split
// submessages, unknown fields, wrong wire types, truncation).
func wireTestCases(t testing.TB) map[string][]byte {
	cases := map[string][]byte{}

	cases["empty"] = []byte{}
	cases["disk"] = mustMarshal(t, diskFileMetadata())
	cases["inline"] = mustMarshal(t, inlineFileMetadata(1024))
	cases["gcs"] = mustMarshal(t, gcsFileMetadata())

	minimal := &sgpb.FileMetadata{StoredSizeBytes: 1}
	cases["minimal"] = mustMarshal(t, minimal)

	// Zero stored_size_bytes is not written on the wire (proto3 implicit
	// presence) and must read back as 0.
	zeroSize := diskFileMetadata()
	zeroSize.StoredSizeBytes = 0
	cases["zero-stored-size"] = mustMarshal(t, zeroSize)

	// GCSMetadata present but empty: presence must still be detected since
	// findMissing treats present-but-expired differently from absent.
	emptyGCS := diskFileMetadata()
	emptyGCS.StorageMetadata = &sgpb.StorageMetadata{
		GcsMetadata: &sgpb.StorageMetadata_GCSMetadata{},
	}
	cases["empty-gcs"] = mustMarshal(t, emptyGCS)

	// Negative int64s encode as 10-byte varints.
	negative := gcsFileMetadata()
	negative.LastAccessUsec = -123
	negative.StoredSizeBytes = -1
	negative.StorageMetadata.GcsMetadata.LastCustomTimeUsec = -456
	cases["negative-values"] = mustMarshal(t, negative)

	// Duplicated scalar field: last occurrence must win.
	dup := mustMarshal(t, diskFileMetadata())
	dup = protowire.AppendTag(dup, fmStoredSizeBytesNum, protowire.VarintType)
	dup = protowire.AppendVarint(dup, 999)
	cases["duplicate-scalar-last-wins"] = dup

	// storage_metadata split across two records: proto merge semantics say
	// the submessages are merged, so the gcs_metadata in the second record
	// must be seen.
	smBytes, err := gcsFileMetadata().GetStorageMetadata().MarshalVT()
	require.NoError(t, err)
	split := mustMarshal(t, diskFileMetadata())
	split = protowire.AppendTag(split, fmStorageMetadataNum, protowire.BytesType)
	split = protowire.AppendBytes(split, smBytes)
	cases["split-storage-metadata"] = split

	// gcs_metadata duplicated with different custom times: last must win.
	gcs2 := protowire.AppendTag(nil, gcsLastCustomTimeUsecNum, protowire.VarintType)
	gcs2 = protowire.AppendVarint(gcs2, 42)
	dupGCS := split
	sm2 := protowire.AppendTag(nil, smGCSMetadataNum, protowire.BytesType)
	sm2 = protowire.AppendBytes(sm2, gcs2)
	dupGCS = protowire.AppendTag(dupGCS, fmStorageMetadataNum, protowire.BytesType)
	dupGCS = protowire.AppendBytes(dupGCS, sm2)
	cases["duplicate-gcs-last-wins"] = dupGCS

	// Unknown fields of every wire type, interleaved at the front, in the
	// middle of the top-level message, and inside submessages.
	unknown := protowire.AppendTag(nil, 99, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 7)
	unknown = protowire.AppendTag(unknown, 100, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("opaque"))
	unknown = protowire.AppendTag(unknown, 101, protowire.Fixed64Type)
	unknown = protowire.AppendFixed64(unknown, 7)
	unknown = protowire.AppendTag(unknown, 102, protowire.Fixed32Type)
	unknown = protowire.AppendFixed32(unknown, 7)
	unknown = append(unknown, mustMarshal(t, gcsFileMetadata())...)
	unknown = protowire.AppendTag(unknown, 103, protowire.StartGroupType)
	unknown = protowire.AppendTag(unknown, 1, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	unknown = protowire.AppendTag(unknown, 103, protowire.EndGroupType)
	cases["unknown-fields"] = unknown

	// Wrong wire type for a field the view decodes: a full unmarshal rejects
	// this, and so does the view.
	wrongType := protowire.AppendTag(nil, fmStoredSizeBytesNum, protowire.Fixed64Type)
	wrongType = protowire.AppendFixed64(wrongType, 123)
	cases["wrong-wire-type"] = wrongType

	// Truncated buffer: both parsers must reject it (not asserted, but must
	// not panic).
	full := mustMarshal(t, gcsFileMetadata())
	cases["truncated"] = full[:len(full)-3]

	return cases
}

func TestFindMissingView(t *testing.T) {
	for name, b := range wireTestCases(t) {
		t.Run(name, func(t *testing.T) {
			checkViewAgainstFullUnmarshal(t, b)
		})
	}
}

// UnmarshalWire must fully reset the view: fields present in an earlier
// buffer but absent in the next must read as zero/absent.
func TestFindMissingViewReset(t *testing.T) {
	var v sgpb.FileMetadataFindMissingView
	require.NoError(t, v.UnmarshalWire(mustMarshal(t, gcsFileMetadata())))
	require.True(t, v.StorageMetadata.HasGcsMetadata)
	require.NotZero(t, v.StorageMetadata.GcsMetadata.LastCustomTimeUsec)

	require.NoError(t, v.UnmarshalWire(mustMarshal(t, &sgpb.FileMetadata{StoredSizeBytes: 1})))
	require.Equal(t, int64(1), v.StoredSizeBytes)
	require.Zero(t, v.LastAccessUsec)
	require.False(t, v.HasStorageMetadata)
	require.False(t, v.StorageMetadata.HasGcsMetadata)
	require.Zero(t, v.StorageMetadata.GcsMetadata.LastCustomTimeUsec)
}

// TestFindMissingViewRandomized cross-checks the view against a full
// unmarshal on a large number of randomly generated and randomly mutated
// wire buffers. This complements FuzzFindMissingView since coverage-guided
// fuzzing doesn't run under bazel.
func TestFindMissingViewRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(20260708))
	for i := 0; i < 100_000; i++ {
		b := randomWireBuffer(t, rng)
		checkViewAgainstFullUnmarshal(t, b)
		if t.Failed() {
			t.Fatalf("failing buffer (iteration %d): %x", i, b)
		}
	}
}

func randomInt64(rng *rand.Rand) int64 {
	switch rng.Intn(4) {
	case 0:
		return 0
	case 1:
		return rng.Int63n(1 << 20)
	case 2:
		return -rng.Int63()
	default:
		return rng.Int63()
	}
}

func randomWireBuffer(t testing.TB, rng *rand.Rand) []byte {
	md := &sgpb.FileMetadata{
		StoredSizeBytes: randomInt64(rng),
		LastAccessUsec:  randomInt64(rng),
		LastModifyUsec:  randomInt64(rng),
	}
	if rng.Intn(2) == 0 {
		md.FileRecord = diskFileMetadata().GetFileRecord()
	}
	if rng.Intn(4) > 0 {
		md.StorageMetadata = &sgpb.StorageMetadata{}
		if rng.Intn(2) == 0 {
			md.StorageMetadata.FileMetadata = &sgpb.StorageMetadata_FileMetadata{Filename: "f"}
		}
		if rng.Intn(2) == 0 {
			md.StorageMetadata.InlineMetadata = &sgpb.StorageMetadata_InlineMetadata{
				Data:          make([]byte, rng.Intn(64)),
				CreatedAtNsec: randomInt64(rng),
			}
		}
		if rng.Intn(2) == 0 {
			md.StorageMetadata.GcsMetadata = &sgpb.StorageMetadata_GCSMetadata{
				BlobName:           "b",
				LastCustomTimeUsec: randomInt64(rng),
			}
		}
	}
	b := mustMarshal(t, md)

	for n := rng.Intn(4); n > 0; n-- {
		switch rng.Intn(6) {
		case 0:
			// Append an unknown (or known-but-wrong-type) field.
			num := protowire.Number(rng.Intn(200) + 1)
			switch rng.Intn(4) {
			case 0:
				b = protowire.AppendTag(b, num, protowire.VarintType)
				b = protowire.AppendVarint(b, rng.Uint64())
			case 1:
				b = protowire.AppendTag(b, num, protowire.BytesType)
				b = protowire.AppendBytes(b, make([]byte, rng.Intn(16)))
			case 2:
				b = protowire.AppendTag(b, num, protowire.Fixed64Type)
				b = protowire.AppendFixed64(b, rng.Uint64())
			default:
				b = protowire.AppendTag(b, num, protowire.Fixed32Type)
				b = protowire.AppendFixed32(b, rng.Uint32())
			}
		case 1:
			// Append a duplicate occurrence of a scalar field in the view.
			b = protowire.AppendTag(b, fmStoredSizeBytesNum, protowire.VarintType)
			b = protowire.AppendVarint(b, rng.Uint64())
		case 2:
			// Append another storage_metadata record containing a
			// gcs_metadata record, exercising submessage merge semantics.
			gcs := protowire.AppendTag(nil, gcsLastCustomTimeUsecNum, protowire.VarintType)
			gcs = protowire.AppendVarint(gcs, rng.Uint64())
			sm := protowire.AppendTag(nil, smGCSMetadataNum, protowire.BytesType)
			sm = protowire.AppendBytes(sm, gcs)
			b = protowire.AppendTag(b, fmStorageMetadataNum, protowire.BytesType)
			b = protowire.AppendBytes(b, sm)
		case 3:
			// Truncate.
			if len(b) > 0 {
				b = b[:rng.Intn(len(b))]
			}
		case 4:
			// Flip a random byte.
			if len(b) > 0 {
				b[rng.Intn(len(b))] ^= byte(1 << rng.Intn(8))
			}
		case 5:
			// Swap two chunks (moves records around, likely corrupts).
			if len(b) > 1 {
				i := rng.Intn(len(b))
				chunk := append([]byte{}, b[i:]...)
				b = append(chunk, b[:i]...)
			}
		}
	}
	return b
}

func FuzzFindMissingView(f *testing.F) {
	for _, b := range wireTestCases(f) {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		checkViewAgainstFullUnmarshal(t, b)
	})
}

// BenchmarkFileMetadataLookup measures the metadata-lookup stage findMissing
// pays per key against a real pebble DB: a Get plus decoding the findMissing
// fields. This puts the parse-level differences in context of the DB read.
func BenchmarkFileMetadataLookup(b *testing.B) {
	// The same instrumented wrapper production uses via p.leaser.DB().
	db, err := bbpebble.Open("", "bench", &cpebble.Options{FS: vfs.NewMem()})
	require.NoError(b, err)
	defer db.Close()

	for _, tc := range []struct {
		name string
		md   *sgpb.FileMetadata
	}{
		{"disk", diskFileMetadata()},
		{"inline1K", inlineFileMetadata(1024)},
		{"gcs", gcsFileMetadata()},
	} {
		key := []byte("PTFOO/GR00123/" + tc.name)
		require.NoError(b, db.Set(key, mustMarshal(b, tc.md), cpebble.Sync))

		// A full unmarshal into a pooled message, like lookupFileMetadata.
		b.Run(tc.name+"/full-unmarshal", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				md := sgpb.FileMetadataFromVTPool()
				if err := bbpebble.GetProto(db, key, md); err != nil {
					b.Fatal(err)
				}
				md.ReturnToVTPool()
			}
		})
		// What findMissing uses: the generated FindMissing wire view.
		b.Run(tc.name+"/view", func(b *testing.B) {
			b.ReportAllocs()
			var v sgpb.FileMetadataFindMissingView
			parse := func(value []byte) error {
				return v.UnmarshalWire(value)
			}
			for i := 0; i < b.N; i++ {
				if err := bbpebble.GetFunc(db, key, parse); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFileMetadataParse(b *testing.B) {
	for _, tc := range []struct {
		name string
		md   *sgpb.FileMetadata
	}{
		{"disk", diskFileMetadata()},
		{"inline1K", inlineFileMetadata(1024)},
		{"gcs", gcsFileMetadata()},
	} {
		buf := mustMarshal(b, tc.md)
		// A full unmarshal into a pooled message.
		b.Run(tc.name+"/full-unmarshal", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				md := sgpb.FileMetadataFromVTPool()
				if err := md.UnmarshalVT(buf); err != nil {
					b.Fatal(err)
				}
				md.ReturnToVTPool()
			}
		})
		// The generated find_missing wire view.
		b.Run(tc.name+"/view", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var v sgpb.FileMetadataFindMissingView
				if err := v.UnmarshalWire(buf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
