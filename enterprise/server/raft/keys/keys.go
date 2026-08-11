package keys

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type Key []byte

var (
	MinByte Key = []byte{0}
	MaxByte Key = []byte{math.MaxUint8}
)

func MakeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

func (k Key) Next() Key {
	nk := make([]byte, len(k)+1)
	copy(nk, k)
	nk[len(nk)-1] = 0
	return nk
}

func RangeMetaKey(key Key) Key {
	return MakeKey([]byte{'\x02'}, key)
}

func SystemKey(key Key) Key {
	return MakeKey([]byte{'\x03'}, key)
}

func IsLocalKey(key Key) bool {
	if len(key) == 0 {
		return false
	}
	return key[0] == '\x01'
}

// Range returns a pair of keys that represent the upper and lower bounds of a
// range identified by the given key prefix.
func Range(key []byte) ([]byte, []byte) {
	return MakeKey(key, MinByte), MakeKey(key, MaxByte)
}

// AtimeIndexPrefix is the node-local eviction-index keyspace. It lives in the
// unreplicated local ('\x01') region but outside every replica's
// '\x01c<rangeID>n<replicaID>-' prefix, so index entries are never part of a
// range span, snapshot stream, split, or replica clear. Each replica's apply
// path derives one entry per file record hosted on this node; entries sort by
// (partition, access time) so the eviction scanner reads coldest-first.
//
// Ownership: the index is derived, per-node state -- not replicated state --
// so it is legitimately mutated both by the replica state machine (which
// writes entries in the same batch as the record, keeping the pair atomic)
// and directly, outside raft, by node-local maintenance: the usagetracker
// sweep drops orphaned entries and backfills/verifies the index, and the
// store deletes entries when removing a replica's data. Going through raft
// for those would be the wrong layer: each node's entry set differs (it
// depends on local recovery history), so there is no replicated truth to
// agree on. Correctness rests on the index being orphan-tolerant (a stale
// entry is skipped and dropped; record deletes carry a MatchAtime guard) and
// on missing entries being repaired by the verifier.
var AtimeIndexPrefix = []byte{'\x01', 'a', 't', 'i', 'd', 'x', '/'}

// AtimeIndexKey returns the eviction-index key for a file record:
// AtimeIndexPrefix + partitionID + '/' + atimeUsec (8-byte big-endian, so byte
// order equals time order) + '/' + fileKey.
func AtimeIndexKey(partitionID string, atimeUsec int64, fileKey []byte) []byte {
	if atimeUsec < 0 {
		// A negative atime encoded as uint64 would sort after every valid
		// entry while parsing back as ancient, so the sweep (which stops at
		// the age boundary) would never reach it and the record could never
		// be evicted. Clamp to zero: malformed atimes sort first and are
		// evicted first. Entry writes and deletes both come through here, so
		// the clamped key stays symmetric.
		atimeUsec = 0
	}
	k := make([]byte, 0, len(AtimeIndexPrefix)+len(partitionID)+1+8+1+len(fileKey))
	k = append(k, AtimeIndexPrefix...)
	k = append(k, partitionID...)
	k = append(k, '/')
	k = binary.BigEndian.AppendUint64(k, uint64(atimeUsec))
	k = append(k, '/')
	k = append(k, fileKey...)
	return k
}

// ParseAtimeIndexKey splits an index key back into its parts. The partition ID
// must not contain '/' (the 8 atime bytes may, but they are located by offset,
// not by separator).
func ParseAtimeIndexKey(key []byte) (partitionID string, atimeUsec int64, fileKey []byte, err error) {
	rest, ok := bytes.CutPrefix(key, AtimeIndexPrefix)
	if !ok {
		return "", 0, nil, status.InvalidArgumentErrorf("not an atime index key: %q", key)
	}
	part, rest, ok := bytes.Cut(rest, []byte{'/'})
	if !ok || len(rest) < 9 || rest[8] != '/' {
		return "", 0, nil, status.InvalidArgumentErrorf("malformed atime index key: %q", key)
	}
	return string(part), int64(binary.BigEndian.Uint64(rest[:8])), rest[9:], nil
}

// AtimeIndexPartitionRange returns bounds spanning every eviction-index entry
// for the given partition.
func AtimeIndexPartitionRange(partitionID string) ([]byte, []byte) {
	return Range(MakeKey(AtimeIndexPrefix, []byte(partitionID), []byte{'/'}))
}

// AtimeIndexBackfillMarkerKey returns the node-local marker recording that the
// one-time atime-index backfill completed for the partition. It lives in the
// unreplicated local region but sorts outside every AtimeIndexPartitionRange
// ('-' precedes '/'), so index sweeps never see it.
func AtimeIndexBackfillMarkerKey(partitionID string) []byte {
	return MakeKey([]byte{'\x01'}, []byte("atidx-backfill/"), []byte(partitionID))
}

// PartitionIDFromRangeStart parses the partition ID out of a range descriptor's
// start key. Range data is keyed under "PT<partition_id>/..."; returns "" for
// keys without that prefix (e.g., the meta range).
func PartitionIDFromRangeStart(key []byte) string {
	rest, found := bytes.CutPrefix(key, []byte(filestore.PartitionDirectoryPrefix))
	if !found {
		return ""
	}
	before, _, ok := bytes.Cut(rest, []byte{'/'})
	if !ok {
		return ""
	}
	return string(before)
}

// RangeMetricLabels builds the standard per-range label set used by raft
// metrics (e.g. RaftRangeReplica, RaftBytes, RaftLeases, RaftLeaders,
// RaftProposals), so they can all be sliced by the same dimensions: range,
// nodehost, partition, and zone.
func RangeMetricLabels(rd *rfpb.RangeDescriptor, nhid, zone string) prometheus.Labels {
	partitionID := rd.GetPartitionId()
	if partitionID == "" {
		partitionID = PartitionIDFromRangeStart(rd.GetStart())
	}
	if partitionID == "" {
		if rd.GetRangeId() == 1 {
			partitionID = "meta"
		} else {
			partitionID = "unknown"
		}
	}
	return prometheus.Labels{
		metrics.RaftRangeIDLabel:    strconv.FormatUint(rd.GetRangeId(), 10),
		metrics.RaftNodeHostIDLabel: nhid,
		metrics.PartitionID:         partitionID,
		metrics.ZoneLabel:           zone,
	}
}
