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

// AtimeIndexPrefix is the keyspace of the node-local atime index: one entry
// per file record, sorted by (partition, atime). It is in the unreplicated
// local region, outside every replica's prefix, so it is never part of a
// range. Nothing writes to it yet.
var AtimeIndexPrefix = []byte{'\x01', 'a', 't', 'i', 'd', 'x', '/'}

// AtimeIndexKey returns AtimeIndexPrefix + partitionID + '/' + atimeUsec
// (8 bytes, big-endian, so byte order is time order) + '/' + fileKey.
func AtimeIndexKey(partitionID string, atimeUsec int64, fileKey []byte) []byte {
	if atimeUsec < 0 {
		// As a uint64, a negative atime would sort last. Sort it first.
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

// ParseAtimeIndexKey is the inverse of AtimeIndexKey. The partition ID must
// not contain '/'.
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

// AtimeIndexPartitionRange returns the bounds of a partition's index entries.
func AtimeIndexPartitionRange(partitionID string) ([]byte, []byte) {
	return Range(MakeKey(AtimeIndexPrefix, []byte(partitionID), []byte{'/'}))
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
