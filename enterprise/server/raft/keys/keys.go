package keys

import (
	"bytes"
	"math"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
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
