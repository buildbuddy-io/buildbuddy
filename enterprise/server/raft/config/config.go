package config

import (
	"flag"

	dbConfig "github.com/lni/dragonboat/v4/config"
)

var (
	maxRangeSizeBytes = flag.Int64("cache.raft.max_range_size_bytes", 1e8, "If set to a value greater than 0, ranges will be split until smaller than this size")
)

func MaxRangeSizeBytes() int64 {
	return *maxRangeSizeBytes
}

func GetRaftConfig(rangeID, replicaID uint64) dbConfig.Config {
	rc := dbConfig.Config{
		ReplicaID:               replicaID,
		ShardID:                 rangeID,
		WaitReady:               true,
		PreVote:                 true,
		ElectionRTT:             20,
		HeartbeatRTT:            2,
		CheckQuorum:             true,
		SnapshotEntries:         50000,
		CompactionOverhead:      500,
		OrderedConfigChange:     true,
		SnapshotCompressionType: dbConfig.Snappy,
		EntryCompressionType:    dbConfig.Snappy,
	}
	return rc
}
