package config

import (
	"flag"
	"time"

	dbConfig "github.com/lni/dragonboat/v4/config"
)

var (
	targetRangeSizeBytes  = flag.Int64("cache.raft.target_range_size_bytes", 1e8, "If set to a value greater than 0, ranges will be split until smaller than this size")
	rangeSizeJitterFactor = flag.Float64("cache.raft.range_size_jitter_factor", 0.1, "Randomization factor we use to determine the split threshold, should be [0, 1)")
	// This value should be approximately 10x the config.RTTMilliseconds,
	// but we want to include a little more time for the operation itself to
	// complete.
	singleRaftOpTimeout = flag.Duration("cache.raft.op_timeout", 1*time.Second, "The duration of timeout for a single raft operation")
)

type BackupOptions struct {
	Enabled    bool          `yaml:"enabled" json:"enabled"`
	Interval   time.Duration `yaml:"interval" json:"interval"`
	Dir        string        `yaml:"dir" json:"dir"`
	NumWorkers int           `yaml:"num_workers" json:"num_workers"`
}

func TargetRangeSizeBytes() int64 {
	return *targetRangeSizeBytes
}

func RangeSizeJitterFactor() float64 {
	return *rangeSizeJitterFactor
}

func SingleRaftOpTimeout() time.Duration {
	return *singleRaftOpTimeout
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
