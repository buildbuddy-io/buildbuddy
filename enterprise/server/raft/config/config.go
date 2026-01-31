package config

import (
	"flag"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	dbConfig "github.com/lni/dragonboat/v4/config"
)

type LogDBConfigType int

const (
	SmallMemLogDBConfigType LogDBConfigType = iota
	LargeMemLogDBConfigType
)

var (
	targetRangeSizeBytes  = flag.Int64("cache.raft.target_range_size_bytes", 1e8, "If set to a value greater than 0, ranges will be split until smaller than this size")
	rangeSizeJitterFactor = flag.Float64("cache.raft.range_size_jitter_factor", 0.1, "Randomization factor we use to determine the split threshold, should be [0, 1)")
	// This value should be approximately 10x the config.RTTMilliseconds,
	// but we want to include a little more time for the operation itself to
	// complete.
	singleRaftOpTimeout = flag.Duration("cache.raft.op_timeout", 1*time.Second, "The duration of timeout for a single raft operation")
)

func TargetRangeSizeBytes() int64 {
	return *targetRangeSizeBytes
}

func RangeSizeJitterFactor() float64 {
	return *rangeSizeJitterFactor
}

func SingleRaftOpTimeout() time.Duration {
	return *singleRaftOpTimeout
}

func GetLogDBConfig(t LogDBConfigType) dbConfig.LogDBConfig {
	switch t {
	case SmallMemLogDBConfigType:
		return dbConfig.GetSmallMemLogDBConfig()
	case LargeMemLogDBConfigType:
		return dbConfig.GetLargeMemLogDBConfig()
	default:
		alert.UnexpectedEvent("unknown-raft-log-db-config-type", "unknown type: %d", t)
	}
	return dbConfig.GetDefaultLogDBConfig()
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

type ServerConfig struct {
	RootDir           string
	RaftAddr          string
	GRPCAddr          string
	GRPCListeningAddr string
	NHID              string
	Partitions        []disk.Partition
	LogDBConfigType   LogDBConfigType
	FileStorer        filestore.Store
	GossipManager     interfaces.GossipService
}
