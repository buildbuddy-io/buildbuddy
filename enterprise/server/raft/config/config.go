package config

import (
	dbConfig "github.com/lni/dragonboat/v4/config"
)

func GetRaftConfig(shardID, replicaID uint64) dbConfig.Config {
	rc := dbConfig.Config{
		ReplicaID:          replicaID,
		ShardID:            shardID,
		WaitReady:          true,
		PreVote:            true,
		ElectionRTT:        200,
		HeartbeatRTT:       20,
		CheckQuorum:        true,
		SnapshotEntries:    1000000,
		CompactionOverhead: 1000,
	}
	return rc
}
