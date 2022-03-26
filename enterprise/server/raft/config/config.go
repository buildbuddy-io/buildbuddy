package config

import (
	dbConfig "github.com/lni/dragonboat/v3/config"
)

func GetRaftConfig(clusterID, nodeID uint64) dbConfig.Config {
	rc := dbConfig.Config{
		NodeID:             nodeID,
		ClusterID:          clusterID,
		PreVote:            true,
		ElectionRTT:        200,
		HeartbeatRTT:       20,
		CheckQuorum:        true,
		SnapshotEntries:    1000000,
		CompactionOverhead: 1000,
	}
	return rc
}
