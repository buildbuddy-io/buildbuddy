package config

import (
	dbConfig "github.com/lni/dragonboat/v3/config"
)

func GetRaftConfig(clusterID, nodeID uint64) dbConfig.Config {
	rc := dbConfig.Config{
		NodeID:             nodeID,
		ClusterID:          clusterID,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	return rc
}
