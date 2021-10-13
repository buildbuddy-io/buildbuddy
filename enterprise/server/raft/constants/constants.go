package constants

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
)

const (
	NodeHostIDTag = "node_host_id"
	RaftAddressTag = "raft_address"
	GRPCAddressTag = "grpc_address"

	InitialClusterID = 1

	localPrefixByte  = '\x01'
	localMaxByte     = '\x02'
	meta1PrefixByte  = localMaxByte
	meta1MaxByte     = '\x03'
	meta2PrefixByte  = meta1MaxByte
	meta2MaxByte     = '\x04'
	systemPrefixByte = meta2MaxByte
	systemMaxByte    = '\x05'
)

var (
	LocalPrefix      = keys.Key{localPrefixByte}
	SystemPrefix     = keys.Key{systemPrefixByte}

	InitClusterSetupTimeKey = keys.MakeKey(SystemPrefix, []byte("initial_cluster_initialization_time"))
)

