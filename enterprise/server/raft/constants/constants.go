package constants

import (
	"math"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
)

// Gossip (broadcast) constants
const (
	NodeHostIDTag  = "node_host_id"
	RaftAddressTag = "raft_address"
	GRPCAddressTag = "grpc_address"
	MetaRangeTag   = "meta_range"
	NodeUsageTag   = "node_usage"

	RegistryUpdateEvent       = "registry_update_event"
	RegistryQueryEvent        = "registry_query_event"
	AutoBringupEvent          = "auto_bringup_event"
	NodeUsageEvent            = "node_usage_event"
	PlacementDriverQueryEvent = "placement_driver_query_event"
)

// Key range contants
const (
	// Anything written between \x01 and \x02 is a localstore only key so
	// will not be replicated.
	localPrefixByte = '\x01'
	localMaxByte    = '\x02'

	// Anything written between \x02 and \x03 is a metarange key and will
	// live on cluster 1 and never be split.
	metaPrefixByte = localMaxByte
	metaMaxByte    = '\x03'

	systemPrefixByte = metaMaxByte
	systemMaxByte    = '\x04'

	// Anything else (from \x04 onward)  will fall into normal,
	// splittable ranges.
)

// Other constants
const (
	InitialClusterID = 1
	InitialNodeID    = 1
	InitialRangeID   = 1

	MinByte = 0
	MaxByte = math.MaxUint8

	UnsplittableMaxByte = systemMaxByte
)

const CASErrorMessage = "CAS expected value did not match"

// Key constants (some of these have to be vars because of how they are made.
var (
	LocalPrefix     = keys.Key{localPrefixByte}
	MetaRangePrefix = keys.Key{metaPrefixByte}
	SystemPrefix    = keys.Key{systemPrefixByte}

	// The last clusterID that was generated.
	LastClusterIDKey = keys.MakeKey(SystemPrefix, []byte("last_cluster_id"))

	// The last nodeID that was generated.
	LastNodeIDKey = keys.MakeKey(SystemPrefix, []byte("last_node_id"))

	// The last rangeID that was generated.
	LastRangeIDKey = keys.MakeKey(SystemPrefix, []byte("last_range_id"))

	// When the cluster was created.
	ClusterSetupTimeKey = keys.MakeKey(LocalPrefix, []byte("cluster_setup_time"))

	// The last index that was applied by a statemachine.
	LastAppliedIndexKey = keys.MakeKey(LocalPrefix, []byte("lastAppliedIndex"))

	// The range that this statemachine holds.
	LocalRangeKey      = keys.MakeKey(LocalPrefix, []byte("range"))
	LocalRangeLeaseKey = keys.MakeKey(LocalPrefix, []byte("rangelease"))

	// When this local range was set up.
	LocalRangeSetupTimeKey = keys.MakeKey(LocalPrefix, []byte("range_initialization_time"))
)

// Error constants -- sender recognizes these errors.
var (
	RangeNotFoundMsg     = "Range not present"   // break
	RangeNotLeasedMsg    = "Range not leased"    // continue
	RangeNotCurrentMsg   = "Range not current"   // break
	RangeLeaseInvalidMsg = "Range lease invalid" // continue
)
