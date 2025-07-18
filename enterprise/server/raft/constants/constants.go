package constants

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
)

// Gossip (broadcast) constants
const (
	GRPCAddressTag = "grpc_address"
	MetaRangeTag   = "meta_range"
	ZoneTag        = "zone"
	StoreUsageTag  = "store_usage"

	RegistryUpdateEvent       = "registry_update_event"
	RegistryQueryEvent        = "registry_query_event"
	AutoBringupEvent          = "auto_bringup_event"
	NodePartitionUsageEvent   = "node_partition_usage_event"
	PlacementDriverQueryEvent = "placement_driver_query_event"

	CacheName = "raft"
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
	InitialReplicaID = 1
	InitialRangeID   = 1
	MetaRangeID      = 1

	UnsplittableMaxByte = systemMaxByte

	EntryErrorValue = 0

	RTTMillisecond = 10
)

const (
	CASErrorMessage    = "CAS expected value did not match"
	TxnNotFoundMessage = "Transaction not found"
)

// Key constants (some of these have to be vars because of how they are made.
var (
	LocalPrefix = keys.Key{localPrefixByte}

	// MetaRangePrefix is exclusively for range descriptors.
	MetaRangePrefix = keys.Key{metaPrefixByte}

	// SystemPrefix is for data that should live in meta range but are not range
	// descriptors.
	SystemPrefix = keys.Key{systemPrefixByte}

	// System Keys:
	// The last replicaID that was generated.
	LastReplicaIDKeyPrefix = keys.MakeKey(SystemPrefix, []byte("last_replica_id"))

	// The last rangeID that was generated.
	LastRangeIDKey = keys.MakeKey(SystemPrefix, []byte("last_range_id-"))

	// A prefix to prepend to transaction records. Transaction Records were written
	// by the transaction coordinator when the transaction state changes. They
	// were used to recover stuck transactions. They are different from shard-specific
	// transaction entries written by replicas when preparing the transactions.
	TxnRecordPrefix = keys.MakeKey(SystemPrefix, []byte("txn-record-"))

	// A prefix to prepend to session keys
	SessionPrefix = keys.MakeKey(SystemPrefix, []byte("session-"))

	// Local Keys:
	// When the cluster was created.
	ClusterSetupTimeKey = keys.MakeKey(LocalPrefix, []byte("cluster_setup_time"))

	// The last index that was applied by a statemachine.
	LastAppliedIndexKey = keys.MakeKey(LocalPrefix, []byte("lastAppliedIndex"))

	// Key that contains partition metadata (size, counts).
	PartitionMetadatasKey = keys.MakeKey(LocalPrefix, []byte("partition_metadatas"))

	// The range that this statemachine holds.
	LocalRangeKey      = keys.MakeKey(LocalPrefix, []byte("range"))
	LocalRangeLeaseKey = keys.MakeKey(LocalPrefix, []byte("rangelease"))

	// When this local range was set up.
	LocalRangeSetupTimeKey = keys.MakeKey(LocalPrefix, []byte("range_initialization_time"))

	// A prefix to prepend to transactions.
	LocalTransactionPrefix = keys.MakeKey(LocalPrefix, []byte("txn-"))
)

// Error constants -- sender recognizes these errors.
var (
	RangeNotFoundMsg     = "Range not present"   // continue
	RangeNotLeasedMsg    = "Range not leased"    // continue
	RangeNotCurrentMsg   = "Range not current"   // break
	RangeLeaseInvalidMsg = "Range lease invalid" // continue
	RangeSplittingMsg    = "Range splitting"
	ConflictKeyMsg       = "Conflict on key"
)

type ReplicaState int

const (
	ReplicaStateUnknown ReplicaState = iota
	ReplicaStateCurrent
	ReplicaStateBehind
)
