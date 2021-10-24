package constants

import (
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const (
	NodeHostIDTag  = "node_host_id"
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
	LocalPrefix  = keys.Key{localPrefixByte}
	SystemPrefix = keys.Key{systemPrefixByte}

	NextClusterIDKey        = keys.MakeKey(SystemPrefix, []byte("last_cluster_id"))
	InitClusterSetupTimeKey = keys.MakeKey(SystemPrefix, []byte("initial_cluster_initialization_time"))
)

// TODO(tylerw): This is obviously not a "constant". Move it to the write place.
func FilePath(fileDir string, r *rfpb.FileRecord) (string, error) {
	// This function cannot change without a data migration.
	// filepaths look like this:
	//   // {rootDir}/{groupID}/{ac|cas}/{hashPrefix:4}/{hash}
	//   // for example:
	//   //   /bb/files/GR123456/ac/abcd/abcd12345asdasdasd123123123asdasdasd
	segments := make([]string, 0, 5)
	segments = append(segments, fileDir)

	if r.GetGroupId() == "" {
		return "", status.FailedPreconditionError("Empty group ID not allowed in filerecord.")
	}
	segments = append(segments, r.GetGroupId())

	if r.GetIsolation().GetCacheType() == rfpb.Isolation_CAS_CACHE {
		segments = append(segments, "cas")
	} else if r.GetIsolation().GetCacheType() == rfpb.Isolation_ACTION_CACHE {
		segments = append(segments, "ac")
	} else {
		return "", status.FailedPreconditionError("Isolation type must be explicitly set, not UNKNOWN.")
	}
	if len(r.GetDigest().GetHash()) > 4 {
		segments = append(segments, r.GetDigest().GetHash()[:4])
	} else {
		return "", status.FailedPreconditionError("Malformed digest; too short.")
	}
	segments = append(segments, r.GetDigest().GetHash())
	return filepath.Join(segments...), nil
}
