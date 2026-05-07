package execution_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

// TestOLAPExecToClientProto_ExecuteResponseDigest_Invariant locks in that
// OLAPExecToClientProto produces an ExecuteResponseDigest equal to
// digest.Compute(originalExecutionID), even though the function reconstructs
// the execution ID from the split columns instead of reading the dropped
// execution_id column. If this ever diverges, clients will fail to fetch the
// cached ExecuteResponse from the Action Cache (proto/execution_stats.proto:99).
func TestOLAPExecToClientProto_ExecuteResponseDigest_Invariant(t *testing.T) {
	const sha256Hash = "072d9dd55aacaa829d7d1cc9ec8c4b5180ef49acac4a3c2f3ca16a3db134982d"

	for _, tc := range []struct {
		name           string
		instanceName   string
		digestFunction repb.DigestFunction_Value
		compressor     repb.Compressor_Value
		sizeBytes      int64
	}{
		{name: "OldStyleSha256", instanceName: "instance_name", digestFunction: repb.DigestFunction_SHA256, compressor: repb.Compressor_IDENTITY, sizeBytes: 1234},
		{name: "Blake3", instanceName: "instance_name", digestFunction: repb.DigestFunction_BLAKE3, compressor: repb.Compressor_IDENTITY, sizeBytes: 9876},
		{name: "Sha256Zstd", instanceName: "instance_name", digestFunction: repb.DigestFunction_SHA256, compressor: repb.Compressor_ZSTD, sizeBytes: 4321},
		{name: "Blake3Zstd", instanceName: "instance_name", digestFunction: repb.DigestFunction_BLAKE3, compressor: repb.Compressor_ZSTD, sizeBytes: 55},
		{name: "EmptyInstanceName", instanceName: "", digestFunction: repb.DigestFunction_SHA256, compressor: repb.Compressor_IDENTITY, sizeBytes: 7},
		{name: "InstanceNameContainingUploads", instanceName: "bad/uploads/instance", digestFunction: repb.DigestFunction_SHA256, compressor: repb.Compressor_IDENTITY, sizeBytes: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rn := digest.NewCASResourceName(
				&repb.Digest{Hash: sha256Hash, SizeBytes: tc.sizeBytes},
				tc.instanceName,
				tc.digestFunction,
			)
			rn.SetCompressor(tc.compressor)
			executionID := rn.NewUploadString()

			expected, err := digest.Compute(strings.NewReader(executionID), tc.digestFunction)
			require.NoError(t, err)

			row := &olaptables.Execution{}
			require.NoError(t, clickhouse.FillExecutionResourceFieldsFromExecutionID(row, executionID))

			out, err := execution.OLAPExecToClientProto(row)
			require.NoError(t, err)

			require.Equal(t, executionID, out.GetExecutionId())
			require.Equal(t, expected.GetHash(), out.GetExecuteResponseDigest().GetHash())
			require.Equal(t, expected.GetSizeBytes(), out.GetExecuteResponseDigest().GetSizeBytes())
		})
	}
}
