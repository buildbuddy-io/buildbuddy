package filestore_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

func TestKeyVersionCrossCompatibility(t *testing.T) {
	testGroupID := "GR7890"
	partitionID := "FOO"
	fs := filestore.New(filestore.Opts{})

	// What we are testing here is that for every version a key can be
	// written at, it can also be re-read and rewritten at every other version.
	for i := filestore.UndefinedKeyVersion; i < filestore.TestingMaxKeyVersion; i++ {
		d, _ := testdigest.NewRandomDigestBuf(t, 100)
		fr := &rfpb.FileRecord{
			Isolation: &rfpb.Isolation{
				CacheType:          resource.CacheType_AC,
				RemoteInstanceName: "remote_instance_name",
				PartitionId:        partitionID,
				GroupId:            testGroupID,
			},
			Digest: d,
		}
		if i%2 == 0 {
			fr.Isolation.CacheType = resource.CacheType_CAS
		}
		sourceKey, err := fs.PebbleKey(fr)
		require.NoError(t, err)

		keyBytes, err := sourceKey.Bytes(i)
		require.NoError(t, err)

		for j := filestore.UndefinedKeyVersion; j < filestore.TestingMaxKeyVersion; j++ {
			parsedKey := &filestore.PebbleKey{}
			parsedVersion, err := parsedKey.FromBytes(keyBytes)

			assert.NoError(t, err)
			assert.Equal(t, i, parsedVersion)
			assert.Equal(t, sourceKey.String(), parsedKey.String())

			_, err = parsedKey.Bytes(j)
			require.NoError(t, err)
		}
	}
}
