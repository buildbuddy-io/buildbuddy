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

func TestKnownVersions(t *testing.T) {
	versionExemplars := map[filestore.PebbleKeyVersion][]string{
		filestore.UndefinedKeyVersion: []string{
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2",
			"PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309",
		},
		filestore.Version1: []string{
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/v1",
			"PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
		},
		filestore.Version2: []string{
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/cas/v2",
			"PTFOO/GR7890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/2364854541/v2",
		},
	}

	for version := filestore.UndefinedKeyVersion; version < filestore.TestingMaxKeyVersion; version++ {
		exemplars, ok := versionExemplars[version]
		if !ok {
			t.Fatalf("Please add test exemplars for pebble key version: %d", version)
		}
		for _, exemplar := range exemplars {
			var key filestore.PebbleKey
			parsedVersion, err := key.FromBytes([]byte(exemplar))
			assert.NoError(t, err)
			assert.Equal(t, version, parsedVersion)
		}
	}
}
