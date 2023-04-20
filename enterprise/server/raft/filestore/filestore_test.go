package filestore_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
				CacheType:          rspb.CacheType_AC,
				RemoteInstanceName: "remote_instance_name",
				PartitionId:        partitionID,
				GroupId:            testGroupID,
			},
			Digest: d,
		}
		if i%2 == 0 {
			fr.Isolation.CacheType = rspb.CacheType_CAS
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
			"PTdefault/GR5787812970202071253/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f",
		},
		filestore.Version1: []string{
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/v1",
			"PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
			"PTFOO/GR7890/ac/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
		},
		filestore.Version2: []string{
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/cas/v2",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/2364854541/v2",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/v2",
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

// Tests that keys have the expected format when being represented at different versions.
func TestMigration(t *testing.T) {
	cases := []map[filestore.PebbleKeyVersion]string{
		{
			filestore.UndefinedKeyVersion: "PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2",
			filestore.Version1:            "PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/v1",
			filestore.Version2:            "PTFOO/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/cas/v2",
		},
		{
			filestore.UndefinedKeyVersion: "PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309",
			filestore.Version1:            "PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
			filestore.Version2:            "PTFOO/GR00000000000000007890/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/v2",
		},
		{
			filestore.UndefinedKeyVersion: "PTdefault/GR7890/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f",
			filestore.Version1:            "PTdefault/GR7890/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/v1",
			filestore.Version2:            "PTdefault/GR00000000000000007890/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/ac/v2",
		},
	}

	for _, tc := range cases {
		for startingVersion := filestore.UndefinedKeyVersion; startingVersion < filestore.TestingMaxKeyVersion; startingVersion++ {
			key, ok := tc[startingVersion]
			if !ok {
				t.Fatalf("Please add test exemplars for pebble key version: %d", startingVersion)
			}
			var parsedKey filestore.PebbleKey
			_, err := parsedKey.FromBytes([]byte(key))
			require.NoError(t, err)

			for version := filestore.UndefinedKeyVersion; version < filestore.TestingMaxKeyVersion; version++ {
				t.Run(fmt.Sprintf("from_v%d_to_v%d", startingVersion, version), func(t *testing.T) {
					versionedKey, err := parsedKey.Bytes(version)
					require.NoError(t, err)

					expectedKey, ok := tc[version]
					if !ok {
						t.Fatalf("Please add test exemplars for pebble key version: %d", version)
					}
					require.Equal(t, expectedKey, string(versionedKey))
				})
			}
		}
	}
}

func MustParseKey(t *testing.T, ks string) *filestore.PebbleKey {
	var key filestore.PebbleKey
	_, err := key.FromBytes([]byte(ks))
	if err != nil {
		t.Fatal(err)
	}
	return &key
}

func TestLockID(t *testing.T) {
	// All versions of a key should have the same LockID.
	{
		versions := []string{
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2",
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/v1",
			"PTFOO/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/cas/v2",
		}
		control := MustParseKey(t, versions[0]).LockID()
		for i := 1; i < len(versions); i++ {
			assert.Equal(t, control, MustParseKey(t, versions[i]).LockID())
		}
	}
	// Different users (with same remote instance name) should have different LockID
	{
		versions := []string{
			"PTFOO/GR1/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309",
			"PTFOO/GR2/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
			"PTFOO/GR3/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/v2",
		}
		uniqueLocks := make(map[string]struct{}, 0)
		for _, version := range versions {
			l := MustParseKey(t, version).LockID()
			assert.NotContains(t, uniqueLocks, l)
			uniqueLocks[l] = struct{}{}
		}
	}
	// Same users (with same remote instance name) should have same LockID
	{
		versions := []string{
			"PTFOO/GR1/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309",
			"PTFOO/GR1/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
			"PTFOO/GR1/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/v2",
		}
		control := MustParseKey(t, versions[0]).LockID()
		for i := 1; i < len(versions); i++ {
			assert.Equal(t, control, MustParseKey(t, versions[i]).LockID())
		}
	}
	// Same users (with different remote instance name) should have different LockID
	{
		versions := []string{
			"PTFOO/GR1/ac/1212121212/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309",
			"PTFOO/GR1/ac/2323232323/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
			"PTFOO/GR1/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/4545454545/v2",
		}
		uniqueLocks := make(map[string]struct{}, 0)
		for _, version := range versions {
			l := MustParseKey(t, version).LockID()
			assert.NotContains(t, uniqueLocks, l)
			uniqueLocks[l] = struct{}{}
		}
	}
}
