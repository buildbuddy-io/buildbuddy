package filestore_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

// These constants are presisted, so care must be taken to not change them.
func TestKeyVersionDefinitions(t *testing.T) {
	assert.Equal(t, -1, int(filestore.UnspecifiedKeyVersion))
	assert.Equal(t, 0, int(filestore.UndefinedKeyVersion))
	assert.Equal(t, 1, int(filestore.Version1))
	assert.Equal(t, 2, int(filestore.Version2))
	assert.Equal(t, 3, int(filestore.Version3))
	assert.Equal(t, 4, int(filestore.Version4))
	assert.Equal(t, 5, int(filestore.Version5))
	assert.Equal(t, 6, int(filestore.Version6))
	assert.Equal(t, 7, int(filestore.MaxKeyVersion))
}

func toFileRecord(r *rspb.ResourceName) *sgpb.FileRecord {
	return &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:          r.GetCacheType(),
			RemoteInstanceName: r.GetInstanceName(),
			PartitionId:        "FOO",
			GroupId:            "GR7890",
		},
		Digest:         r.GetDigest(),
		DigestFunction: r.GetDigestFunction(),
	}
}

func getRandomKeyBytes(t *testing.T, fs filestore.Store, i filestore.PebbleKeyVersion, cacheType rspb.CacheType) ([]byte, *sgpb.FileRecord) {
	r, _ := testdigest.NewRandomResourceAndBuf(t, 100, cacheType, "remote_instance_name")
	fr := toFileRecord(r)
	sourceKey, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	keyBytes, err := sourceKey.Bytes(i)
	require.NoError(t, err)
	return keyBytes, fr
}

func TestKeyVersionCrossCompatibility(t *testing.T) {
	fs := filestore.New()

	type testCase struct {
		fromVersion filestore.PebbleKeyVersion
		toVersion   filestore.PebbleKeyVersion
		cacheType   rspb.CacheType
		wantErr     error
	}

	// generate test cases:
	// A key with an earlier version can be re-read and written as a higher
	// version key, up to v5.
	// AC entries can be converted to v6 from an earlier version.
	// CAS entries cannot be converted to v6 from an earlier version because
	// group ID info is lost.
	//
	testCases := []testCase{}
	for i := filestore.UndefinedKeyVersion; i < filestore.MaxKeyVersion; i++ {
		for _, cacheType := range []rspb.CacheType{rspb.CacheType_AC, rspb.CacheType_CAS} {
			maxCASMigratableVersion := filestore.Version5
			if i >= filestore.Version6 {
				maxCASMigratableVersion = filestore.MaxKeyVersion - 1
			}
			for j := i; j < filestore.MaxKeyVersion; j++ {
				tc := testCase{
					fromVersion: i,
					toVersion:   j,
					cacheType:   cacheType,
					wantErr:     nil,
				}
				if j > maxCASMigratableVersion && cacheType == rspb.CacheType_CAS {
					tc.wantErr = filestore.ErrMissingKeyInfo
				}
				testCases = append(testCases, tc)
			}
		}
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("from_v%d_to_v%d_cache_type_%s", tc.fromVersion, tc.toVersion, tc.cacheType), func(t *testing.T) {
			r, _ := testdigest.NewRandomResourceAndBuf(t, 100, tc.cacheType, "remote_instance_name")
			fr := toFileRecord(r)
			sourceKey, err := fs.PebbleKey(fr)
			require.NoError(t, err)
			keyBytes, err := sourceKey.Bytes(tc.fromVersion)
			require.NoError(t, err)

			parsedKey := &filestore.PebbleKey{}
			parsedVersion, err := parsedKey.FromBytes(keyBytes)
			assert.NoError(t, err)
			assert.Equal(t, tc.fromVersion, parsedVersion)
			parsedKeyBytes, err := parsedKey.Bytes(tc.toVersion)
			if tc.wantErr == nil {
				require.NoError(t, err)
				expectedKeyBytes, err := sourceKey.Bytes(tc.toVersion)
				require.NoError(t, err)

				// The same file record should be parsed to the same key.
				assert.True(t, bytes.Equal(expectedKeyBytes, parsedKeyBytes), "expected %q, actual: %q", expectedKeyBytes, parsedKeyBytes)
			} else {
				assert.ErrorIs(t, err, tc.wantErr, "expected: %s, actual: %s", tc.wantErr, err)
			}
		})
	}
}

func TestKnownVersions(t *testing.T) {
	versionExemplars := map[filestore.PebbleKeyVersion][]string{
		filestore.UndefinedKeyVersion: {
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2",
			"PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309",
			"PTdefault/GR5787812970202071253/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f",
		},
		filestore.Version1: {
			"PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/v1",
			"PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
			"PTFOO/GR7890/ac/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1",
		},
		filestore.Version2: {
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/cas/v2",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/2364854541/v2",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/v2",
		},
		filestore.Version3: {
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/cas/v3",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/cas/EK123/v3",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/2364854541/v3",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/2364854541/EK123/v3",
			"PTFOO/GR00000000000000007890/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/0/v3",
			"PTFOO/ANON/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/ac/0/v3",
		},
		filestore.Version4: {
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/v4",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/EK123/v4",
			"PTFOO/GR74042147050500190371/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/2364854541/v4",
			"PTFOO/GR74042147050500190371/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/2364854541/EK123/v4",
			"PTFOO/GR74042147050500190371/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/0/v4",
		},
		filestore.Version5: {
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/v5",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/EK123/v5",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/v5",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/EK123/v5",
		},
		filestore.Version6: {
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/v6",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/cas/EK123/v6",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/v6",
			"PTFOO/9c1385f58c3caf4a21a2626217c86303a9d157603d95eb6799811abb12ebce6b/9/ac/EK123/v6",
		},
	}

	for version := filestore.UndefinedKeyVersion; version < filestore.MaxKeyVersion; version++ {
		exemplars, ok := versionExemplars[version]
		if !ok {
			t.Fatalf("Please add test exemplars for pebble key version: %d", version)
		}
		for _, exemplar := range exemplars {
			var key filestore.PebbleKey
			parsedVersion, err := key.FromBytes([]byte(exemplar))
			assert.NoError(t, err)
			assert.Equal(t, version, parsedVersion)
			reSerialized, err := key.Bytes(parsedVersion)
			assert.NoError(t, err)
			assert.Equal(t, string(exemplar), string(reSerialized))
		}
	}
}

// Tests that keys have the expected format when being represented at different versions.
func TestMigration(t *testing.T) {
	type result struct {
		key     string
		wantErr error
	}
	cases := []map[filestore.PebbleKeyVersion]result{
		{
			filestore.UndefinedKeyVersion: {key: "PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2"},
			filestore.Version1:            {key: "PTFOO/cas/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/v1"},
			filestore.Version2:            {key: "PTFOO/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/cas/v2"},
			filestore.Version3:            {key: "PTFOO/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/cas/v3"},
			filestore.Version4:            {key: "PTFOO/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/1/cas/v4"},
			filestore.Version5:            {key: "PTFOO/baec85817b2bf76db939f38e33f1acccdfeb5683885d014717918bbc0c1996d2/1/cas/v5"},
			filestore.Version6:            {wantErr: filestore.ErrMissingKeyInfo},
		},
		{
			filestore.UndefinedKeyVersion: {key: "PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309"},
			filestore.Version1:            {key: "PTFOO/GR7890/ac/2364854541/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/v1"},
			filestore.Version2:            {key: "PTFOO/GR00000000000000007890/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/v2"},
			filestore.Version3:            {key: "PTFOO/GR00000000000000007890/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/v3"},
			filestore.Version4:            {key: "PTFOO/GR00000000000000007890/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/1/ac/2364854541/v4"},
			filestore.Version5:            {key: "PTFOO/8d35507f8943fa0242206a2077054c0ead9c71ba9f5d01c6b7f3578c6d4ba464/1/ac/v5"},
			filestore.Version6:            {key: "PTFOO/8d35507f8943fa0242206a2077054c0ead9c71ba9f5d01c6b7f3578c6d4ba464/1/ac/v6"},
		},
		{
			filestore.UndefinedKeyVersion: {key: "PTdefault/GR7890/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f"},
			filestore.Version1:            {key: "PTdefault/GR7890/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/v1"},
			filestore.Version2:            {key: "PTdefault/GR00000000000000007890/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/ac/v2"},
			filestore.Version3:            {key: "PTdefault/GR00000000000000007890/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/ac/0/v3"},
			filestore.Version4:            {key: "PTdefault/GR00000000000000007890/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/1/ac/0/v4"},
			filestore.Version5:            {key: "PTdefault/1e664dcc33e701dc7d59472f93f110159ca128b5e52e4e849d6eefeb4bda7f72/1/ac/v5"},
			filestore.Version6:            {key: "PTdefault/1e664dcc33e701dc7d59472f93f110159ca128b5e52e4e849d6eefeb4bda7f72/1/ac/v6"},
		},
		{
			filestore.UndefinedKeyVersion: {key: "PTdefault/ANON/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f"},
			filestore.Version1:            {key: "PTdefault/ANON/ac/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/v1"},
			filestore.Version2:            {key: "PTdefault/ANON/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/ac/v2"},
			filestore.Version3:            {key: "PTdefault/ANON/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/ac/0/v3"},
			filestore.Version4:            {key: "PTdefault/GR74042147050500190371/ffb4ed9aea57f797c92a1a8ea784dde745becc35ca60315cb14f3a3db772939f/1/ac/0/v4"},
			filestore.Version5:            {key: "PTdefault/4929cde6f4c93cd5ab0ffde947e7e5da09bdb0677057c2ae7ea75b083c67feaa/1/ac/v5"},
			filestore.Version6:            {key: "PTdefault/4929cde6f4c93cd5ab0ffde947e7e5da09bdb0677057c2ae7ea75b083c67feaa/1/ac/v6"},
		},
	}

	for _, tc := range cases {
		for startingVersion := filestore.UndefinedKeyVersion; startingVersion < filestore.MaxKeyVersion; startingVersion++ {
			testCase, ok := tc[startingVersion]
			if !ok {
				t.Fatalf("Please add test exemplars for pebble key version: %d", startingVersion)
			}
			var parsedKey filestore.PebbleKey
			if testCase.key == "" {
				continue
			}
			_, err := parsedKey.FromBytes([]byte(testCase.key))
			require.NoError(t, err)

			for version := startingVersion; version < filestore.MaxKeyVersion; version++ {
				t.Run(fmt.Sprintf("from_v%d_to_v%d", startingVersion, version), func(t *testing.T) {
					expected, ok := tc[version]
					if !ok {
						t.Fatalf("Please add test exemplars for pebble key version: %d", version)
					}
					versionedKey, err := parsedKey.Bytes(version)
					if expected.wantErr == nil {
						require.Equal(t, expected.key, string(versionedKey))
					} else {
						require.ErrorIs(t, err, expected.wantErr)
					}
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

func formatKey(t *testing.T, fr *sgpb.FileRecord, version filestore.PebbleKeyVersion) string {
	fs := filestore.New()
	pk, err := fs.PebbleKey(fr)
	require.NoError(t, err)
	bs, err := pk.Bytes(version)
	require.NoError(t, err)
	return string(bs)
}

func TestVersion3(t *testing.T) {
	partitionID := "foo"
	groupID := "GR123"
	d := &repb.Digest{Hash: "647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309", SizeBytes: 123}

	// AC
	{
		// AC w/o instance name.
		fr := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:          rspb.CacheType_AC,
				RemoteInstanceName: "",
				PartitionId:        partitionID,
				GroupId:            groupID,
			},
			Digest:         d,
			DigestFunction: repb.DigestFunction_SHA256,
		}
		assert.Equal(t, "PTfoo/GR00000000000000000123/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/0/v3", formatKey(t, fr, filestore.Version3))

		// AC w/ instance name.
		fr.Isolation.RemoteInstanceName = "remote_instance_name"
		assert.Equal(t, "PTfoo/GR00000000000000000123/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/v3", formatKey(t, fr, filestore.Version3))

		// AC w/ instance name & encryption.
		fr.Encryption = &sgpb.Encryption{KeyId: "EK456"}
		assert.Equal(t, "PTfoo/GR00000000000000000123/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/ac/2364854541/EK456/v3", formatKey(t, fr, filestore.Version3))
	}

	// CAS
	{
		// CAS w/o encryption
		fr := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   rspb.CacheType_CAS,
				PartitionId: partitionID,
				GroupId:     groupID,
			},
			Digest:         d,
			DigestFunction: repb.DigestFunction_SHA256,
		}
		assert.Equal(t, "PTfoo/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/cas/v3", formatKey(t, fr, filestore.Version3))

		// CAS w/ encryption
		fr.Encryption = &sgpb.Encryption{KeyId: "EK456"}
		assert.Equal(t, "PTfoo/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/cas/EK456/v3", formatKey(t, fr, filestore.Version3))
	}
}

func TestVersion5(t *testing.T) {
	partitionID := "foo"
	groupID := "GR123"
	d := &repb.Digest{Hash: "647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309", SizeBytes: 123}

	// AC
	{
		// AC w/o instance name.
		fr := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:          rspb.CacheType_AC,
				RemoteInstanceName: "",
				PartitionId:        partitionID,
				GroupId:            groupID,
			},
			Digest:         d,
			DigestFunction: repb.DigestFunction_SHA256,
		}
		assert.Equal(t, "PTfoo/72879509a94331dd1daab801d58eb1e5a6523097150916aeaee4c584d46de5ea/1/ac/v5", formatKey(t, fr, filestore.Version5))

		// AC w/ instance name.
		fr.Isolation.RemoteInstanceName = "remote_instance_name"
		assert.Equal(t, "PTfoo/7f9486526761dd87bc045a9fa4637d01142f13760a0b656991509baa720d0883/1/ac/v5", formatKey(t, fr, filestore.Version5))

		// AC w/ instance name & encryption.
		fr.Encryption = &sgpb.Encryption{KeyId: "EK456"}
		assert.Equal(t, "PTfoo/7f9486526761dd87bc045a9fa4637d01142f13760a0b656991509baa720d0883/1/ac/EK456/v5", formatKey(t, fr, filestore.Version5))
	}

	// CAS
	{
		// CAS w/o encryption
		fr := &sgpb.FileRecord{
			Isolation: &sgpb.Isolation{
				CacheType:   rspb.CacheType_CAS,
				PartitionId: partitionID,
				GroupId:     groupID,
			},
			Digest:         d,
			DigestFunction: repb.DigestFunction_SHA256,
		}
		assert.Equal(t, "PTfoo/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/1/cas/v5", formatKey(t, fr, filestore.Version5))

		// CAS w/ encryption
		fr.Encryption = &sgpb.Encryption{KeyId: "EK456"}
		assert.Equal(t, "PTfoo/647c5961cba680d5deeba0169a64c8913d6b5b77495a1ee21c808ac6a514f309/1/cas/EK456/v5", formatKey(t, fr, filestore.Version5))
	}
}
