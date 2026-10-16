package kms_test

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func writeKey(t *testing.T, dir, id string) string {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	testfs.WriteFile(t, dir, id, string(key))
	return "local-insecure-kms://" + id
}

func TestMAC(t *testing.T) {
	kmsDir := testfs.MakeTempDir(t)
	flags.Set(t, "keystore.local_insecure_kms_directory", kmsDir)
	keyURI := writeKey(t, kmsDir, "key")
	otherKeyURI := writeKey(t, kmsDir, "other-key")
	ctx := context.Background()

	k, err := kms.NewWithKeyURI(ctx, keyURI)
	require.NoError(t, err)

	mac, err := k.MAC(ctx, keyURI, []byte("data"))
	require.NoError(t, err)
	require.Len(t, mac, 32)

	again, err := k.MAC(ctx, keyURI, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, mac, again)

	otherData, err := k.MAC(ctx, keyURI, []byte("other data"))
	require.NoError(t, err)
	require.NotEqual(t, mac, otherData)

	otherKey, err := k.MAC(ctx, otherKeyURI, []byte("data"))
	require.NoError(t, err)
	require.NotEqual(t, mac, otherKey)

	_, err = k.MAC(ctx, "local-insecure-kms://missing", []byte("data"))
	require.Error(t, err)

	_, err = k.MAC(ctx, "gcp-kms://projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1", []byte("data"))
	require.Error(t, err)
}
