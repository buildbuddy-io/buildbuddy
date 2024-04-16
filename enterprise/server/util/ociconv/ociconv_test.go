package ociconv_test

import (
	"context"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestOciconv(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	os.Setenv("REGISTRY_AUTH_FILE", "_null")
	_, err := ociconv.CreateDiskImage(ctx, nil /*=docker*/, root, "gcr.io/flame-public/test-alpine@sha256:6457d53fb065d6f250e1504b9bc42d5b6c65941d57532c072d929dd0628977d0", oci.Credentials{})
	require.NoError(t, err)
}
