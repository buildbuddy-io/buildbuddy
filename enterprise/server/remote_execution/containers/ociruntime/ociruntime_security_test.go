package ociruntime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

func TestPullImageIfNecessaryReauthenticatesCachedOCIImage(t *testing.T) {
	registryCreds := &testregistry.BasicAuthCreds{
		Username: "authorized-user",
		Password: "authorized-password",
	}
	reg := testregistry.Run(t, testregistry.Opts{Creds: registryCreds})
	imageRef, _ := reg.PushNamedImageWithFiles(t, "private-image", map[string][]byte{
		"/private.txt": []byte("group A private contents"),
	}, registryCreds)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))

	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, 10_000_000_000, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()

	resolver, err := oci.NewResolver(env)
	require.NoError(t, err)
	imageStore, err := NewImageStore(resolver, filepath.Join(testfs.MakeTempDir(t), "images", "oci"), fc)
	require.NoError(t, err)

	groupACtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	groupAContainer := &ociContainer{
		imageRef:   imageRef,
		imageStore: imageStore,
	}
	err = container.PullImageIfNecessary(groupACtx, env, groupAContainer, oci.Credentials{
		Username: registryCreds.Username,
		Password: registryCreds.Password,
	}, imageRef, false)
	require.NoError(t, err)
	groupAContainer.lockedImage.Unlock()

	groupBCtx, err := ta.WithAuthenticatedUser(ctx, "US2")
	require.NoError(t, err)
	groupBContainer := &ociContainer{
		imageRef:   imageRef,
		imageStore: imageStore,
	}
	t.Cleanup(func() {
		if groupBContainer.lockedImage != nil {
			groupBContainer.lockedImage.Unlock()
		}
	})

	err = container.PullImageIfNecessary(groupBCtx, env, groupBContainer, oci.Credentials{
		Username: registryCreds.Username,
		Password: "wrong-password",
	}, imageRef, false)

	require.True(t, status.IsPermissionDeniedError(err), "cached private OCI image must still be re-authenticated for a different group; got %v", err)
}
