package firecracker_test

import (
	"bytes"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

// Set via x_defs to $(rlocationpath @busybox), the OCI image, not the
// preconverted busybox.ext4 used by the container-level tests.
var busyboxOCIImageRlocationpath string

func pushRegistryBusybox(t *testing.T, registry *testregistry.Registry, name string, creds *testregistry.BasicAuthCreds) string {
	t.Helper()
	image := testregistry.ImageFromRlocationpath(t, busyboxOCIImageRlocationpath)
	digest, err := image.Digest()
	require.NoError(t, err)
	registry.Push(t, image, name, creds)
	return "docker://" + registry.ImageAddress(name) + "@" + digest.String()
}

func testFirecrackerColdImageCache(t *testing.T, rbe *firecrackerEnv) {
	var blobReads atomic.Int64
	registry := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
				blobReads.Add(1)
			}
			return true
		},
	})
	t.Cleanup(func() { require.NoError(t, registry.Shutdown()) })

	// The host executor pulls the image, not the guest. The registry's
	// loopback listener is therefore reachable without guest networking.
	// Its fresh port/reference prevents hitting a preconverted image entry.
	imageRef := pushRegistryBusybox(t, registry, "cold-busybox", nil)
	blobReads.Store(0)
	inputDir := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, inputDir, map[string]string{"world.txt": "world"})
	command := firecrackerCommand(
		`printf '%s %s' "$GREETING" "$(cat world.txt)"; printf 'guest stderr' >&2; printf 'converted image works' > output.txt`,
		&repb.Platform_Property{Name: "container-image", Value: imageRef},
		&repb.Platform_Property{Name: "recycle-runner", Value: "false"},
	)
	command.EnvironmentVariables = []*repb.Command_EnvironmentVariable{{Name: "GREETING", Value: "Hello"}}
	command.OutputFiles = []string{"output.txt"}

	var coldBlobReads int64
	for _, phase := range []string{"cold", "warm"} {
		t.Run(phase, func(t *testing.T) {
			result := rbe.Execute(command, &rbetest.ExecuteOpts{
				TestingT:         t,
				APIKey:           rbe.APIKey1,
				InputRootDir:     inputDir,
				ActionTimeout:    30 * time.Second,
				DoNotCacheAction: true,
			}).Wait()
			require.Equal(t, 0, result.ExitCode, "stderr: %s", result.Stderr)
			require.Equal(t, "Hello world", result.Stdout)
			require.Equal(t, "guest stderr", result.Stderr)
			require.NotEmpty(t, result.ActionResult.GetExecutionMetadata().GetWorker())
			// Fetch using the authenticated group's CAS namespace.
			require.Len(t, result.ActionResult.GetOutputFiles(), 1)
			output := result.ActionResult.GetOutputFiles()[0]
			require.Equal(t, "output.txt", output.GetPath())
			var contents bytes.Buffer
			err := cachetools.GetBlob(rbe.WithAPIKey(t.Context(), rbe.APIKey1), rbe.GetByteStreamClient(),
				digest.NewCASResourceName(output.GetDigest(), result.InstanceName, repb.DigestFunction_SHA256), &contents)
			require.NoError(t, err)
			require.Equal(t, "converted image works", contents.String())
			if phase == "cold" {
				coldBlobReads = blobReads.Load()
				require.Positive(t, coldBlobReads, "cold execution must fetch the OCI image")
			} else {
				// Action caching and VM recycling are disabled: the second
				// action boots a VM using the converted image cache.
				require.Equal(t, coldBlobReads, blobReads.Load(), "warm execution must not download image blobs again")
			}
		})
	}
}

// otherRegistryGroupAPIKey uses one of rbetest's existing random groups rather
// than weakening the app authenticator or substituting executor-side auth.
func otherRegistryGroupAPIKey(t *testing.T, rbe *rbetest.Env) string {
	t.Helper()
	group := &tables.Group{}
	err := rbe.GetDBHandle().NewQuery(t.Context(), "firecracker_test_other_group").Raw(
		`SELECT group_id, user_id FROM Groups WHERE group_id != ? AND user_id != '' LIMIT 1`,
		rbe.GroupID1,
	).Take(group)
	require.NoError(t, err)
	require.NotEqual(t, rbe.GroupID1, group.GroupID)
	ctx := rbe.WithUserID(t.Context(), group.UserID)
	response, err := rbe.GetBuildBuddyServiceClient().CreateApiKey(ctx, &akpb.CreateApiKeyRequest{
		RequestContext: &ctxpb.RequestContext{
			UserId:  &uidpb.UserId{Id: group.UserID},
			GroupId: group.GroupID,
		},
		Label: "firecracker-private-registry-test",
		Capability: []cappb.Capability{
			cappb.Capability_CAS_WRITE,
			cappb.Capability_CACHE_WRITE,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, response.GetApiKey().GetValue())
	return response.GetApiKey().GetValue()
}

func testFirecrackerCachedPrivateImageRequiresAuthorization(t *testing.T, rbe *firecrackerEnv) {
	otherAPIKey := otherRegistryGroupAPIKey(t, rbe.Env)

	creds := &testregistry.BasicAuthCreds{Username: "registry-user", Password: "registry-password"}
	var unauthorizedRequests atomic.Int64
	registry := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			username, password, _ := r.BasicAuth()
			if username != creds.Username || password != creds.Password {
				unauthorizedRequests.Add(1)
			}
			return true
		},
	})
	t.Cleanup(func() { require.NoError(t, registry.Shutdown()) })
	imageRef := pushRegistryBusybox(t, registry, "private-busybox", creds)
	inputDir := testfs.MakeTempDir(t)

	execute := func(t *testing.T, apiKey string, credentials *testregistry.BasicAuthCreds) *rbetest.Command {
		t.Helper()
		properties := []*repb.Platform_Property{
			{Name: "container-image", Value: imageRef},
			{Name: "recycle-runner", Value: "false"},
		}
		if credentials != nil {
			properties = append(properties,
				&repb.Platform_Property{Name: "container-registry-username", Value: credentials.Username},
				&repb.Platform_Property{Name: "container-registry-password", Value: credentials.Password},
			)
		}
		return rbe.Execute(firecrackerCommand(`printf 'private image executed'`, properties...), &rbetest.ExecuteOpts{
			TestingT:         t,
			APIKey:           apiKey,
			InputRootDir:     inputDir,
			ActionTimeout:    30 * time.Second,
			DoNotCacheAction: true,
		})
	}

	// Warm the converted image and authorization token as the first group.
	warm := execute(t, rbe.APIKey1, creds).Wait()
	require.Equal(t, 0, warm.ExitCode, "stderr: %s", warm.Stderr)
	require.Equal(t, "private image executed", warm.Stdout)
	worker := warm.ActionResult.GetExecutionMetadata().GetWorker()
	require.NotEmpty(t, worker)

	// A token intentionally authorizes the whole group for a short time.
	// Anonymous callers and a different group must authenticate even though
	// the image is cached.
	for _, tc := range []struct {
		name   string
		apiKey string
		creds  *testregistry.BasicAuthCreds
	}{
		{name: "anonymous"},
		{name: "other_group_missing_credentials", apiKey: otherAPIKey},
		{name: "other_group_wrong_credentials", apiKey: otherAPIKey, creds: &testregistry.BasicAuthCreds{Username: creds.Username, Password: "wrong"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unauthorizedRequests.Store(0)
			// MustFailToStart checks execution_metadata.worker, unlike
			// MustFailToSchedule: scheduler rejection cannot pass this test.
			err := execute(t, tc.apiKey, tc.creds).MustFailToStart()
			// Runner setup wraps image-pull errors in Unavailable. Keep the
			// registry-auth cause strict instead of accepting any setup failure.
			require.True(t, status.IsUnavailableError(err), "expected runner setup error, got %s", err)
			require.Contains(t, status.Message(err), "authenticate with registry")
			require.Contains(t, status.Message(err), "remote registry HTTP status 401")
			require.Positive(t, unauthorizedRequests.Load(), "the executor must attempt registry authorization")
		})
	}

	// Valid credentials let the second group execute on the same executor.
	// This also guards against confusing pool isolation with image auth.
	authorized := execute(t, otherAPIKey, creds).Wait()
	require.Equal(t, 0, authorized.ExitCode, "stderr: %s", authorized.Stderr)
	require.Equal(t, "private image executed", authorized.Stdout)
	require.Equal(t, worker, authorized.ActionResult.GetExecutionMetadata().GetWorker())
}
