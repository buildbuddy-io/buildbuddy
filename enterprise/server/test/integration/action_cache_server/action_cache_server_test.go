package action_cache_server_test

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestRestrictedPrefixes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		initIDService bool
		instanceNames []string
		clientID      string
		canWrite      bool
		canRead       bool
	}{
		{
			name:          "no id service, CAN write and read unrestricted instance names normally",
			initIDService: false,
			instanceNames: []string{"normal instance name", "", "another totally normal name"},
			clientID:      "",
			canWrite:      true,
			canRead:       true,
		},
		{
			name:          "id service, CAN write and read unrestricted instance names normally",
			initIDService: true,
			instanceNames: []string{"normal instance name", "", "another totally normal name"},
			clientID:      "",
			canWrite:      true,
			canRead:       true,
		},
		{
			name:          "no id service, CANNOT write or read restricted instance names",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      "",
			canWrite:      false,
			canRead:       false,
		},
		{
			name:          "id service, CAN write or read restricted instance names from app",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      interfaces.ClientIdentityApp,
			canWrite:      true,
			canRead:       true,
		},
		{
			name:          "id service, CAN write or read restricted instance names from executor",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      interfaces.ClientIdentityExecutor,
			canWrite:      true,
			canRead:       true,
		},
		{
			name:          "id service, CAN write or read restricted instance names from cache-proxy",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      interfaces.ClientIdentityCacheProxy,
			canWrite:      true,
			canRead:       true,
		},
		{
			name:          "id service, CANNOT write or read restricted instance names from workflow",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      interfaces.ClientIdentityWorkflow,
			canWrite:      false,
			canRead:       false,
		},
		{
			name:          "id service, CANNOT write or read restricted instance names from untrusted client",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      "untrusted",
			canWrite:      false,
			canRead:       false,
		},
		{
			name:          "id service, CANNOT write or read restricted instance names from empty client",
			initIDService: true,
			instanceNames: []string{interfaces.OCIImageInstanceNamePrefix, interfaces.OCIImageInstanceNamePrefix + "suffix"},
			clientID:      "",
			canWrite:      false,
			canRead:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			flags.Set(t, "app.client_identity.client", tc.clientID)
			if tc.initIDService {
				key, err := random.RandomString(16)
				require.NoError(t, err)
				flags.Set(t, "app.client_identity.key", string(key))
				clientidentity.Register(te)
			}
			_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
			testcache.Setup(t, te, localGRPClis)
			go runServer()

			for _, instanceName := range tc.instanceNames {
				arDigest, err := digest.Compute(bytes.NewReader([]byte(tc.name)), repb.DigestFunction_SHA256)
				require.NoError(t, err)
				acClient := te.GetActionCacheClient()
				{
					ar := &repb.ActionResult{
						ExecutionMetadata: &repb.ExecutedActionMetadata{
							Worker: tc.name,
						},
					}
					ctx := context.Background()
					if tc.initIDService {
						ctx, err = te.GetClientIdentityService().AddIdentityToContext(ctx)
						require.NoError(t, err)
					}
					uar, err := acClient.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
						InstanceName:   instanceName,
						DigestFunction: repb.DigestFunction_SHA256,
						ActionDigest:   arDigest,
						ActionResult:   ar,
					})
					if tc.canWrite {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
						require.Nil(t, uar)
						require.True(t, status.IsUnauthenticatedError(err))
					}
				}

				{
					ctx := context.Background()
					if tc.initIDService {
						ctx, err = te.GetClientIdentityService().AddIdentityToContext(ctx)
						require.NoError(t, err)
					}
					ar, err := acClient.GetActionResult(ctx, &repb.GetActionResultRequest{
						InstanceName:   instanceName,
						DigestFunction: repb.DigestFunction_SHA256,
						ActionDigest:   arDigest,
					})
					if tc.canRead {
						require.NoError(t, err)
						require.NotNil(t, ar.ExecutionMetadata)
						require.Equal(t, tc.name, ar.ExecutionMetadata.Worker)
					} else {
						require.Error(t, err)
						require.Nil(t, ar)
						require.True(t, status.IsUnauthenticatedError(err))
					}
				}
			}
		})
	}
}

func TestImageCacheWriteCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name         string
		capability   cappb.Capability
		instanceName string
		clientID     string
		wantStored   bool
		wantAuthErr  bool
	}{
		{"image prefix", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix, interfaces.ClientIdentityApp, true, false},
		{"image suffix", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix + "image/layer", interfaces.ClientIdentityExecutor, true, false},
		{"dots within component", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix + "image..name/layer", interfaces.ClientIdentityCacheProxy, true, false},
		{"ordinary instance", cappb.Capability_IMAGE_CACHE_WRITE, "ordinary", interfaces.ClientIdentityApp, false, false},
		{"empty instance", cappb.Capability_IMAGE_CACHE_WRITE, "", interfaces.ClientIdentityApp, false, false},
		{"root traversal", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix + "/../ordinary", interfaces.ClientIdentityApp, false, false},
		{"nested traversal", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix + "image/nested/../layer", interfaces.ClientIdentityApp, false, false},
		{"trailing traversal", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix + "image/..", interfaces.ClientIdentityApp, false, false},
		{"missing client identity", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix, "", false, true},
		{"workflow identity", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix, interfaces.ClientIdentityWorkflow, false, true},
		{"untrusted identity", cappb.Capability_IMAGE_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix, "untrusted", false, true},
		{"read only image", cappb.Capability_UNKNOWN_CAPABILITY, interfaces.OCIImageInstanceNamePrefix, interfaces.ClientIdentityApp, false, false},
		{"CAS writer image", cappb.Capability_CAS_WRITE, interfaces.OCIImageInstanceNamePrefix, interfaces.ClientIdentityApp, false, false},
		{"cache writer ordinary", cappb.Capability_CACHE_WRITE, "ordinary", "", true, false},
		{"cache writer image", cappb.Capability_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix, interfaces.ClientIdentityApp, true, false},
		{"cache writer traversal", cappb.Capability_CACHE_WRITE, interfaces.OCIImageInstanceNamePrefix + "/../ordinary", interfaces.ClientIdentityApp, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			te.SetAuthenticator(testauth.NewTestAuthenticator(t, nil))
			// Exercise actual filesystem path normalization for traversal cases.
			cache, err := disk_cache.NewDiskCache(te, &disk_cache.Options{RootDirectory: t.TempDir()}, 10_000_000)
			require.NoError(t, err)
			cache.WaitUntilMapped()
			te.SetCache(cache)
			flags.Set(t, "app.client_identity.client", tc.clientID)
			key, err := random.RandomString(16)
			require.NoError(t, err)
			flags.Set(t, "app.client_identity.key", key)
			clientidentity.Register(te)
			_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
			testcache.Setup(t, te, lis)
			go runServer()
			// Unlike testcache's client, this connection does not automatically
			// attach a client identity, allowing the missing-identity case.
			conn, err := testenv.LocalGRPCConn(context.Background(), lis)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			client := repb.NewActionCacheClient(conn)
			ctx := testauth.WithAuthenticatedUserInfo(context.Background(), &testauth.TestUser{
				UserID: "US1", GroupID: "GR1", Capabilities: []cappb.Capability{tc.capability},
			})
			writeCtx := ctx
			if tc.clientID != "" {
				writeCtx, err = te.GetClientIdentityService().AddIdentityToContext(ctx)
				require.NoError(t, err)
			}
			d, err := digest.Compute(bytes.NewReader([]byte(tc.name)), repb.DigestFunction_SHA256)
			require.NoError(t, err)
			result := &repb.ActionResult{StdoutRaw: []byte("cached image metadata")}
			_, err = client.UpdateActionResult(writeCtx, &repb.UpdateActionResultRequest{
				InstanceName: tc.instanceName, DigestFunction: repb.DigestFunction_SHA256,
				ActionDigest: d, ActionResult: result,
			})
			if tc.wantAuthErr {
				require.True(t, status.IsUnauthenticatedError(err), "got %v", err)
			} else {
				// Insufficient capabilities intentionally produce a no-op success.
				require.NoError(t, err)
			}

			// Always read with a trusted identity: an auth error on read would
			// otherwise hide writes that should never have been persisted.
			flags.Set(t, "app.client_identity.client", interfaces.ClientIdentityApp)
			readCtx, err := te.GetClientIdentityService().AddIdentityToContext(ctx)
			require.NoError(t, err)
			for _, instanceName := range []string{tc.instanceName, path.Clean(tc.instanceName)} {
				got, err := client.GetActionResult(readCtx, &repb.GetActionResultRequest{
					InstanceName: instanceName, DigestFunction: repb.DigestFunction_SHA256, ActionDigest: d,
				})
				if tc.wantStored {
					require.NoError(t, err)
					require.Equal(t, result.GetStdoutRaw(), got.GetStdoutRaw())
				} else {
					require.True(t, status.IsNotFoundError(err), "instance %q: got %v", instanceName, err)
				}
			}
		})
	}
}
