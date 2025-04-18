package action_cache_server_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

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
