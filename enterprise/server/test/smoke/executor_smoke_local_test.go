package smoke_test

import (
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/smoke/executorsmoke"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
)

// set by x_defs in BUILD file
var executorRlocationpath string

// TestExecutorSmoke_LocalApp runs the smoke test suite against an executor
// registered with a test-scoped app. By default, the executor is built from
// source; pass --executor_binary to test another binary instead.
func TestExecutorSmoke_LocalApp(t *testing.T) {
	app := buildbuddy_enterprise.Run(
		t,
		"--remote_execution.enable_remote_exec=true",
		"--remote_execution.require_executor_authorization=true",
		"--remote_execution.enable_user_owned_executors=true",
	)
	wc := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)
	createKey := func(caps ...cappb.Capability) string {
		rsp := &akpb.CreateApiKeyResponse{}
		err := wc.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
			RequestContext: wc.RequestContext,
			Capability:     caps,
		}, rsp)
		require.NoError(t, err)
		return rsp.GetApiKey().GetValue()
	}

	opts := executorsmoke.OptionsFromFlags()
	if opts.ExecutorBinary == "" {
		path, err := runfiles.Rlocation(executorRlocationpath)
		require.NoError(t, err)
		opts.ExecutorBinary = path
	}
	executorsmoke.Run(t, executorsmoke.Target{
		AppTarget:      app.GRPCAddress(),
		APIKey:         createKey(cappb.Capability_CACHE_WRITE),
		ExecutorAPIKey: createKey(cappb.Capability_REGISTER_EXECUTOR),
		AdminAPIKey:    createKey(cappb.Capability_ORG_ADMIN),
		GroupID:        wc.RequestContext.GetGroupId(),
	}, opts)
}
