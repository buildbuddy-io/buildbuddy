package smoke_test

import (
	"cmp"
	"flag"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/smoke/executorsmoke"
)

var (
	appTarget      = flag.String("app_target", "", "gRPC target of the BuildBuddy app the executor should register with, e.g. grpcs://remote.buildbuddy.dev.")
	apiKey         = flag.String("api_key", "", "API key used to run actions. Defaults to $EXECUTOR_SMOKE_API_KEY.")
	executorAPIKey = flag.String("executor_api_key", "", "API key the executor registers with. Defaults to $EXECUTOR_SMOKE_EXECUTOR_API_KEY, then to --api_key.")
	groupID        = flag.String("group_id", "", "ID of the group owning the API keys. If set, registration details are verified via GetExecutionNodes, which requires an ORG_ADMIN key.")
	adminAPIKey    = flag.String("admin_api_key", "", "API key with ORG_ADMIN capability, used with --group_id. Defaults to $EXECUTOR_SMOKE_ADMIN_API_KEY, then to --api_key.")
	instanceName   = flag.String("instance_name", "", "Remote instance name.")
)

// TestExecutorSmoke smoke tests an executor binary against an existing
// BuildBuddy app. See README.md for usage.
func TestExecutorSmoke(t *testing.T) {
	if *appTarget == "" {
		t.Fatal("--app_target is required")
	}
	key := cmp.Or(*apiKey, os.Getenv("EXECUTOR_SMOKE_API_KEY"))
	executorsmoke.Run(t, executorsmoke.Target{
		AppTarget:      *appTarget,
		APIKey:         key,
		ExecutorAPIKey: cmp.Or(*executorAPIKey, os.Getenv("EXECUTOR_SMOKE_EXECUTOR_API_KEY"), key),
		AdminAPIKey:    cmp.Or(*adminAPIKey, os.Getenv("EXECUTOR_SMOKE_ADMIN_API_KEY")),
		GroupID:        *groupID,
		InstanceName:   *instanceName,
	}, executorsmoke.OptionsFromFlags())
}
