package ci_runner_env

const (
	BuildBuddyAPIKeyEnvVarName       = "BUILDBUDDY_API_KEY"
	BuildBuddyInvocationIDEnvVarName = "BUILDBUDDY_INVOCATION_ID"
	BuildBuddyRunIDEnvVarName        = "BUILDBUDDY_RUN_ID"
	BuildBuddyActionNameEnvVarName   = "BUILDBUDDY_ACTION_NAME"
	BuildBuddyTriggerEventEnvVarName = "BUILDBUDDY_TRIGGER_EVENT"

	BBGrpcClientOriginEnvVarName   = "BB_GRPC_CLIENT_ORIGIN"
	BBGrpcClientIdentityEnvVarName = "BB_GRPC_CLIENT_IDENTITY"

	BuildBuddySecretEnvVarNamesForRedaction = "BUILDBUDDY_SECRET_ENV_VAR_NAMES"

	// NOTE: These env vars migiht not be populated for non-private repos.
	RepoUserEnvVarName  = "REPO_USER"
	RepoTokenEnvVarName = "REPO_TOKEN"

	// EmbedRepoTokenInRemoteURLEnvVarName, when set to "1", makes the CI
	// runner save the repo access token in the workspace's git remote URL, in
	// addition to supplying it with the git credential helper.
	//
	// This is a temporary fallback for workloads that depended on the token
	// being in the remote URL (e.g. reusing `git remote get-url origin` in
	// other steps).
	// TODO: remove once workloads have migrated off of it.
	EmbedRepoTokenInRemoteURLEnvVarName = "BUILDBUDDY_EMBED_REPO_TOKEN_IN_REMOTE_URL"
)
