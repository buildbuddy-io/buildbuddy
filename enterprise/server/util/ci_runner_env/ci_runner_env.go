package ci_runner_env

const (
	BuildBuddyAPIKeyEnvVarName       = "BUILDBUDDY_API_KEY"
	BuildBuddyInvocationIDEnvVarName = "BUILDBUDDY_INVOCATION_ID"
	BuildBuddyRunIDEnvVarName        = "BUILDBUDDY_RUN_ID"

	// Targets of the app server that started the run. These allow `bb` commands
	// run in a step to target that app rather than the public defaults.
	BuildBuddyAPITargetEnvVarName  = "BUILDBUDDY_API_TARGET"
	BuildBuddyHTTPTargetEnvVarName = "BUILDBUDDY_HTTP_TARGET"

	BBGrpcClientOriginEnvVarName   = "BB_GRPC_CLIENT_ORIGIN"
	BBGrpcClientIdentityEnvVarName = "BB_GRPC_CLIENT_IDENTITY"

	BuildBuddySecretEnvVarNamesForRedaction = "BUILDBUDDY_SECRET_ENV_VAR_NAMES"
)
