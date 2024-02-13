package capabilities_filter

import (
	"context"
	"flag"
	"path"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

var (
	adminOnlyCreateGroup = flag.Bool("app.admin_only_create_group", false, "If true, only admins of an existing group can create a new groups.")

	// unfilteredRPCs are always allowed by the capabilities filter.
	// They may rely on other auth checks within the RPC implementation
	// if appropriate.
	unfilteredRPCs = []string{
		// RPCs that happen pre-login and don't require group membership.
		"GetUser",
		"CreateUser",
		"GetGroup",

		// Invocations can be shared publicly, so authorization for these RPCs is
		// done purely using perms bits attached to each row.
		"GetInvocation",
		"GetEventLogChunk",
		"GetCacheScoreCard",
		"GetCacheMetadata",
		"GetTreeDirectorySizes",
		"GetTarget",
		"GetTargetHistory",
		"GetExecution",
		"GetZipManifest",
		// Users do not need any particular role within their current group to be
		// able to create another group or request to join an existing group.
		"JoinGroup",
		// Anonymous users can see the Bazel config required to use BuildBuddy, so
		// don't require a group role.
		"GetBazelConfig",
		// API calls are role independent
		// TODO(bduffany): prefix all of these with the service name,
		// since API methods and BuildBuddyService methods may be the same.
		"GetInvocation",
		"GetLog",
		"DeleteFile",
		"GetTarget",
		"GetAction",
		"GetFile",
		"DeleteFile",
		// GitHub passthrough endpoints use User's linked GitHub account
		"GetGithubUserInstallations",
		"GetGithubUser",
		"GetGithubRepo",
		"GetGithubContent",
		"GetGithubTree",
		"CreateGithubTree",
		"GetGithubBlob",
		"CreateGithubBlob",
		"CreateGithubPull",
		"MergeGithubPull",
		"GetGithubCompare",
		"GetGithubForks",
		"CreateGithubFork",
		"GetGithubCommits",
		"CreateGithubCommit",
		"UpdateGithubRef",
		"CreateGithubRef",
		"GetGithubPullRequest",
		"GetGithubPullRequestDetails",
	}

	// groupMemberRPCs can only be called when logged in as a member of
	// the selected group, irrespective of capabilities.
	groupMemberRPCs = []string{
		// Invocation history and historical data for the org
		"SearchInvocation",
		"GetInvocationStat",
		"GetTrend",
		"GetStatHeatmap",
		"GetStatDrilldown",
		"GetSuggestion",
		"SearchExecution",
		// Workflow configuration and history (read-only).
		"GetWorkflows",
		"GetRepos",
		"GetWorkflowHistory",
		// Github configuration (read-only).
		"GetLinkedGitHubRepos",
		// Per-invocation actions
		"UpdateInvocation",
		"DeleteInvocation",
		"CancelExecutions",
		"ExecuteWorkflow",
		// Org API keys (implementation only returns developer-visible keys
		// for developers; admins can see all keys).
		"GetApiKeys",
		"GetApiKey",
		// User-level API keys
		"GetUserApiKeys",
		"GetUserApiKey",
		"CreateUserApiKey",
		"UpdateUserApiKey",
		"DeleteUserApiKey",
		// Remote Bazel
		"Run",
		// Codesearch
		"Search",
	}

	// AdminOnlyRPCs can only be called by admins of the selected group.
	groupAdminOnlyRPCs = []string{
		// Org details management
		"UpdateGroup",
		// Org members management
		"GetGroupUsers",
		"UpdateGroupUsers",
		// Org GitHub account link management
		"UnlinkGitHubAccount",
		// Org GitHub app link management
		"LinkGitHubAppInstallation",
		"GetGitHubAppInstallations",
		"UnlinkGitHubAppInstallation",
		// Org GitHub repo management
		"GetAccessibleGitHubRepos",
		"LinkGitHubRepo",
		"UnlinkGitHubRepo",
		// Org API key management
		"CreateApiKey",
		"UpdateApiKey",
		"DeleteApiKey",
		// Secret management
		"GetPublicKey",
		"ListSecrets",
		"UpdateSecret",
		"DeleteSecret",
		// Workflow management
		"CreateWorkflow",
		"DeleteWorkflow",
		// RBE deployment view
		"GetExecutionNodes",
		// BuildBuddy usage data
		"GetUsage",
		// Encryption.
		"GetEncryptionConfig",
		"SetEncryptionConfig",
		// Audit logs.
		"GetAuditLogs",
		// Repo management
		"CreateRepo",
		// IP Rules.
		"GetIPRules",
		"AddIPRule",
		"UpdateIPRule",
		"DeleteIPRule",
		"GetIPRulesConfig",
		"SetIPRulesConfig",
		// GCP
		"GetGCPProject",
	}

	// ServerAdminOnlyRPCs can only be called by server admins. It is different
	// from AdminOnlyRPCs in that it requires the authenticated user to be an
	// admin of the configured server-admin group, and not just an admin of
	// their authenticated group.
	serverAdminOnlyRPCs = []string{
		"GetInvocationOwner",

		// Quota APIs
		"GetNamespace",
		"RemoveNamespace",
		"ModifyNamespace",
		"ApplyBucket",

		// Impersonation
		"CreateImpersonationApiKey",
	}
)

// AllowedRPCs returns the complete list of RPCs that are allowed for the given
// request context and selected group ID.
func AllowedRPCs(ctx context.Context, env environment.Env, groupID string) []string {
	var out []string
	out = append(out, getUnfilteredRPCs()...)

	if err := authorizeServerAdmin(ctx, env); err == nil {
		out = append(out, serverAdminOnlyRPCs...)
	}

	if groupID != "" {
		userGroupCapabilities, err := capabilities.ForAuthenticatedUserGroup(ctx, env, groupID)
		if err == nil {
			if slices.Contains(userGroupCapabilities, akpb.ApiKey_ORG_ADMIN_CAPABILITY) {
				out = append(out, getGroupAdminOnlyRPCs()...)
			}
			out = append(out, groupMemberRPCs...)
		}
	}

	return out
}

// AllRPCsForTestOnly returns all RPC names known to the capabilities filter.
// For testing only.
func AllRPCsForTestOnly() []string {
	var out []string
	out = append(out, getUnfilteredRPCs()...)
	out = append(out, groupMemberRPCs...)
	out = append(out, getGroupAdminOnlyRPCs()...)
	out = append(out, serverAdminOnlyRPCs...)
	return out
}

func getUnfilteredRPCs() []string {
	r := unfilteredRPCs
	if !*adminOnlyCreateGroup {
		r = append(r, "CreateGroup")
	}
	return r
}

func getGroupAdminOnlyRPCs() []string {
	r := groupAdminOnlyRPCs
	if *adminOnlyCreateGroup {
		r = append(r, "CreateGroup")
	}
	return r
}

// AuthorizeRPC applies a coarse-grained authorization check on an RPC to ensure
// that the user has the appropriate role within their org to call the RPC.
//
// If the RPC accesses any specific resources within the org, further
// authorization checks may be needed beyond this coarse-grained filter.
func AuthorizeRPC(ctx context.Context, env environment.Env, rpcName string) error {
	// Strip off the service name since it varies depending on whether it was
	// accessed via protolet or gRPC.
	rpcName = path.Base(rpcName)

	var groupID string
	u, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err == nil {
		groupID = u.GetGroupID()
	}

	if !slices.Contains(AllowedRPCs(ctx, env, groupID), rpcName) {
		return status.PermissionDeniedError("permission denied")
	}

	return nil
}

func authorizeServerAdmin(ctx context.Context, env environment.Env) error {
	u, err := env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}

	// If impersonation is in effect, it implies the user is an admin.
	// Can't check group membership because impersonation modifies
	// group information.
	if u.IsImpersonating() {
		return nil
	}

	serverAdminGID := env.GetAuthenticator().AdminGroupID()
	if serverAdminGID == "" {
		return status.PermissionDeniedError("permission denied")
	}
	for _, m := range u.GetGroupMemberships() {
		if m.GroupID == serverAdminGID && (capabilities.ToInt(m.Capabilities)&int32(akpb.ApiKey_ORG_ADMIN_CAPABILITY) != 0) {
			return nil
		}
	}
	return status.PermissionDeniedError("Permission denied.")
}
