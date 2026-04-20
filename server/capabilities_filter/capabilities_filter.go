package capabilities_filter

import (
	"context"
	"flag"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
)

var (
	buildBuddyServicePrefix = "/" + bbspb.BuildBuddyService_ServiceDesc.ServiceName + "/"
	apiServicePrefix        = "/" + apipb.ApiService_ServiceDesc.ServiceName + "/"
)

var (
	adminOnlyCreateGroup = flag.Bool("app.admin_only_create_group", false, "If true, only admins of an existing group can create a new groups.")

	// unfilteredRPCs are always allowed by the capabilities filter.
	// They may rely on other auth checks within the RPC implementation
	// if appropriate.
	unfilteredRPCs = []string{
		// RPCs that happen pre-login and don't require group membership.
		buildBuddyServicePrefix + "GetUser",
		buildBuddyServicePrefix + "CreateUser",
		buildBuddyServicePrefix + "GetGroup",

		// Invocations can be shared publicly, so authorization for these RPCs is
		// done purely using perms bits attached to each row.
		buildBuddyServicePrefix + "GetInvocation",
		buildBuddyServicePrefix + "GetEventLogChunk",
		buildBuddyServicePrefix + "GetEventLog",
		buildBuddyServicePrefix + "GetCacheScoreCard",
		buildBuddyServicePrefix + "GetCacheMetadata",
		buildBuddyServicePrefix + "GetTree",
		buildBuddyServicePrefix + "GetTreeDirectorySizes",
		buildBuddyServicePrefix + "GetTarget",
		buildBuddyServicePrefix + "GetTargetHistory",
		buildBuddyServicePrefix + "GetExecution",
		buildBuddyServicePrefix + "GetExecutionDownloads",
		buildBuddyServicePrefix + "WaitExecution",
		buildBuddyServicePrefix + "GetZipManifest",
		// Users do not need any particular role within their current group to be
		// able to create another group or request to join an existing group.
		buildBuddyServicePrefix + "JoinGroup",
		// Anonymous users can see the Bazel config required to use BuildBuddy, so
		// don't require a group role.
		buildBuddyServicePrefix + "GetBazelConfig",

		// API calls which have their own auth checks
		apiServicePrefix + "GetInvocation",
		apiServicePrefix + "GetAuditLog",
		apiServicePrefix + "GetLog",
		apiServicePrefix + "DeleteFile",
		apiServicePrefix + "GetTarget",
		apiServicePrefix + "GetAction",
		apiServicePrefix + "GetFile",
		apiServicePrefix + "GetFileRange",
		apiServicePrefix + "DeleteFile",

		// GitHub user-level token management does not require group membership.
		buildBuddyServicePrefix + "UnlinkUserGitHubAccount",

		// GitHub passthrough endpoints use User's linked GitHub account
		buildBuddyServicePrefix + "GetGithubUserInstallations",
		buildBuddyServicePrefix + "GetGithubUser",
		buildBuddyServicePrefix + "GetGithubRepo",
		buildBuddyServicePrefix + "GetGithubContent",
		buildBuddyServicePrefix + "GetGithubTree",
		buildBuddyServicePrefix + "CreateGithubTree",
		buildBuddyServicePrefix + "GetGithubBlob",
		buildBuddyServicePrefix + "CreateGithubBlob",
		buildBuddyServicePrefix + "CreateGithubPull",
		buildBuddyServicePrefix + "MergeGithubPull",
		buildBuddyServicePrefix + "GetGithubCompare",
		buildBuddyServicePrefix + "GetGithubForks",
		buildBuddyServicePrefix + "CreateGithubFork",
		buildBuddyServicePrefix + "GetGithubCommits",
		buildBuddyServicePrefix + "CreateGithubCommit",
		buildBuddyServicePrefix + "UpdateGithubRef",
		buildBuddyServicePrefix + "CreateGithubRef",
		buildBuddyServicePrefix + "GetGithubPullRequest",
		buildBuddyServicePrefix + "GetGithubPullRequestDetails",
		buildBuddyServicePrefix + "CreateGithubPullRequestComment",
		buildBuddyServicePrefix + "UpdateGithubPullRequestComment",
		buildBuddyServicePrefix + "DeleteGithubPullRequestComment",
		buildBuddyServicePrefix + "SendGithubPullRequestReview",

		// Audit logs.
		buildBuddyServicePrefix + "GetAuditLogs",
	}

	// groupMemberRPCs can only be called when logged in as a member of
	// the selected group, irrespective of capabilities.
	groupMemberRPCs = []string{
		// Invocation history and historical data for the org
		buildBuddyServicePrefix + "SearchInvocation",
		buildBuddyServicePrefix + "GetInvocationStat",
		buildBuddyServicePrefix + "GetTrend",
		buildBuddyServicePrefix + "GetStatHeatmap",
		buildBuddyServicePrefix + "GetStatDrilldown",
		buildBuddyServicePrefix + "GetTargetTrends",
		buildBuddyServicePrefix + "GetSuggestion",
		buildBuddyServicePrefix + "SearchExecution",
		buildBuddyServicePrefix + "GetTargetStats",
		buildBuddyServicePrefix + "GetDailyTargetStats",
		buildBuddyServicePrefix + "GetTargetFlakeSamples",
		buildBuddyServicePrefix + "GetInvocationFilterSuggestions",
		// Workflow configuration and history (read-only).
		buildBuddyServicePrefix + "GetWorkflows",
		buildBuddyServicePrefix + "GetRepos",
		buildBuddyServicePrefix + "GetWorkflowHistory",
		// Github configuration (read-only).
		buildBuddyServicePrefix + "GetLinkedGitHubRepos",
		// Per-invocation actions
		buildBuddyServicePrefix + "UpdateInvocation",
		buildBuddyServicePrefix + "DeleteInvocation",
		buildBuddyServicePrefix + "CancelExecutions",
		buildBuddyServicePrefix + "ExecuteWorkflow",
		apiServicePrefix + "ExecuteWorkflow",
		buildBuddyServicePrefix + "InvalidateSnapshot",
		buildBuddyServicePrefix + "WriteEventLog",
		buildBuddyServicePrefix + "UpdateRunStatus",
		// Org API keys (implementation only returns developer-visible keys
		// for developers; admins can see all keys).
		buildBuddyServicePrefix + "GetApiKeys",
		buildBuddyServicePrefix + "GetApiKey",
		// User-level API keys
		buildBuddyServicePrefix + "GetUserApiKeys",
		buildBuddyServicePrefix + "GetUserApiKey",
		buildBuddyServicePrefix + "CreateUserApiKey",
		apiServicePrefix + "CreateUserApiKey",
		buildBuddyServicePrefix + "UpdateUserApiKey",
		buildBuddyServicePrefix + "DeleteUserApiKey",
		// Remote Bazel
		buildBuddyServicePrefix + "Run",
		apiServicePrefix + "Run",
		// Codesearch and Kythe
		buildBuddyServicePrefix + "Search",
		buildBuddyServicePrefix + "KytheProxy",
		buildBuddyServicePrefix + "Index",
		buildBuddyServicePrefix + "RepoStatus",
		// Workspace management
		buildBuddyServicePrefix + "GetWorkspace",
		buildBuddyServicePrefix + "SaveWorkspace",
		buildBuddyServicePrefix + "GetWorkspaceDirectory",
		buildBuddyServicePrefix + "GetWorkspaceFile",
	}

	// AdminOnlyRPCs can only be called by admins of the selected group.
	groupAdminOnlyRPCs = []string{
		// Org details management
		buildBuddyServicePrefix + "UpdateGroup",
		// Org members management
		buildBuddyServicePrefix + "GetGroupUsers",
		buildBuddyServicePrefix + "UpdateGroupUsers",
		// Org user list management
		buildBuddyServicePrefix + "GetUserLists",
		buildBuddyServicePrefix + "GetUserList",
		buildBuddyServicePrefix + "CreateUserList",
		buildBuddyServicePrefix + "DeleteUserList",
		buildBuddyServicePrefix + "UpdateUserList",
		buildBuddyServicePrefix + "UpdateUserListMembership",
		// Org GitHub account link management
		buildBuddyServicePrefix + "UnlinkGitHubAccount",
		// Org GitHub app link management
		buildBuddyServicePrefix + "LinkGitHubAppInstallation",
		buildBuddyServicePrefix + "GetGitHubAppInstallations",
		buildBuddyServicePrefix + "UnlinkGitHubAppInstallation",
		buildBuddyServicePrefix + "UpdateGitHubAppInstallation",
		// Org GitHub repo management
		buildBuddyServicePrefix + "GetAccessibleGitHubRepos",
		buildBuddyServicePrefix + "GetGitHubAppInstallPath",
		buildBuddyServicePrefix + "LinkGitHubRepo",
		buildBuddyServicePrefix + "UnlinkGitHubRepo",
		buildBuddyServicePrefix + "UpdateGitHubRepoSettings",
		// Org API key management
		buildBuddyServicePrefix + "CreateApiKey",
		buildBuddyServicePrefix + "UpdateApiKey",
		buildBuddyServicePrefix + "DeleteApiKey",
		// Secret management
		buildBuddyServicePrefix + "GetPublicKey",
		buildBuddyServicePrefix + "ListSecrets",
		buildBuddyServicePrefix + "UpdateSecret",
		buildBuddyServicePrefix + "DeleteSecret",
		// Workflow management
		buildBuddyServicePrefix + "DeleteWorkflow",
		buildBuddyServicePrefix + "InvalidateAllSnapshotsForRepo",
		// RBE deployment view
		buildBuddyServicePrefix + "GetExecutionNodes",
		// BuildBuddy usage data
		buildBuddyServicePrefix + "GetUsage",
		// Encryption.
		buildBuddyServicePrefix + "GetEncryptionConfig",
		buildBuddyServicePrefix + "SetEncryptionConfig",
		// Repo management
		buildBuddyServicePrefix + "CreateRepo",
		// IP Rules.
		buildBuddyServicePrefix + "GetIPRules",
		buildBuddyServicePrefix + "AddIPRule",
		buildBuddyServicePrefix + "UpdateIPRule",
		buildBuddyServicePrefix + "DeleteIPRule",
		buildBuddyServicePrefix + "GetIPRulesConfig",
		buildBuddyServicePrefix + "SetIPRulesConfig",
		// GCP
		buildBuddyServicePrefix + "GetGCPProject",
	}

	// ServerAdminOnlyRPCs can only be called by server admins. It is different
	// from AdminOnlyRPCs in that it requires the authenticated user to be an
	// admin of the configured server-admin group, and not just an admin of
	// their authenticated group.
	serverAdminOnlyRPCs = []string{
		buildBuddyServicePrefix + "GetInvocationOwner",

		// Quota APIs
		buildBuddyServicePrefix + "GetNamespace",
		buildBuddyServicePrefix + "RemoveNamespace",
		buildBuddyServicePrefix + "ModifyNamespace",
		buildBuddyServicePrefix + "ApplyBucket",

		// Impersonation
		buildBuddyServicePrefix + "CreateImpersonationApiKey",

		// Org management
		buildBuddyServicePrefix + "SetGroupStatus",
		buildBuddyServicePrefix + "GetSSOConfig",
		buildBuddyServicePrefix + "SetSSOConfig",
	}
)

// AllowedRPCs returns the complete list of RPCs that are allowed for the given
// request context and selected group ID.
func AllowedRPCs(ctx context.Context, env environment.Env, groupID string) []string {
	var out []string
	out = append(out, getUnfilteredRPCs()...)

	if err := claims.AuthorizeServerAdmin(ctx); err == nil {
		out = append(out, serverAdminOnlyRPCs...)
	}

	if groupID != "" {
		userGroupCapabilities, err := capabilities.ForAuthenticatedUserGroup(ctx, env.GetAuthenticator(), groupID)
		if err == nil {
			if slices.Contains(userGroupCapabilities, cappb.Capability_ORG_ADMIN) {
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
		r = append(r, buildBuddyServicePrefix+"CreateGroup")
	}
	return r
}

func getGroupAdminOnlyRPCs() []string {
	r := groupAdminOnlyRPCs
	if *adminOnlyCreateGroup {
		r = append(r, buildBuddyServicePrefix+"CreateGroup")
	}
	return r
}

// AuthorizeRPC applies a coarse-grained authorization check on an RPC to ensure
// that the user has the appropriate role within their org to call the RPC.
//
// If the RPC accesses any specific resources within the org, further
// authorization checks may be needed beyond this coarse-grained filter.
func AuthorizeRPC(ctx context.Context, env environment.Env, rpcName string) error {
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
