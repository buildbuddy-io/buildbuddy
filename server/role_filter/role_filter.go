package role_filter

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

const (
	globalAdminGroupID = "admin"
)

var (
	// RoleIndependentRPCs do not require a particular group role for auth. They
	// may rely on other forms of authorization if appropriate.
	RoleIndependentRPCs = []string{
		// RPCs that happen pre-login and don't require group membership.
		"GetUser",
		"CreateUser",
		"GetGroup",
		// Invocations can be shared publicly, so authorization for these RPCs is
		// done purely using perms bits attached to each row.
		"GetInvocation",
		"GetEventLogChunk",
		"GetTarget",
		"GetExecution",
		// Users do not need any particular role within their current group to be
		// able to create another group or request to join an existing group.
		"CreateGroup",
		"JoinGroup",
		// Anonymous users can see the Bazel config required to use BuildBuddy, so
		// don't require a group role.
		"GetBazelConfig",
	}

	// DeveloperRPCs can be called only by developers or admins of the selected
	// group.
	GroupDeveloperRPCs = []string{
		// Invocation history and historical data for the org
		"SearchInvocation",
		"GetInvocationStat",
		"GetTrend",
		// Per-invocation actions
		"UpdateInvocation",
		"DeleteInvocation",
		"ExecuteWorkflow",
		// Setup
		"GetApiKeys",
		// Remote Bazel
		"Run",
	}

	// AdminOnlyRPCs can only be called by admins of the selected group.
	GroupAdminOnlyRPCs = []string{
		// Org details management
		"UpdateGroup",
		// Org members management
		"GetGroupUsers",
		"UpdateGroupUsers",
		// API key management
		"CreateApiKey",
		"UpdateApiKey",
		"DeleteApiKey",
		// Workflow management
		"CreateWorkflow",
		"DeleteWorkflow",
		"GetWorkflows",
		"GetRepos",
		// RBE deployment view
		"GetExecutionNodes",
		// BuildBuddy usage data
		"GetUsage",
	}
)

// GroupSelector returns the user's preferred group ID.
type GroupSelector func(ctx context.Context, u interfaces.UserInfo) (string, error)

var (
	// RPCGroupSelector returns the preferred group ID for gRPC requests.
	GRPCGroupSelector GroupSelector = groupIDFromUserInfo

	// HTTPGroupSelector returns the preferred group ID for HTTP-based (protolet)
	// gRPC requests.
	HTTPGroupSelector GroupSelector = groupIDFromRequestContext
)

// AuthorizeBuildBuddyServiceRPC applies a coarse-grained authorization check on
// a BuildBuddyService RPC to ensure that the user has the appropriate role
// within their org to call the RPC.
//
// If the RPC accesses any specific resources within the org, further
// authorization checks may be needed beyond this coarse-grained filter.
func AuthorizeBuildBuddyServiceRPC(ctx context.Context, env environment.Env, rpcName string, groupSelector GroupSelector) error {
	if stringSliceContains(RoleIndependentRPCs, rpcName) {
		return nil
	}

	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		return err
	}

	if stringSliceContains(u.GetAllowedGroups(), globalAdminGroupID) {
		return nil
	}

	groupID, err := groupSelector(ctx, u)
	if err != nil {
		return err
	}

	allowedRoles := role.Admin | role.Developer
	if stringSliceContains(GroupAdminOnlyRPCs, rpcName) {
		allowedRoles = role.Admin
	}

	return authutil.AuthorizeGroupRole(u, groupID, allowedRoles)
}

func stringSliceContains(slice []string, val string) bool {
	for _, v := range slice {
		if val == v {
			return true
		}
	}
	return false
}

func groupIDFromUserInfo(ctx context.Context, u interfaces.UserInfo) (string, error) {
	if u.GetGroupID() == "" {
		alert.UnexpectedEvent(
			"authenticated_user_missing_group_id",
			"Could not determine group ID for authenticated user; user_id=%q",
			u.GetUserID())
		return "", status.InternalError("Preferred group ID could not be determined from the request.")
	}
	return u.GetGroupID(), nil
}

func groupIDFromRequestContext(ctx context.Context, u interfaces.UserInfo) (string, error) {
	reqCtx := requestcontext.ProtoRequestContextFromContext(ctx)
	if reqCtx == nil || reqCtx.GetGroupId() == "" {
		return "", status.InvalidArgumentError(`Request is missing "request_context.group_id" field`)
	}
	return reqCtx.GetGroupId(), nil
}
