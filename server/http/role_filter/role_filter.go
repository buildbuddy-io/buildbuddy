package role_filter

import (
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"

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
		// able to create another group or request to join another group.
		"CreateGroup",
		"JoinGroup",
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
		"GetBazelConfig",
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

func AuthorizeSelectedGroupRole(env environment.Env, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rpcName := r.URL.Path
		if stringSliceContains(RoleIndependentRPCs, rpcName) {
			next.ServeHTTP(w, r)
			return
		}

		u, err := perms.AuthenticatedUser(ctx, env)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		if stringSliceContains(u.GetAllowedGroups(), globalAdminGroupID) {
			next.ServeHTTP(w, r)
			return
		}

		reqCtx := requestcontext.ProtoRequestContextFromContext(ctx)
		if reqCtx == nil || reqCtx.GetGroupId() == "" {
			http.Error(w, `Request is missing "request_context.group_id" field`, http.StatusBadRequest)
			return
		}

		// TODO(bduffs): Once we expose role info in UserInfo, get the user's actual
		// group role instead of assuming admin role
		if !stringSliceContains(u.GetAllowedGroups(), reqCtx.GetGroupId()) {
			http.Error(w, `User does not have access to the requested group.`, http.StatusForbidden)
			return
		}
		uRole := role.Admin

		if stringSliceContains(GroupAdminOnlyRPCs, rpcName) && (uRole&role.Admin != role.Admin) {
			http.Error(w, `RPC requires group admin role.`, http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func stringSliceContains(slice []string, val string) bool {
	for _, v := range slice {
		if val == v {
			return true
		}
	}
	return false
}
