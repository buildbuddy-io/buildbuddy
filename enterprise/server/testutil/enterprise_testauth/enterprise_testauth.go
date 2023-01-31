package enterprise_testauth

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/stretchr/testify/require"
)

// Configure sets up an Authenticator in the env that authenticates similarly to
// the real enterprise app, performing queries against UserDB and AuthDB, rather
// than using a static user mapping.
func Configure(t *testing.T, env environment.Env) *testauth.TestAuthenticator {
	a := testauth.NewTestAuthenticator(nil /*=testUsers*/)

	a.UserProvider = func(userID string) interfaces.UserInfo {
		// Fake the minimal auth context needed to look up the real user and
		// group memberships.
		ctx := testauth.WithAuthenticatedUserInfo(
			context.Background(),
			&testauth.TestUser{UserID: userID},
		)
		u, err := env.GetUserDB().GetUser(ctx)
		require.NoErrorf(t, err, "failed to lookup user %q", userID)
		// Now return the claims for the real user.
		if len(u.Groups) > 0 {
			// For now, use the first group as the "effective" group for UI
			// endpoints which use the group_id from request context.
			reqCtx := testauth.RequestContext(u.UserID, u.Groups[0].Group.GroupID)
			ctx = requestcontext.ContextWithProtoRequestContext(ctx, reqCtx)
		}

		tu, err := auth.ClaimsFromSubID(ctx, env, u.SubID)
		require.NoError(t, err, "failed to get claims from subid %q", u.SubID)
		return tu
	}

	a.APIKeyProvider = func(apiKey string) interfaces.UserInfo {
		akg, err := env.GetAuthDB().GetAPIKeyGroupFromAPIKey(context.Background(), apiKey)
		require.NoErrorf(t, err, "failed to look up APIKeyGroup from test API key %q", apiKey)
		return auth.APIKeyGroupClaims(akg)
	}

	env.SetAuthenticator(a)
	return a
}
