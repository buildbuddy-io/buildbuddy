package enterprise_testauth

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

// Configure sets up an Authenticator in the env that authenticates similarly to
// the real enterprise app, performing queries against UserDB and AuthDB, rather
// than using a static user mapping.
func Configure(t *testing.T, env *real_environment.RealEnv) *testauth.TestAuthenticator {
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

		tu, err := claims.ClaimsFromSubID(ctx, env, u.SubID)
		require.NoError(t, err, "failed to get claims from subid %q", u.SubID)
		return tu
	}

	a.APIKeyProvider = func(apiKey string) interfaces.UserInfo {
		akg, err := env.GetAuthDB().GetAPIKeyGroupFromAPIKey(context.Background(), apiKey)
		require.NoErrorf(t, err, "failed to look up APIKeyGroup from test API key %q", apiKey)
		return claims.APIKeyGroupClaims(akg)
	}

	env.SetAuthenticator(a)
	return a
}

// CreateRandomGroups creates several randomly generated orgs with several
// randomly generated users under each.
func CreateRandomGroups(t *testing.T, env environment.Env) []*tables.User {
	ctx := context.Background()
	udb := env.GetUserDB()
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	var uids []string

	for g := 0; g < 12; g++ {
		// Create an admin user with a self-owned group.
		domain := fmt.Sprintf("rand-%d-%d.io", g, rand.Int63n(1e12))
		admin := CreateRandomUser(t, env, domain)
		uids = append(uids, admin.UserID)
		adminCtx, err := auth.WithAuthenticatedUser(ctx, admin.UserID)
		require.NoError(t, err)
		u, err := udb.GetUser(adminCtx)
		require.NoError(t, err)
		require.Len(t, u.Groups, 1)
		gid := u.Groups[0].Group.GroupID

		// Take ownership of the domain so that users are auto-added to it.
		slug := fmt.Sprintf("slug-%d-%d", g, rand.Int63n(1e12))
		_, err = udb.InsertOrUpdateGroup(adminCtx, &tables.Group{
			GroupID:       gid,
			URLIdentifier: &slug,
			OwnedDomain:   domain,
		})
		require.NoError(t, err)

		// Create a random number of users.
		nDevs := int(rand.Float64() * 8)
		for u := 0; u < nDevs; u++ {
			dev := CreateRandomUser(t, env, domain)
			uids = append(uids, dev.UserID)

			// Sanity check that the dev is only a member of the admin's
			// group ID.
			authCtx, err := auth.WithAuthenticatedUser(ctx, dev.UserID)
			require.NoError(t, err)
			dev, err = udb.GetUser(authCtx)
			require.NoError(t, err)
			require.Len(t, dev.Groups, 1)
			require.Equal(t, gid, dev.Groups[0].Group.GroupID)
		}
	}

	users := make([]*tables.User, 0, len(uids))
	for _, uid := range uids {
		authCtx, err := auth.WithAuthenticatedUser(ctx, uid)
		require.NoError(t, err)
		tu, err := udb.GetUser(authCtx)
		require.NoError(t, err)
		users = append(users, tu)
	}

	// Return the users to the caller in a random order, to avoid any dependence
	// on DB insertion order.
	rand.Shuffle(len(users), func(i, j int) {
		users[i], users[j] = users[j], users[i]
	})

	return users
}

// CreateRandomUser creates a random user with the given email domain.
func CreateRandomUser(t *testing.T, env environment.Env, domain string) *tables.User {
	udb := env.GetUserDB()
	tu := randomUser(t, domain)
	ctx := context.Background()
	err := udb.InsertUser(ctx, tu)
	require.NoError(t, err)
	// Refresh user to pick up default group.
	tu, err = udb.GetUserByIDWithoutAuthCheck(ctx, tu.UserID)
	require.NoError(t, err)
	return tu
}

func randomUser(t *testing.T, domain string) *tables.User {
	uid, err := tables.PrimaryKeyForTable((&tables.User{}).TableName())
	require.NoError(t, err)
	return &tables.User{
		UserID:    uid,
		SubID:     "SubID-" + uid,
		Email:     uid + "@" + domain,
		FirstName: "FirstName-" + uid,
		LastName:  "LastName-" + uid,
	}
}
