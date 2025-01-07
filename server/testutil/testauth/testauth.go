package testauth

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt"
	"google.golang.org/grpc/metadata"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

// Do not use this authenticator in production. It is useful
// for *testing* flows that require authentication. In your test,
// create a new TestAuthenticator with some known set of users
// and then test that they are used.
//
// Example:
//   authenticator := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
//   testEnv.SetAuthenticator(authenticator)
//   ... test code that uses auth ...
//

var (
	jwtTestKey = []byte("testKey")
)

type TestUser = claims.Claims

func User(userID, groupID string) *TestUser {
	return &TestUser{
		UserID:        userID,
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
		GroupMemberships: []*interfaces.GroupMembership{
			{
				GroupID:      groupID,
				Capabilities: capabilities.DefaultAuthenticatedUserCapabilities,
				Role:         role.Admin,
			},
		},
		Capabilities: capabilities.DefaultAuthenticatedUserCapabilities,
	}
}

// TestUsers creates a map of test users from arguments of the form:
// user_id1, group_id1, user_id2, group_id2, ..., user_idN, group_idN
//
// All users are created with Admin role within each group, to ease testing of admin-only APIs.
func TestUsers(vals ...string) map[string]interfaces.UserInfo {
	if len(vals)%2 != 0 {
		log.Errorf("You're calling TestUsers wrong!")
	}
	testUsers := make(map[string]interfaces.UserInfo, 0)
	var uid string
	for i, val := range vals {
		if i%2 == 0 {
			uid = val
		} else {
			gid := val
			testUsers[uid] = User(uid, gid)
		}
	}
	return testUsers
}

type userProvider func(userID string) interfaces.UserInfo
type apiKeyUserProvider func(apiKey string) interfaces.UserInfo

type TestAuthenticator struct {
	*nullauth.NullAuthenticator
	UserProvider       userProvider
	APIKeyProvider     apiKeyUserProvider
	ServerAdminGroupID string
}

func NewTestAuthenticator(testUsers map[string]interfaces.UserInfo) *TestAuthenticator {
	return &TestAuthenticator{
		NullAuthenticator: &nullauth.NullAuthenticator{},
		UserProvider:      func(userID string) interfaces.UserInfo { return testUsers[userID] },
		APIKeyProvider:    func(apiKey string) interfaces.UserInfo { return testUsers[apiKey] },
	}
}

func (a *TestAuthenticator) AdminGroupID() string {
	return a.ServerAdminGroupID
}

func (a *TestAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	apiKey := r.Header.Get(authutil.APIKeyHeader)
	if apiKey == "" {
		return r.Context()
	}
	return a.AuthContextFromAPIKey(r.Context(), apiKey)
}

func (a *TestAuthenticator) AuthenticatedGRPCContext(ctx context.Context) context.Context {
	grpcMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	for _, apiKey := range grpcMD[authutil.APIKeyHeader] {
		if apiKey == "" {
			return ctx
		}
		jwt, err := a.TestJWTForAPIKey(apiKey)
		if err != nil {
			log.Errorf("Failed to mint JWT from API key: %s", err)
			continue
		}
		return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	}
	for _, jwt := range grpcMD[authutil.ContextTokenStringKey] {
		_, err := a.authenticateJWT(jwt)
		if err != nil {
			log.Errorf("Failed to authenticate incoming JWT: %s", err)
			continue
		}
		return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
	}
	return ctx
}

func (a *TestAuthenticator) authenticateJWT(token string) (interfaces.UserInfo, error) {
	if token == "" {
		return nil, status.PermissionDeniedError("JWT is empty")
	}
	claims := &TestUser{}
	_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return jwtTestKey, nil
	})
	if err != nil {
		return nil, status.PermissionDeniedErrorf("failed to authenticate JWT: %s", err)
	}
	return claims, nil
}

func (a *TestAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	if grpcMD, ok := metadata.FromIncomingContext(ctx); ok {
		for _, apiKey := range grpcMD[authutil.APIKeyHeader] {
			if apiKey == "" {
				continue
			}
			u := a.APIKeyProvider(apiKey)
			if u == nil {
				return nil, status.PermissionDeniedErrorf("invalid API key: %q", apiKey)
			}
			return u, nil
		}
		for _, jwt := range grpcMD[authutil.ContextTokenStringKey] {
			return a.authenticateJWT(jwt)
		}
	}
	return nil, authutil.AnonymousUserError("gRPC request is missing credentials.")
}

func (a *TestAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	if err, ok := authutil.AuthErrorFromContext(ctx); ok {
		return nil, err
	}
	if jwt, ok := ctx.Value(authutil.ContextTokenStringKey).(string); ok {
		return a.authenticateJWT(jwt)
	}
	return nil, authutil.AnonymousUserError("User not found")
}

func (a *TestAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	return nil
}

func (a *TestAuthenticator) Login(w http.ResponseWriter, r *http.Request) error {
	return status.UnimplementedError("Auth not implemented")
}

func (a *TestAuthenticator) Auth(w http.ResponseWriter, r *http.Request) error {
	return status.UnimplementedError("Auth not implemented")
}

func (a *TestAuthenticator) Logout(w http.ResponseWriter, r *http.Request) error {
	return status.UnimplementedError("Auth not implemented")
}

func (a *TestAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	jwt, err := a.TestJWTForAPIKey(apiKey)
	if err != nil {
		log.Errorf("Failed to mint JWT for API key: %s", err)
		return ctx
	}
	ctx = context.WithValue(ctx, authutil.APIKeyHeader, apiKey)
	return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
}

func (a *TestAuthenticator) TrustedJWTFromAuthContext(ctx context.Context) string {
	jwt, ok := ctx.Value(authutil.ContextTokenStringKey).(string)
	if !ok {
		return ""
	}
	return jwt
}

func (a *TestAuthenticator) AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context {
	return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
}

func (a *TestAuthenticator) WithAuthenticatedUser(ctx context.Context, userID string) (context.Context, error) {
	userInfo := a.UserProvider(userID)
	if userInfo == nil {
		return nil, status.FailedPreconditionErrorf("User %q unknown to test authenticator.", userID)
	}
	return WithAuthenticatedUserInfo(ctx, userInfo), nil
}

func RequestContext(userID string, groupID string) *ctxpb.RequestContext {
	return &ctxpb.RequestContext{
		UserId: &uidpb.UserId{
			Id: userID,
		},
		GroupId: groupID,
	}
}

// WithAuthenticatedUserInfo sets the authenticated user to the given user.
func WithAuthenticatedUserInfo(ctx context.Context, userInfo interfaces.UserInfo) context.Context {
	jwt, err := jwt.NewWithClaims(jwt.SigningMethodHS256, userInfo).SignedString(jwtTestKey)
	if err != nil {
		log.Errorf("Failed to mint JWT from UserInfo: %s", err)
		return ctx
	}
	return context.WithValue(ctx, authutil.ContextTokenStringKey, jwt)
}

func (a *TestAuthenticator) TestJWTForUserID(userID string) (string, error) {
	u := a.UserProvider(userID)
	if u == nil {
		return "", status.PermissionDeniedErrorf("user %s is unknown to TestAuthenticator", userID)
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, u).SignedString(jwtTestKey)
}

func (a *TestAuthenticator) TestJWTForAPIKey(apiKey string) (string, error) {
	if apiKey == "" {
		return "", status.InvalidArgumentError("API key is empty")
	}
	u := a.APIKeyProvider(apiKey)
	if u == nil {
		return "", status.PermissionDeniedErrorf("API key %s is unknown to TestAuthenticator", apiKey)
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, u).SignedString(jwtTestKey)
}
