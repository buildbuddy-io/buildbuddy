package testauth

import (
	"context"
	"net/http"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/dgrijalva/jwt-go"
	"google.golang.org/grpc/metadata"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
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

const (
	testAuthenticationHeader = "test-auth-header"

	TestApiKeyHeader = "test-buildbuddy-api-key"
	jwtHeader        = "x-buildbuddy-jwt"
)

var (
	testApiKeyRegex = regexp.MustCompile(TestApiKeyHeader + "=([a-zA-Z0-9]+)")
	jwtTestKey      = []byte("testKey")
)

type TestUser struct {
	jwt.StandardClaims
	UserID                 string                   `json:"user_id"`
	GroupID                string                   `json:"group_id"`
	AllowedGroups          []string                 `json:"allowed_groups"`
	Capabilities           []akpb.ApiKey_Capability `json:"capabilities"`
	UseGroupOwnedExecutors bool                     `json:"use_group_owned_executors,omitempty"`
}

func (c *TestUser) GetUserID() string          { return c.UserID }
func (c *TestUser) GetGroupID() string         { return c.GroupID }
func (c *TestUser) GetAllowedGroups() []string { return c.AllowedGroups }
func (c *TestUser) IsAdmin() bool              { return false }
func (c *TestUser) HasCapability(cap akpb.ApiKey_Capability) bool {
	for _, cc := range c.Capabilities {
		if cap == cc {
			return true
		}
	}
	return false
}
func (c *TestUser) GetUseGroupOwnedExecutors() bool {
	return c.UseGroupOwnedExecutors
}

// TestUsers creates a map of test users from arguments of the form:
// user_id1, group_id1, user_id2, group_id2, ..., user_idN, group_idN
func TestUsers(vals ...string) map[string]interfaces.UserInfo {
	if len(vals)%2 != 0 {
		log.Errorf("You're calling TestUsers wrong!")
	}
	testUsers := make(map[string]interfaces.UserInfo, 0)
	var u *TestUser
	for i, val := range vals {
		if i%2 == 0 {
			u = &TestUser{UserID: val}
		} else {
			u.GroupID = val
			u.AllowedGroups = []string{val}
			u.Capabilities = capabilities.DefaultAuthenticatedUserCapabilities
			testUsers[u.UserID] = u
		}
	}
	return testUsers
}

type TestAuthenticator struct {
	testUsers map[string]interfaces.UserInfo
}

func NewTestAuthenticator(testUsers map[string]interfaces.UserInfo) *TestAuthenticator {
	return &TestAuthenticator{
		testUsers: testUsers,
	}
}

func (a *TestAuthenticator) AuthenticateHTTPRequest(w http.ResponseWriter, r *http.Request) context.Context {
	headerVal := r.Header.Get(TestApiKeyHeader)
	if user, ok := a.testUsers[headerVal]; ok {
		return context.WithValue(r.Context(), testAuthenticationHeader, user)
	}
	return r.Context()
}

func (a *TestAuthenticator) AuthenticateGRPCRequest(ctx context.Context) context.Context {
	if grpcMD, ok := metadata.FromIncomingContext(ctx); ok {
		for _, h := range []string{TestApiKeyHeader, jwtHeader} {
			headerVals := grpcMD[h]
			for _, headerVal := range headerVals {
				if user, ok := a.testUsers[headerVal]; ok {
					return context.WithValue(ctx, testAuthenticationHeader, user)
				}
			}
		}
	}
	return ctx
}

func (a *TestAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	uVal := ctx.Value(testAuthenticationHeader)
	u, ok := uVal.(interfaces.UserInfo)
	if ok {
		return u, nil
	}
	if jwt, ok := ctx.Value(jwtHeader).(string); ok {
		if u := a.testUsers[TestUserIDForJWT(jwt)]; u != nil {
			return u, nil
		}
	}
	return nil, status.PermissionDeniedError("User not found")
}

func (a *TestAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	return nil
}

func (a *TestAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", 301)
}

func (a *TestAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", 301)
}

func (a *TestAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/", 301)
}

func (a *TestAuthenticator) ParseAPIKeyFromString(input string) string {
	matches := testApiKeyRegex.FindStringSubmatch(input)
	if matches != nil && len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (a *TestAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, testAuthenticationHeader, a.testUsers[apiKey])
}

func RequestContext(userID string, groupID string) *ctxpb.RequestContext {
	return &ctxpb.RequestContext{
		UserId: &uidpb.UserId{
			Id: userID,
		},
		GroupId: groupID,
	}
}

// WithAuthenticatedUser sets the authenticated user to the given user.
func WithAuthenticatedUser(ctx context.Context, userInfo interfaces.UserInfo) context.Context {
	return context.WithValue(ctx, testAuthenticationHeader, userInfo)
}

func TestJWTForUserID(userID string) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, &TestUser{UserID: userID}).SignedString(jwtTestKey)
}

func TestUserIDForJWT(token string) string {
	claims := &TestUser{}
	jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return jwtTestKey, nil
	})
	return claims.UserID
}
