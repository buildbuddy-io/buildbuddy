package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

const (
	testIssuer        = "testIssuer"
	validJWT          = "validJWT"
	expiredJWT        = "expiredJWT"
	refreshedJWT      = "refreshedJWT"
	userID            = "1234"
	userName          = "Fluffy"
	userEmail         = "fluffy@cuteanimals.test"
	subID             = testIssuer + "/" + userID
	validRefreshToken = "abc123"
)

var (
	validUserToken = &userToken{
		issuer: testIssuer,
		Sub:    userID,
		Name:   userName,
	}
)

type fakeOidcAuthenticator struct {
}

func (f fakeOidcAuthenticator) getIssuer() string {
	return testIssuer
}

func (f fakeOidcAuthenticator) authCodeURL(state string, opts ...oauth2.AuthCodeOption) string {
	return "https://auth.test"
}

func (f fakeOidcAuthenticator) exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (f fakeOidcAuthenticator) verifyTokenAndExtractUser(ctx context.Context, jwt string, checkExpiry bool) (*userToken, error) {
	if jwt == validJWT || jwt == expiredJWT && !checkExpiry {
		return validUserToken, nil
	} else if jwt == expiredJWT {
		return nil, status.PermissionDeniedErrorf("expired JWT")
	}
	return nil, status.PermissionDeniedError("invalid JWT")
}

func (f fakeOidcAuthenticator) renewToken(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	if refreshToken == validRefreshToken {
		t := (&oauth2.Token{}).WithExtra(map[string]interface{}{"id_token": refreshedJWT})
		return t, nil
	}
	return nil, status.PermissionDeniedError("invalid refresh token")
}

func TestAuthenticateHTTPRequest(t *testing.T) {
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{})

	auth, err := newForTesting(context.Background(), env, &fakeOidcAuthenticator{})
	require.NoErrorf(t, err, "could not create authenticator")

	// JWT cookie not present, auth should fail.
	request, err := http.NewRequest(http.MethodGet, "/", strings.NewReader(""))
	require.NoErrorf(t, err, "could not create HTTP request")
	response := httptest.NewRecorder()
	authCtx := auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticationError(t, authCtx)

	// Valid JWT cookie, but user does not exist.
	request.AddCookie(&http.Cookie{Name: jwtCookie, Value: validJWT})
	request.AddCookie(&http.Cookie{Name: authIssuerCookie, Value: testIssuer})
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticationError(t, authCtx)
	// User information should still be populated (it's needed for user creation).
	require.Equal(t, validUserToken, authCtx.Value(contextUserKey), "context user details should match details returned by provider")

	// Create matching user, authentication should now succeed.
	user := &tables.User{
		UserID: userID,
		SubID:  subID,
		Email:  userEmail,
	}
	err = env.GetUserDB().InsertUser(context.Background(), user)
	require.NoError(t, err, "could not insert user")
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticated(t, authCtx)
	require.Equal(t, validUserToken, authCtx.Value(contextUserKey), "context user details should match details returned by provider")

	// Send request with an expired JWT cookie.
	// DB doesn't have a refresh token so authentication should fail.
	request, err = http.NewRequest(http.MethodGet, "/", strings.NewReader(""))
	require.NoErrorf(t, err, "could not create HTTP request")
	request.AddCookie(&http.Cookie{Name: jwtCookie, Value: expiredJWT})
	request.AddCookie(&http.Cookie{Name: authIssuerCookie, Value: testIssuer})
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticationError(t, authCtx)

	// Insert a refresh token into the DB & auth should succeed.
	token := &tables.Token{RefreshToken: validRefreshToken}
	err = env.GetAuthDB().InsertOrUpdateUserToken(context.Background(), subID, token)
	require.NoError(t, err, "could not insert token")
	response = httptest.NewRecorder()
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticated(t, authCtx)
	newJwtCookie := getResponseCookie(response.Result(), jwtCookie)
	require.NotNil(t, newJwtCookie, "JWT cookie should be updated")
	require.Equal(t, refreshedJWT, newJwtCookie.Value, "JWT cookie should be updated")
	require.Equal(t, validUserToken, authCtx.Value(contextUserKey), "context user details should match details returned by provider")
}

func getResponseCookie(response *http.Response, name string) *http.Cookie {
	for _, c := range response.Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func requireAuthenticationError(t *testing.T, ctx context.Context) {
	require.NotNil(t, ctx.Value(contextUserErrorKey), "context auth error key should be set")
	require.Nil(t, ctx.Value(contextTokenStringKey), "context auth jwt token should not be set")
}

func requireAuthenticated(t *testing.T, ctx context.Context) {
	require.Nil(t, ctx.Value(contextUserErrorKey), "context auth error key should not be set")
	require.NotNil(t, ctx.Value(contextTokenStringKey), "context auth jwt token should be set")
}
