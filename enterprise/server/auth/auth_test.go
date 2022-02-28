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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

const (
	testIssuer        = "testIssuer"
	testSlug          = "test-slug"
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

func (f fakeOidcAuthenticator) getSlug() string {
	return testSlug
}

func (f fakeOidcAuthenticator) authCodeURL(state string, opts ...oauth2.AuthCodeOption) (string, error) {
	return "https://auth.test", nil
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

func (f fakeOidcAuthenticator) checkAccessToken(ctx context.Context, jwt, accessToken string) error {
	return nil
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

	sessionID := "e34ff952-6ef0-4a35-ae3d-fe6166fe277e"
	// Valid JWT cookie, but user does not exist.
	request.AddCookie(&http.Cookie{Name: jwtCookie, Value: validJWT})
	request.AddCookie(&http.Cookie{Name: authIssuerCookie, Value: testIssuer})
	request.AddCookie(&http.Cookie{Name: sessionIDCookie, Value: sessionID})
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticationError(t, authCtx)

	// User information should still be populated (it's needed for user creation).
	require.Equal(t, validUserToken, authCtx.Value(contextUserKey), "context user details should match details returned by provider")

	// Create matching user, and token. Authentication should now succeed.
	user := &tables.User{
		UserID: userID,
		SubID:  subID,
		Email:  userEmail,
	}
	err = env.GetUserDB().InsertUser(context.Background(), user)
	require.NoError(t, err, "could not insert user")
	err = env.GetAuthDB().InsertOrUpdateUserSession(context.Background(), sessionID, &tables.Session{
		SessionID:    sessionID,
		SubID:        subID,
		AccessToken:  "access",
		RefreshToken: "refresh",
	})
	require.NoError(t, err, "could not insert token")
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticated(t, authCtx)
	require.Equal(t, validUserToken, authCtx.Value(contextUserKey), "context user details should match details returned by provider")

	// Send request with an expired JWT cookie.
	// DB doesn't have a refresh token so authentication should fail.
	request, err = http.NewRequest(http.MethodGet, "/", strings.NewReader(""))
	require.NoErrorf(t, err, "could not create HTTP request")
	request.AddCookie(&http.Cookie{Name: jwtCookie, Value: expiredJWT})
	request.AddCookie(&http.Cookie{Name: authIssuerCookie, Value: testIssuer})
	request.AddCookie(&http.Cookie{Name: sessionIDCookie, Value: sessionID})
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticationError(t, authCtx)

	// Insert a refresh token into the DB & auth should succeed.
	session := &tables.Session{RefreshToken: validRefreshToken}
	err = env.GetAuthDB().InsertOrUpdateUserSession(context.Background(), sessionID, session)
	require.NoError(t, err, "could not insert token")
	response = httptest.NewRecorder()
	authCtx = auth.AuthenticatedHTTPContext(response, request)
	requireAuthenticated(t, authCtx)
	newJwtCookie := getResponseCookie(response.Result(), jwtCookie)
	require.NotNil(t, newJwtCookie, "JWT cookie should be updated")
	require.Equal(t, refreshedJWT, newJwtCookie.Value, "JWT cookie should be updated")
	require.Equal(t, validUserToken, authCtx.Value(contextUserKey), "context user details should match details returned by provider")
}

func TestParseAPIKeyFromString(t *testing.T) {
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{})
	auth, err := newForTesting(context.Background(), env, &fakeOidcAuthenticator{})
	require.NoError(t, err)
	testCases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:    "non-empty key",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=abc123'",
			want:    "abc123",
			wantErr: false,
		},
		{
			name:    "empty key",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key='",
			want:    "",
			wantErr: true,
		},
		{
			name:    "empty key in the middle",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=' --bes_backend=grpc://localhost:1985",
			want:    "",
			wantErr: true,
		},
		{
			name:    "key not set",
			input:   "--bes_results_url=http://localhost:8080/invocation/",
			want:    "",
			wantErr: false,
		},
		{
			name:    "multiple API keys",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=abc123' --remote_header='x-buildbuddy-api-key=abc456",
			want:    "abc456",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := auth.ParseAPIKeyFromString(tc.input)
			assert.Equal(t, tc.want, output)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
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
	require.Nil(t, ctx.Value(contextUserErrorKey), ctx.Value(contextUserErrorKey))
	require.NotNil(t, ctx.Value(contextTokenStringKey), "context auth jwt token should be set")
}
