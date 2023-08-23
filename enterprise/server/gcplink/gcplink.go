package gcplink

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/cookie"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

const (
	Issuer         = "https://accounts.google.com"
	scope          = "https://www.googleapis.com/auth/cloud-platform"
	linkParamName  = "link_gcp_for_group"
	linkCookieName = "link-gcp-for-group"
	cookieDuration = 1 * time.Hour
	// These can be used to call gcloud auth activate-refresh-token token $CLOUDSDK_AUTH_REFRESH_TOKEN
	refreshTokenEnvVariableName = "CLOUDSDK_AUTH_REFRESH_TOKEN"
)

// Returns true if the request contains either a gcp link url param or cookie.
func IsLinkRequest(r *http.Request) bool {
	return (r.URL.Query().Get(linkParamName) != "" && cookie.GetCookie(r, cookie.AuthIssuerCookie) == Issuer) ||
		cookie.GetCookie(r, linkCookieName) != ""
}

// Redirects the user to the auth flow with a request for the GCP scope.
func RequestAccess(w http.ResponseWriter, r *http.Request, authUrl string) {
	authUrl = strings.ReplaceAll(authUrl, "scope=", fmt.Sprintf("scope=%s+", scope))
	cookie.SetCookie(w, linkCookieName, r.URL.Query().Get(linkParamName), time.Now().Add(cookieDuration), true)
	http.Redirect(w, r, authUrl, http.StatusTemporaryRedirect)
}

// Accepts the redirect from the auth provider and stores the results as secrets.
func LinkForGroup(env environment.Env, w http.ResponseWriter, r *http.Request, refreshToken string) error {
	if refreshToken == "" {
		return status.PermissionDeniedErrorf("Empty refresh token")
	}
	rc := &ctxpb.RequestContext{
		GroupId: cookie.GetCookie(r, linkCookieName),
	}
	cookie.ClearCookie(w, linkCookieName)
	ctx := requestcontext.ContextWithProtoRequestContext(r.Context(), rc)
	ctx = env.GetAuthenticator().AuthenticatedHTTPContext(w, r.WithContext(ctx))
	secretResponse, err := env.GetSecretService().GetPublicKey(ctx, &skpb.GetPublicKeyRequest{
		RequestContext: rc,
	})
	if err != nil {
		return status.PermissionDeniedErrorf("Error getting public key: %s", err)
	}
	box, err := keystore.NewAnonymousSealedBox(secretResponse.PublicKey.Value, refreshToken)
	if err != nil {
		return status.PermissionDeniedErrorf("Error sealing box: %s", err)
	}
	_, _, err = env.GetSecretService().UpdateSecret(ctx, &skpb.UpdateSecretRequest{
		RequestContext: rc,
		Secret: &skpb.Secret{
			Name:  refreshTokenEnvVariableName,
			Value: box,
		},
	})
	if err != nil {
		return status.PermissionDeniedErrorf("Error updating secret: %s", err)
	}
	http.Redirect(w, r, cookie.GetCookie(r, cookie.RedirCookie), http.StatusTemporaryRedirect)
	return nil
}
