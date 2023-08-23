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
	Issuer                      = "https://accounts.google.com"
	scope                       = "https://www.googleapis.com/auth/cloud-platform"
	paramName                   = "link_gcp_for_group"
	cookieName                  = "link-gcp-for-group"
	cookieDuration              = 1 * time.Hour
	refreshTokenEnvVariableName = "CLOUDSDK_AUTH_REFRESH_TOKEN"
	authAccountEnvVariableName  = "CLOUDSDK_AUTH_ACCOUNT"
)

func IsLinkRequest(r *http.Request) bool {
	return (r.URL.Query().Get(paramName) != "" && cookie.GetCookie(r, cookie.AuthIssuerCookie) == Issuer) ||
		cookie.GetCookie(r, cookieName) != ""
}

func RequestAccess(w http.ResponseWriter, r *http.Request, authUrl string) {
	authUrl = strings.ReplaceAll(authUrl, "scope=", fmt.Sprintf("scope=%s+", scope))
	cookie.SetCookie(w, cookieName, r.URL.Query().Get(paramName), time.Now().Add(cookieDuration), true)
	http.Redirect(w, r, authUrl, http.StatusTemporaryRedirect)
}

func LinkForGroup(env environment.Env, w http.ResponseWriter, r *http.Request, email, refreshToken string) error {
	rc := &ctxpb.RequestContext{
		GroupId: cookie.GetCookie(r, cookieName),
	}
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
	box, err = keystore.NewAnonymousSealedBox(secretResponse.PublicKey.Value, email)
	if err != nil {
		return status.PermissionDeniedErrorf("Error sealing box: %s", err)
	}
	_, _, err = env.GetSecretService().UpdateSecret(ctx, &skpb.UpdateSecretRequest{
		RequestContext: rc,
		Secret: &skpb.Secret{
			Name:  authAccountEnvVariableName,
			Value: box,
		},
	})
	if err != nil {
		return status.PermissionDeniedErrorf("Error updating secret: %s", err)
	}
	cookie.ClearCookie(w, cookieName)
	http.Redirect(w, r, cookie.GetCookie(r, cookie.RedirCookie), http.StatusTemporaryRedirect)
	return nil
}
