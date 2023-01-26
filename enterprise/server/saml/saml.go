package saml

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/crewjam/saml/samlsp"
)

var (
	certFile = flag.String("auth.saml.cert_file", "", "Path to a PEM encoded certificate file used for SAML auth.")
	keyFile  = flag.String("auth.saml.key_file", "", "Path to a PEM encoded certificate key file used for SAML auth.")
)

const (
	authRedirectParam      = "redirect_url"
	slugParam              = "slug"
	slugCookie             = "Slug"
	cookieDuration         = 365 * 24 * time.Hour
	sessionDuration        = 12 * time.Hour
	contextSamlSessionKey  = "saml.session"
	contextSamlEntityIDKey = "saml.entityID"
	contextSamlSlugKey     = "saml.slug"
)

var (
	samlFirstNameAttributes = []string{"FirstName", "givenName", "givenname", "given_name"}
	samlLastNameAttributes  = []string{"LastName", "surname", "sn"}
	samlEmailAttributes     = []string{"email", "mail", "emailAddress", "Email", "emailaddress", "email_address"}
	samlSubjectAttributes   = append([]string{"urn:oasis:names:tc:SAML:attribute:subject-id", "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", "user_id", "username"}, samlEmailAttributes...)
)

type SAMLAuthenticator struct {
	env           environment.Env
	samlProviders map[string]*samlsp.Middleware
	fallback      interfaces.Authenticator
}

func Register(env environment.Env) error {
	if *certFile == "" || env.GetAuthenticator() == nil {
		return nil
	}
	if _, ok := env.GetAuthenticator().(*nullauth.NullAuthenticator); ok {
		return nil
	}
	log.Info("SAML auth configured.")
	env.SetAuthenticator(NewSAMLAuthenticator(env, env.GetAuthenticator()))
	return nil
}

func NewSAMLAuthenticator(env environment.Env, fallback interfaces.Authenticator) *SAMLAuthenticator {
	return &SAMLAuthenticator{
		env:           env,
		samlProviders: make(map[string]*samlsp.Middleware),
		fallback:      fallback,
	}
}

func (a *SAMLAuthenticator) AdminGroupID() string {
	return a.fallback.AdminGroupID()
}

func (a *SAMLAuthenticator) AnonymousUsageEnabled() bool {
	return a.fallback.AnonymousUsageEnabled()
}

func (a *SAMLAuthenticator) PublicIssuers() []string {
	return a.fallback.PublicIssuers()
}

func (a *SAMLAuthenticator) SSOEnabled() bool {
	if *certFile != "" {
		return true
	}
	return a.fallback.SSOEnabled()
}

func (a *SAMLAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	slug := a.getSlugFromRequest(r)
	if slug == "" {
		a.fallback.Login(w, r)
		return
	}
	sp, err := a.serviceProviderFromRequest(r)
	if err != nil {
		a.fallback.Login(w, r)
		return
	}
	auth.SetCookie(a.env, w, slugCookie, slug, time.Now().Add(cookieDuration), true /* httpOnly= */)
	session, err := sp.Session.GetSession(r)
	if session != nil {
		redirectURL := r.URL.Query().Get(authRedirectParam)
		if redirectURL == "" {
			redirectURL = "/" // default to redirecting home.
		}
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return
	}
	sp.HandleStartAuthFlow(w, r)
}

func (a *SAMLAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	ctx := r.Context()
	if sp, err := a.serviceProviderFromRequest(r); err == nil {
		session, err := sp.Session.GetSession(r)
		if err != nil {
			return auth.AuthContextWithError(ctx, status.PermissionDeniedErrorf("%s: %s", auth.ExpiredSessionMsg, err.Error()))
		}
		sa, ok := session.(samlsp.SessionWithAttributes)
		if ok {
			ctx = context.WithValue(ctx, contextSamlSessionKey, sa)
			ctx = context.WithValue(ctx, contextSamlEntityIDKey, sp.ServiceProvider.EntityID)
			ctx = context.WithValue(ctx, contextSamlSlugKey, a.getSlugFromRequest(r))
			return ctx
		}
	} else if slug := auth.GetCookie(r, slugCookie); slug != "" {
		return auth.AuthContextWithError(ctx, status.PermissionDeniedErrorf("Error getting service provider for slug %s: %s", slug, err.Error()))
	}
	return a.fallback.AuthenticatedHTTPContext(w, r)
}

func (a *SAMLAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	pk, err := tables.PrimaryKeyForTable("Users")
	if err != nil {
		return err
	}
	if subjectID, session := a.subjectIDAndSessionFromContext(ctx); subjectID != "" && session != nil {
		attributes := session.GetAttributes()
		user.UserID = pk
		user.SubID = subjectID
		user.FirstName = firstSet(attributes, samlFirstNameAttributes)
		user.LastName = firstSet(attributes, samlLastNameAttributes)
		user.Email = firstSet(attributes, samlEmailAttributes)
		if slug, ok := ctx.Value(contextSamlSlugKey).(string); ok && slug != "" {
			user.Groups = []*tables.GroupRole{
				{Group: tables.Group{URLIdentifier: &slug}},
			}
		}
		return nil
	}
	return a.fallback.FillUser(ctx, user)
}

func (a *SAMLAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	auth.ClearCookie(a.env, w, slugCookie)
	if sp, err := a.serviceProviderFromRequest(r); err == nil {
		sp.Session.DeleteSession(w, r)
	}
	a.fallback.Logout(w, r)
}

func (a *SAMLAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	if s, _ := a.subjectIDAndSessionFromContext(ctx); s != "" {
		claims, err := auth.ClaimsFromSubID(ctx, a.env, s)
		return claims, err
	}
	return a.fallback.AuthenticatedUser(ctx)
}

func (a *SAMLAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/auth/saml/") {
		a.fallback.Auth(w, r)
		return
	}
	sp, err := a.serviceProviderFromRequest(r)
	if err != nil {
		log.Warningf("SAML Auth Failed: %s", err)
		auth.ClearCookie(a.env, w, slugCookie)
		http.Redirect(w, r, "/?error="+url.QueryEscape(err.Error()), http.StatusTemporaryRedirect)
		return
	}
	// Store slug as a cookie to enable logins directly from the /acs page.
	slug := r.URL.Query().Get(slugParam)
	auth.SetCookie(a.env, w, slugCookie, slug, time.Now().Add(cookieDuration), true /* httpOnly= */)

	sp.ServeHTTP(w, r)
}

func (a *SAMLAuthenticator) AuthenticatedGRPCContext(ctx context.Context) context.Context {
	return a.fallback.AuthenticatedGRPCContext(ctx)
}

func (a *SAMLAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	return a.fallback.AuthenticateGRPCRequest(ctx)
}

func (a *SAMLAuthenticator) ParseAPIKeyFromString(s string) (string, error) {
	return a.fallback.ParseAPIKeyFromString(s)
}

func (a *SAMLAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	return a.fallback.AuthContextFromAPIKey(ctx, apiKey)
}

func (a *SAMLAuthenticator) AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context {
	return a.fallback.AuthContextFromTrustedJWT(ctx, jwt)
}

func (a *SAMLAuthenticator) TrustedJWTFromAuthContext(ctx context.Context) string {
	return a.fallback.TrustedJWTFromAuthContext(ctx)
}

func (a *SAMLAuthenticator) serviceProviderFromRequest(r *http.Request) (*samlsp.Middleware, error) {
	slug := a.getSlugFromRequest(r)
	if slug == "" {
		return nil, status.FailedPreconditionError("Organization slug not set")
	}
	provider, ok := a.samlProviders[slug]
	if ok {
		return provider, nil
	}
	if *certFile == "" || *keyFile == "" {
		return nil, status.NotFoundError("No SAML Configured")
	}
	keyPair, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		return nil, err
	}
	keyPair.Leaf, err = x509.ParseCertificate(keyPair.Certificate[0])
	if err != nil {
		return nil, err
	}
	idpMetadataURL, err := a.getSAMLMetadataUrlForSlug(r.Context(), slug)
	if err != nil {
		return nil, err
	}
	idpMetadata, err := samlsp.FetchMetadata(context.Background(), http.DefaultClient,
		*idpMetadataURL)
	if err != nil {
		return nil, err
	}
	entityURL := build_buddy_url.WithPath("saml/metadata")
	query := fmt.Sprintf("%s=%s", slugParam, slug)
	entityURL.RawQuery = query
	samlSP, _ := samlsp.New(samlsp.Options{
		EntityID:          entityURL.String(),
		URL:               *build_buddy_url.WithPath("/auth/"),
		Key:               keyPair.PrivateKey.(*rsa.PrivateKey),
		Certificate:       keyPair.Leaf,
		IDPMetadata:       idpMetadata,
		AllowIDPInitiated: true,
	})
	samlSP.ServiceProvider.MetadataURL.RawQuery = query
	samlSP.ServiceProvider.AcsURL.RawQuery = query
	samlSP.ServiceProvider.SloURL.RawQuery = query
	if cookieProvider, ok := samlSP.Session.(samlsp.CookieSessionProvider); ok {
		cookieProvider.MaxAge = sessionDuration
		if codec, ok := cookieProvider.Codec.(samlsp.JWTSessionCodec); ok {
			codec.MaxAge = sessionDuration
			cookieProvider.Codec = codec
		}
		samlSP.Session = cookieProvider
	}
	a.samlProviders[slug] = samlSP
	return samlSP, nil
}

func (a *SAMLAuthenticator) getSlugFromRequest(r *http.Request) string {
	slug := r.URL.Query().Get(slugParam)
	if slug == "" {
		slug = auth.GetCookie(r, slugCookie)
	}
	return slug
}

func (a *SAMLAuthenticator) getSAMLMetadataUrlForSlug(ctx context.Context, slug string) (*url.URL, error) {
	group, err := a.groupForSlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	if group.SamlIdpMetadataUrl == nil || *group.SamlIdpMetadataUrl == "" {
		return nil, status.NotFoundErrorf("Group %s does not have SAML configured", slug)
	}
	metadataUrl, err := url.Parse(*group.SamlIdpMetadataUrl)
	if err != nil {
		return nil, err
	}
	return metadataUrl, nil
}

func (a *SAMLAuthenticator) groupForSlug(ctx context.Context, slug string) (*tables.Group, error) {
	userDB := a.env.GetUserDB()
	if userDB == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	if slug == "" {
		return nil, status.InvalidArgumentError("Slug is required.")
	}
	return userDB.GetGroupByURLIdentifier(ctx, slug)
}

func (a *SAMLAuthenticator) subjectIDAndSessionFromContext(ctx context.Context) (string, samlsp.SessionWithAttributes) {
	entityID, ok := ctx.Value(contextSamlEntityIDKey).(string)
	if !ok || entityID == "" {
		return "", nil
	}
	if sa, ok := ctx.Value(contextSamlSessionKey).(samlsp.SessionWithAttributes); ok {
		return fmt.Sprintf("%s/%s", entityID, firstSet(sa.GetAttributes(), samlSubjectAttributes)), sa
	}
	return "", nil
}

func firstSet(attributes samlsp.Attributes, keys []string) string {
	for _, key := range keys {
		value := attributes.Get(key)
		if value == "" {
			continue
		}
		return value
	}
	return ""
}
