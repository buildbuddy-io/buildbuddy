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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/cookie"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/crewjam/saml/samlsp"
)

var (
	certFile = flag.String("auth.saml.cert_file", "", "Path to a PEM encoded certificate file used for SAML auth.")
	cert     = flagutil.New("auth.saml.cert", "", "PEM encoded certificate used for SAML auth.", flagutil.SecretTag)
	keyFile  = flag.String("auth.saml.key_file", "", "Path to a PEM encoded certificate key file used for SAML auth.")
	key      = flagutil.New("auth.saml.key", "", "PEM encoded certificate key used for SAML auth.", flagutil.SecretTag)
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
	mu            sync.Mutex
	samlProviders map[string]*samlsp.Middleware
}

func IsEnabled(env environment.Env) bool {
	if (*certFile == "" && *cert == "") || env.GetAuthenticator() == nil {
		return false
	}
	return true
}

func NewSAMLAuthenticator(env environment.Env) *SAMLAuthenticator {
	return &SAMLAuthenticator{
		env:           env,
		samlProviders: make(map[string]*samlsp.Middleware),
	}
}

func (a *SAMLAuthenticator) SSOEnabled() bool {
	return true
}

func (a *SAMLAuthenticator) Login(w http.ResponseWriter, r *http.Request) error {
	slug := a.getSlugFromRequest(r)
	if slug == "" {
		return status.FailedPreconditionError("SAML Login attempted without a slug")
	}
	sp, err := a.serviceProviderFromRequest(r)
	if err != nil {
		return err
	}
	cookie.SetCookie(w, slugCookie, slug, time.Now().Add(cookieDuration), true /* httpOnly= */)
	session, err := sp.Session.GetSession(r)
	if session != nil {
		redirectURL := r.URL.Query().Get(authRedirectParam)
		if redirectURL == "" {
			redirectURL = "/" // default to redirecting home.
		}
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return nil
	}
	sp.HandleStartAuthFlow(w, r)
	return nil
}

func (a *SAMLAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	ctx := r.Context()
	if sp, err := a.serviceProviderFromRequest(r); err == nil {
		session, err := sp.Session.GetSession(r)
		if err != nil {
			return authutil.AuthContextWithError(ctx, status.PermissionDeniedErrorf("%s: %s", authutil.ExpiredSessionMsg, err.Error()))
		}
		sa, ok := session.(samlsp.SessionWithAttributes)
		if ok {
			ctx = context.WithValue(ctx, contextSamlSessionKey, sa)
			ctx = context.WithValue(ctx, contextSamlEntityIDKey, sp.ServiceProvider.EntityID)
			ctx = context.WithValue(ctx, contextSamlSlugKey, a.getSlugFromRequest(r))
			return ctx
		}
	} else if slug := cookie.GetCookie(r, slugCookie); slug != "" {
		return authutil.AuthContextWithError(ctx, status.PermissionDeniedErrorf("Error getting service provider for slug %s: %s", slug, err.Error()))
	}
	return ctx
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
	return status.UnauthenticatedError("No SAML User found")
}

func (a *SAMLAuthenticator) Logout(w http.ResponseWriter, r *http.Request) error {
	cookie.ClearCookie(w, slugCookie)
	if sp, err := a.serviceProviderFromRequest(r); err == nil {
		sp.Session.DeleteSession(w, r)
	}
	return status.UnauthenticatedError("Logged out!")
}

func (a *SAMLAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	if s, _ := a.subjectIDAndSessionFromContext(ctx); s != "" {
		claims, err := claims.ClaimsFromSubID(ctx, a.env, s)
		if err != nil {
			return nil, status.UnauthenticatedErrorf(authutil.UserNotFoundMsg)
		}
		return claims, nil
	}
	return nil, status.UnauthenticatedError("No SAML User found")
}

func (a *SAMLAuthenticator) Auth(w http.ResponseWriter, r *http.Request) error {
	if !strings.HasPrefix(r.URL.Path, "/auth/saml/") {
		return status.NotFoundErrorf("Auth path does not have SAML prefix %s:", r.URL.Path)
	}
	sp, err := a.serviceProviderFromRequest(r)
	if err != nil {
		cookie.ClearCookie(w, slugCookie)
		return status.NotFoundErrorf("SAML Auth Failed: %s", err)
	}
	// Store slug as a cookie to enable logins directly from the /acs page.
	slug := r.URL.Query().Get(slugParam)
	cookie.SetCookie(w, slugCookie, slug, time.Now().Add(cookieDuration), true /* httpOnly= */)

	sp.ServeHTTP(w, r)
	return nil
}

func (a *SAMLAuthenticator) serviceProviderFromRequest(r *http.Request) (*samlsp.Middleware, error) {
	slug := a.getSlugFromRequest(r)
	if slug == "" {
		return nil, status.FailedPreconditionError("Organization slug not set")
	}
	a.mu.Lock()
	provider, ok := a.samlProviders[slug]
	a.mu.Unlock()
	if ok {
		return provider, nil
	}
	if (*certFile == "" && *cert == "") || (*keyFile == "" && *key == "") {
		return nil, status.UnauthenticatedError("No SAML Configured")
	}
	if *certFile != "" && *cert != "" {
		return nil, status.FailedPreconditionError("SAML cert should be specified as a file or directly, but not both")
	}
	if *keyFile != "" && *key != "" {
		return nil, status.FailedPreconditionError("SAML key should be specified as a file or directly, but not both")
	}
	var certData []byte
	if *cert != "" {
		certData = []byte(*cert)
	} else {
		data, err := os.ReadFile(*certFile)
		if err != nil {
			return nil, err
		}
		certData = data
	}
	var keyData []byte
	if *key != "" {
		keyData = []byte(*key)
	} else {
		data, err := os.ReadFile(*keyFile)
		if err != nil {
			return nil, err
		}
		keyData = data
	}
	keyPair, err := tls.X509KeyPair(certData, keyData)
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
	a.mu.Lock()
	a.samlProviders[slug] = samlSP
	a.mu.Unlock()
	return samlSP, nil
}

func (a *SAMLAuthenticator) getSlugFromRequest(r *http.Request) string {
	slug := r.URL.Query().Get(slugParam)
	if slug == "" {
		slug = cookie.GetCookie(r, slugCookie)
	}
	return slug
}

func (a *SAMLAuthenticator) getSAMLMetadataUrlForSlug(ctx context.Context, slug string) (*url.URL, error) {
	group, err := a.groupForSlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	if group.SamlIdpMetadataUrl == nil || *group.SamlIdpMetadataUrl == "" {
		return nil, status.UnauthenticatedErrorf("Group %s does not have SAML configured", slug)
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
		return nil, status.UnauthenticatedErrorf("Not Implemented")
	}
	if slug == "" {
		return nil, status.UnauthenticatedErrorf("Slug is required.")
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
