package saml

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
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
	"github.com/crewjam/saml"
	"github.com/crewjam/saml/samlsp"
	"github.com/golang-jwt/jwt"
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
	sessionCookieName      = "token"
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

// CookieRequestTracker tracks requests by setting a uniquely named
// cookie for each request.
//
// This is a modified version of samlsp.CookieRequestTracker that also sets
// the domain field on the cookies.
type CookieRequestTracker struct {
	ServiceProvider *saml.ServiceProvider
	NamePrefix      string
	Codec           samlsp.TrackedRequestCodec
	MaxAge          time.Duration
	RelayStateFunc  func(w http.ResponseWriter, r *http.Request) string
	SameSite        http.SameSite
}

// TrackRequest starts tracking the SAML request with the given ID. It returns
// an `index` that should be used as the RelayState in the SAMl request flow.
func (t CookieRequestTracker) TrackRequest(w http.ResponseWriter, r *http.Request, samlRequestID string) (string, error) {
	randomBytes := make([]byte, 42)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	trackedRequest := samlsp.TrackedRequest{
		Index:         base64.RawURLEncoding.EncodeToString(randomBytes),
		SAMLRequestID: samlRequestID,
		URI:           r.URL.String(),
	}

	if t.RelayStateFunc != nil {
		relayState := t.RelayStateFunc(w, r)
		if relayState != "" {
			trackedRequest.Index = relayState
		}
	}

	signedTrackedRequest, err := t.Codec.Encode(trackedRequest)
	if err != nil {
		return "", err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     t.NamePrefix + trackedRequest.Index,
		Value:    signedTrackedRequest,
		Domain:   cookie.Domain(),
		MaxAge:   int(t.MaxAge.Seconds()),
		HttpOnly: true,
		SameSite: t.SameSite,
		Secure:   t.ServiceProvider.AcsURL.Scheme == "https",
		Path:     t.ServiceProvider.AcsURL.Path,
	})

	return trackedRequest.Index, nil
}

// StopTrackingRequest stops tracking the SAML request given by index, which is
// a string previously returned from TrackRequest
func (t CookieRequestTracker) StopTrackingRequest(w http.ResponseWriter, r *http.Request, index string) error {
	c, err := r.Cookie(t.NamePrefix + index)
	if err != nil {
		return err
	}
	c.Value = ""
	c.Domain = cookie.Domain()
	// past time as close to epoch as possible, but not zero time.Time{}
	c.Expires = time.Unix(1, 0)
	http.SetCookie(w, c)
	return nil
}

// GetTrackedRequests returns all the pending tracked requests
func (t CookieRequestTracker) GetTrackedRequests(r *http.Request) []samlsp.TrackedRequest {
	rv := []samlsp.TrackedRequest{}
	for _, cookie := range r.Cookies() {
		if !strings.HasPrefix(cookie.Name, t.NamePrefix) {
			continue
		}

		trackedRequest, err := t.Codec.Decode(cookie.Value)
		if err != nil {
			continue
		}
		index := strings.TrimPrefix(cookie.Name, t.NamePrefix)
		if index != trackedRequest.Index {
			continue
		}

		rv = append(rv, *trackedRequest)
	}
	return rv
}

// GetTrackedRequest returns a pending tracked request.
func (t CookieRequestTracker) GetTrackedRequest(r *http.Request, index string) (*samlsp.TrackedRequest, error) {
	cookie, err := r.Cookie(t.NamePrefix + index)
	if err != nil {
		return nil, err
	}

	trackedRequest, err := t.Codec.Decode(cookie.Value)
	if err != nil {
		return nil, err
	}
	if trackedRequest.Index != index {
		return nil, fmt.Errorf("expected index %q, got %q", index, trackedRequest.Index)
	}
	return trackedRequest, nil
}

// wrapper around samlsp.CookieSesionProvider that allows a seamless migration
// of cookies to a different domain value.
type cookieSessionProvider struct {
	oldDomain string
	d         samlsp.CookieSessionProvider
}

func (p cookieSessionProvider) CreateSession(w http.ResponseWriter, r *http.Request, assertion *saml.Assertion) error {
	if p.oldDomain != "" {
		clone := p.d
		clone.Domain = p.oldDomain
		if err := clone.DeleteSession(w, r); err != nil {
			return err
		}
	}
	return p.d.CreateSession(w, r, assertion)
}

func (p cookieSessionProvider) DeleteSession(w http.ResponseWriter, r *http.Request) error {
	if p.oldDomain != "" {
		clone := p.d
		clone.Domain = p.oldDomain
		if err := clone.DeleteSession(w, r); err != nil {
			return err
		}
	}
	return p.d.DeleteSession(w, r)
}

func (p cookieSessionProvider) GetSession(r *http.Request) (samlsp.Session, error) {
	return p.d.GetSession(r)
}

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
	opts := samlsp.Options{
		EntityID:          entityURL.String(),
		URL:               *build_buddy_url.WithPath("/auth/"),
		Key:               keyPair.PrivateKey.(*rsa.PrivateKey),
		Certificate:       keyPair.Leaf,
		IDPMetadata:       idpMetadata,
		AllowIDPInitiated: true,
	}
	samlSP, _ := samlsp.New(opts)
	samlSP.ServiceProvider.MetadataURL.RawQuery = query
	samlSP.ServiceProvider.AcsURL.RawQuery = query
	samlSP.ServiceProvider.SloURL.RawQuery = query
	samlSP.RequestTracker = &CookieRequestTracker{
		ServiceProvider: &samlSP.ServiceProvider,
		NamePrefix:      "saml_",
		Codec:           samlsp.DefaultTrackedRequestCodec(opts),
		MaxAge:          saml.MaxIssueDelay,
		RelayStateFunc:  opts.RelayStateFunc,
		SameSite:        opts.CookieSameSite,
	}
	sessionCodec := &samlsp.JWTSessionCodec{
		SigningMethod: jwt.SigningMethodRS256,
		Audience:      opts.URL.String(),
		Issuer:        opts.URL.String(),
		MaxAge:        sessionDuration,
		Key:           opts.Key,
	}
	sessionDomain := opts.URL.Host
	if cookie.Domain() != "" {
		sessionDomain = cookie.Domain()
	}
	csp := &cookieSessionProvider{d: samlsp.CookieSessionProvider{
		Name:     sessionCookieName,
		Domain:   sessionDomain,
		MaxAge:   sessionDuration,
		HTTPOnly: true,
		Secure:   opts.URL.Scheme == "https",
		SameSite: opts.CookieSameSite,
		Codec:    sessionCodec,
	}}
	if cookie.Domain() != "" {
		csp.oldDomain = opts.URL.Host
	}
	samlSP.Session = csp
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
