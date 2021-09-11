package saml

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"net/http"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/crewjam/saml/samlsp"
)

const (
	authRedirectParam     = "redirect_url"
	slugParam             = "slug"
	slugCookie            = "Slug"
	loginCookieDuration   = 365*24*time.Hour + time.Hour
	contextSamlSessionKey = "saml.session"
)

type SAMLAuthenticator struct {
	env           environment.Env
	samlProviders map[string]*samlsp.Middleware
}

func NewSAMLAuthenticator(env environment.Env) *SAMLAuthenticator {
	return &SAMLAuthenticator{
		env:           env,
		samlProviders: make(map[string]*samlsp.Middleware),
	}
}

func (a *SAMLAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	if slug := r.URL.Query().Get(slugParam); slug != "" {
		setCookie(w, slugCookie, slug, time.Now().Add(loginCookieDuration))
	}

	sp, err := a.serviceProviderFromRequest(r)
	if err != nil {
		redirectWithError(w, r, err)
		return
	}

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
		session, _ := sp.Session.GetSession(r)
		sa, ok := session.(samlsp.SessionWithAttributes)
		if ok {
			ctx = context.WithValue(ctx, contextSamlSessionKey, sa)
		}
	}
	return ctx
}

func (a *SAMLAuthenticator) SubjectIDFromContext(ctx context.Context) string {
	if sa, ok := ctx.Value(contextSamlSessionKey).(samlsp.SessionWithAttributes); ok {
		return sa.GetAttributes().Get("urn:oasis:names:tc:SAML:attribute:subject-id")
	}
	return ""
}

func (a *SAMLAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	pk, err := tables.PrimaryKeyForTable("Users")
	if err != nil {
		return err
	}

	if sa, ok := ctx.Value(contextSamlSessionKey).(samlsp.SessionWithAttributes); ok {
		attributes := sa.GetAttributes()
		user.UserID = pk
		user.SubID = attributes.Get("urn:oasis:names:tc:SAML:attribute:subject-id")
		// todo are these standard?
		user.FirstName = attributes.Get("givenName")
		user.LastName = attributes.Get("sn")
		user.Email = attributes.Get("mail")
		// user.ImageURL = attributes.Get()
		return nil
	}
	return status.NotFoundErrorf("User not found")
}

func (a *SAMLAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	if sp, err := a.serviceProviderFromRequest(r); err == nil {
		sp.Session.DeleteSession(w, r)
	}
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

	SAMLConfig := a.env.GetConfigurator().GetSAMLConfig()
	if SAMLConfig == nil {
		return nil, status.NotFoundError("No SAML Configured")
	}

	keyPair, err := tls.LoadX509KeyPair(SAMLConfig.CertFile, SAMLConfig.KeyFile)
	if err != nil {
		return nil, err
	}
	keyPair.Leaf, err = x509.ParseCertificate(keyPair.Certificate[0])
	if err != nil {
		return nil, err
	}

	samlURL, err := a.getSAMLUrlForSlug(r.Context(), slug)
	if err != nil {
		return nil, err
	}

	idpMetadataURL, err := url.Parse(samlURL)
	if err != nil {
		return nil, err
	}

	idpMetadata, err := samlsp.FetchMetadata(context.Background(), http.DefaultClient,
		*idpMetadataURL)
	if err != nil {
		return nil, err
	}

	myURL, err := url.Parse(a.env.GetConfigurator().GetAppBuildBuddyURL())
	if err != nil {
		return nil, err
	}

	authURL, err := myURL.Parse("/auth/")
	if err != nil {
		return nil, err
	}

	entityURL, err := myURL.Parse("saml/metadata")
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("%s=%s", slugParam, slug)
	entityURL.RawQuery = query

	samlSP, _ := samlsp.New(samlsp.Options{
		EntityID:    entityURL.String(),
		URL:         *authURL,
		Key:         keyPair.PrivateKey.(*rsa.PrivateKey),
		Certificate: keyPair.Leaf,
		IDPMetadata: idpMetadata,
	})

	samlSP.ServiceProvider.MetadataURL.RawQuery = query
	samlSP.ServiceProvider.AcsURL.RawQuery = query
	samlSP.ServiceProvider.SloURL.RawQuery = query

	a.samlProviders[slug] = samlSP
	return samlSP, nil
}

func (a *SAMLAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	sp, err := a.serviceProviderFromRequest(r)
	if err != nil {
		redirectWithError(w, r, err)
		return
	}

	sp.ServeHTTP(w, r)
}

func (a *SAMLAuthenticator) getSlugFromRequest(r *http.Request) string {
	slug := r.URL.Query().Get(slugParam)
	if slug == "" {
		slug = getCookie(r, slugCookie)
	}
	return slug
}

func (a *SAMLAuthenticator) getSAMLUrlForSlug(ctx context.Context, slug string) (string, error) {
	group, err := a.groupForSlug(ctx, slug)
	if err != nil {
		return "", err
	}
	if group.SamlIdpMetadataUrl == "" {
		return "", status.NotFoundErrorf("Group %s does not have SAML configured", slug)
	}
	return group.SamlIdpMetadataUrl, nil
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

func redirectWithError(w http.ResponseWriter, r *http.Request, err error) {
	log.Warning(err.Error())
	http.Redirect(w, r, "/?error="+url.QueryEscape(err.Error()), http.StatusTemporaryRedirect)
}

func setCookie(w http.ResponseWriter, name, value string, expiry time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Expires:  expiry,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})
}

func getCookie(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}
