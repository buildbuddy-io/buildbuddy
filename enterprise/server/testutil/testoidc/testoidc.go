// Package testoidc provides a small, multi-user OpenID Connect provider for
// integration tests.
package testoidc

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	ClientID     = "buildbuddy-test"
	ClientSecret = "buildbuddy-test-secret"

	keyID = "testoidc-key"
)

type identity struct {
	subject string
	email   string
}

type authorizationCode struct {
	identity    identity
	clientID    string
	redirectURI string
	nonce       string
}

// Provider is a test-only OIDC provider.
type Provider struct {
	issuerURL string
	server    *http.Server
	listener  net.Listener
	key       *rsa.PrivateKey

	mu            sync.Mutex
	users         map[string]identity
	codes         map[string]authorizationCode
	refreshTokens map[string]identity
}

// Start starts a provider on localhost. The provider is stopped automatically
// when the test completes.
func Start(t testing.TB) *Provider {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate OIDC signing key: %s", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for OIDC requests: %s", err)
	}

	port := listener.Addr().(*net.TCPAddr).Port
	p := &Provider{
		issuerURL:     fmt.Sprintf("http://localhost:%d", port),
		listener:      listener,
		key:           key,
		users:         make(map[string]identity),
		codes:         make(map[string]authorizationCode),
		refreshTokens: make(map[string]identity),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", p.handleDiscovery)
	mux.HandleFunc("/.well-known/jwks.json", p.handleJWKS)
	mux.HandleFunc("/authorize", p.handleAuthorize)
	mux.HandleFunc("/token", p.handleToken)
	p.server = &http.Server{Handler: mux}

	go func() {
		_ = p.server.Serve(listener)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.server.Shutdown(ctx)
	})
	return p
}

// IssuerURL returns the provider's OIDC issuer URL.
func (p *Provider) IssuerURL() string {
	return p.issuerURL
}

// AddUser adds or updates a selectable fixture identity. A subject uniquely
// identifies a user. Authorization codes and refresh tokens retain the
// identity that was selected when they were issued.
func (p *Provider) AddUser(subject, email string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.users[subject] = identity{subject: subject, email: email}
}

func (p *Provider) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p.writeJSON(w, map[string]any{
		"issuer":                                p.issuerURL,
		"authorization_endpoint":                p.issuerURL + "/authorize",
		"token_endpoint":                        p.issuerURL + "/token",
		"jwks_uri":                              p.issuerURL + "/.well-known/jwks.json",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"scopes_supported":                      []string{"openid", "profile", "email", "offline_access"},
		"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post"},
	})
}

func (p *Provider) handleJWKS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p.writeJSON(w, map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			"kid": keyID,
			"n":   base64.RawURLEncoding.EncodeToString(p.key.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(p.key.E)).Bytes()),
		}},
	})
}

var authorizationPage = template.Must(template.New("authorize").Parse(`<!doctype html>
<html>
<body>
<form method="post" action="/authorize">
  <input type="hidden" name="client_id" value="{{.ClientID}}">
  <input type="hidden" name="redirect_uri" value="{{.RedirectURI}}">
  <input type="hidden" name="response_type" value="{{.ResponseType}}">
  <input type="hidden" name="scope" value="{{.Scope}}">
  <input type="hidden" name="state" value="{{.State}}">
  <input type="hidden" name="nonce" value="{{.Nonce}}">
  <select name="user">
    {{range .Users}}<option value="{{.Subject}}">{{.Email}}</option>{{end}}
  </select>
  <button type="submit">Sign in</button>
</form>
</body>
</html>
`))

type authorizationPageData struct {
	ClientID     string
	RedirectURI  string
	ResponseType string
	Scope        string
	State        string
	Nonce        string
	Users        []authorizationPageUser
}

type authorizationPageUser struct {
	Subject string
	Email   string
}

func (p *Provider) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if err := validateAuthorizationRequest(r.URL.Query()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		users := make([]authorizationPageUser, 0, len(p.users))
		for _, u := range p.users {
			users = append(users, authorizationPageUser{Subject: u.subject, Email: u.email})
		}
		p.mu.Unlock()
		sort.Slice(users, func(i, j int) bool { return users[i].Subject < users[j].Subject })
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := authorizationPage.Execute(w, authorizationPageData{
			ClientID:     r.URL.Query().Get("client_id"),
			RedirectURI:  r.URL.Query().Get("redirect_uri"),
			ResponseType: r.URL.Query().Get("response_type"),
			Scope:        r.URL.Query().Get("scope"),
			State:        r.URL.Query().Get("state"),
			Nonce:        r.URL.Query().Get("nonce"),
			Users:        users,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := validateAuthorizationRequest(r.Form); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		p.mu.Lock()
		user, ok := p.users[r.Form.Get("user")]
		if !ok {
			p.mu.Unlock()
			http.Error(w, "unknown user", http.StatusBadRequest)
			return
		}
		code, err := randomToken()
		if err == nil {
			p.codes[code] = authorizationCode{
				identity:    user,
				clientID:    r.Form.Get("client_id"),
				redirectURI: r.Form.Get("redirect_uri"),
				nonce:       r.Form.Get("nonce"),
			}
		}
		p.mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		redirectURL, err := url.Parse(r.Form.Get("redirect_uri"))
		if err != nil {
			http.Error(w, "invalid redirect_uri", http.StatusBadRequest)
			return
		}
		q := redirectURL.Query()
		q.Set("code", code)
		q.Set("state", r.Form.Get("state"))
		redirectURL.RawQuery = q.Encode()
		http.Redirect(w, r, redirectURL.String(), http.StatusFound)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func validateAuthorizationRequest(v url.Values) error {
	if v.Get("client_id") != ClientID {
		return fmt.Errorf("invalid client_id")
	}
	if v.Get("redirect_uri") == "" {
		return fmt.Errorf("missing redirect_uri")
	}
	if v.Get("response_type") != "code" {
		return fmt.Errorf("unsupported response_type")
	}
	if !contains(strings.Fields(v.Get("scope")), "openid") {
		return fmt.Errorf("openid scope is required")
	}
	return nil
}

func (p *Provider) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		p.writeOAuthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	clientID, clientSecret, ok := r.BasicAuth()
	if !ok {
		clientID, clientSecret = r.Form.Get("client_id"), r.Form.Get("client_secret")
	}
	if clientID != ClientID || clientSecret != ClientSecret {
		p.writeOAuthError(w, http.StatusUnauthorized, "invalid_client")
		return
	}

	switch r.Form.Get("grant_type") {
	case "authorization_code":
		p.exchangeAuthorizationCode(w, r.Form, clientID)
	case "refresh_token":
		p.exchangeRefreshToken(w, r.Form.Get("refresh_token"))
	default:
		p.writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type")
	}
}

func (p *Provider) exchangeAuthorizationCode(w http.ResponseWriter, form url.Values, clientID string) {
	p.mu.Lock()
	grant, ok := p.codes[form.Get("code")]
	if ok && grant.clientID == clientID && grant.redirectURI == form.Get("redirect_uri") {
		delete(p.codes, form.Get("code"))
	} else {
		ok = false
	}
	p.mu.Unlock()
	if !ok {
		p.writeOAuthError(w, http.StatusBadRequest, "invalid_grant")
		return
	}

	refreshToken, err := randomToken()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p.mu.Lock()
	p.refreshTokens[refreshToken] = grant.identity
	p.mu.Unlock()
	p.issueTokens(w, grant.identity, refreshToken, grant.nonce)
}

func (p *Provider) exchangeRefreshToken(w http.ResponseWriter, refreshToken string) {
	p.mu.Lock()
	user, ok := p.refreshTokens[refreshToken]
	p.mu.Unlock()
	if !ok {
		p.writeOAuthError(w, http.StatusBadRequest, "invalid_grant")
		return
	}
	p.issueTokens(w, user, refreshToken, "")
}

func (p *Provider) issueTokens(w http.ResponseWriter, user identity, refreshToken, nonce string) {
	accessToken, err := randomToken()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	now := time.Now()
	claims := map[string]any{
		"iss":            p.issuerURL,
		"sub":            user.subject,
		"aud":            ClientID,
		"iat":            now.Unix(),
		"exp":            now.Add(time.Hour).Unix(),
		"email":          user.email,
		"email_verified": true,
	}
	if nonce != "" {
		claims["nonce"] = nonce
	}
	idToken, err := p.signJWT(claims)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p.writeJSON(w, map[string]any{
		"access_token":  accessToken,
		"token_type":    "Bearer",
		"refresh_token": refreshToken,
		"expires_in":    3600,
		"id_token":      idToken,
	})
}

func (p *Provider) signJWT(claims map[string]any) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	unsigned := encodedHeader + "." + encodedPayload
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (p *Provider) writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

func (p *Provider) writeOAuthError(w http.ResponseWriter, statusCode int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
