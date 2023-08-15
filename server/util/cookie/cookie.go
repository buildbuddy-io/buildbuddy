package cookie

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
)

const (
	// The name of the auth cookies used to authenticate the
	// client.
	JWTCookie             = "Authorization"
	SessionIDCookie       = "Session-ID"
	sessionDurationCookie = "Session-Duration-Seconds"
	AuthIssuerCookie      = "Authorization-Issuer"
	RedirCookie           = "Redirect-Url"

	loginCookieDuration = 365 * 24 * time.Hour
)

var (
	httpsOnlyCookies = flag.Bool("auth.https_only_cookies", false, "If true, cookies will only be set over https connections.")
)

func cookieDomain() string {
	domain := ""
	if subdomain.Enabled() {
		domain = build_buddy_url.Domain()
	}
	return domain
}

func cookie(name, value, domain string, expiry time.Time, httpOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Domain:   domain,
		Expires:  expiry,
		HttpOnly: httpOnly,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
		Secure:   *httpsOnlyCookies,
	}
}

func SetCookie(w http.ResponseWriter, name, value string, expiry time.Time, httpOnly bool) {
	cd := cookieDomain()
	// If we're setting the domain on the cookie, clear out any existing cookie
	// that didn't have the domain set.
	if value != "" && cd != "" {
		clearCookie(w, name, "" /*=domain*/)
	}
	http.SetCookie(w, cookie(name, value, cd, expiry, httpOnly))
}

func clearCookie(w http.ResponseWriter, name, domain string) {
	http.SetCookie(w, cookie(name, "", domain, time.Now(), true /*=httpOnly*/))
}

func ClearCookie(w http.ResponseWriter, name string) {
	clearCookie(w, name, cookieDomain())
}

func GetCookie(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}

func SetLoginCookie(w http.ResponseWriter, jwt, issuer, sessionID string, sessionExpireTime int64) {
	expiry := time.Now().Add(loginCookieDuration)
	SetCookie(w, JWTCookie, jwt, expiry, true /* httpOnly= */)
	SetCookie(w, AuthIssuerCookie, issuer, expiry, true /* httpOnly= */)
	SetCookie(w, SessionIDCookie, sessionID, expiry, true /* httpOnly= */)
	// Don't make the session duration cookie httpOnly so the front end knows how frequently it needs to refresh tokens.
	SetCookie(w, sessionDurationCookie, fmt.Sprintf("%d", sessionExpireTime-time.Now().Unix()), expiry, false /* httpOnly= */)
}

func ClearLoginCookie(w http.ResponseWriter) {
	ClearCookie(w, JWTCookie)
	ClearCookie(w, AuthIssuerCookie)
	ClearCookie(w, SessionIDCookie)
}
