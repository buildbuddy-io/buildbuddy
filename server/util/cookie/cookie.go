package cookie

import (
	"flag"
	"fmt"
	"net/http"
	"time"
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

func SetCookie(w http.ResponseWriter, name, value string, expiry time.Time, httpOnly bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Expires:  expiry,
		HttpOnly: httpOnly,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
		Secure:   *httpsOnlyCookies,
	})
}

func ClearCookie(w http.ResponseWriter, name string) {
	SetCookie(w, name, "", time.Now(), true /* httpOnly= */)
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
