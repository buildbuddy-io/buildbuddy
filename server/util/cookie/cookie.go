package cookie

import (
	"flag"
	"net/http"
	"time"
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
