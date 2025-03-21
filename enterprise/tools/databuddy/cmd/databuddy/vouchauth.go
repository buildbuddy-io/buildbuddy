package main

import (
	"context"
	"net/http"
)

var (
	vouchUserContextKey = struct{}{}
)

func WithVouchAuth(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		email := r.Header.Get("X-Vouch-User")
		ctx := context.WithValue(r.Context(), vouchUserContextKey, email)
		r = r.WithContext(ctx)
		h.ServeHTTP(w, r)
	})
}

func VouchUser(ctx context.Context) string {
	v := ctx.Value(vouchUserContextKey)
	if v == nil {
		return ""
	}
	return v.(string)
}
