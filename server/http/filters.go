package filters

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

func printHeaders(r *http.Request) {
	// Loop through headers
	headers := make([]string, 0)
	for name, hkv := range r.Header {
		name = strings.ToLower(name)
		for _, h := range hkv {
			headers = append(headers, fmt.Sprintf("%v: %v", name, h))
		}
	}
	sort.Strings(headers)
	log.Printf("\n\n" + strings.Join(headers, "\n") + "\n\n")
}

func RedirectHTTPS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//printHeaders(r)
		protocol := r.Header.Get("X-Forwarded-Proto") // Set by load balancer
		if protocol == "http" {
			http.Redirect(w, r, "https://"+r.Host+r.URL.String(), http.StatusMovedPermanently)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// gzip, courtesy of https://gist.github.com/CJEnright/bc2d8b8dc0c1389a9feeddb110f822d7
var gzPool = sync.Pool{
	New: func() interface{} {
		w := gzip.NewWriter(ioutil.Discard)
		return w
	},
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func Gzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		// Firefox will not decompress the response if the below header
		// is not also set.
		w.Header().Set("Transfer-Encoding", "chunked")

		gz := gzPool.Get().(*gzip.Writer)
		defer gzPool.Put(gz)

		gz.Reset(w)
		defer gz.Close()

		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
}

func Authenticate(env environment.Env, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := env.GetAuthenticator().AuthenticateRequest(w, r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type wrapFn func(http.Handler) http.Handler

func WrapExternalHandler(next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	wrapFns := []wrapFn{
		Gzip,
		RedirectHTTPS,
	}
	handler := next
	for _, fn := range wrapFns {
		handler = fn(handler)
	}
	return handler
}

func WrapAuthenticatedExternalHandler(env environment.Env, next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	wrapFns := []wrapFn{
		Gzip,
		func(h http.Handler) http.Handler { return Authenticate(env, h) },
		RedirectHTTPS,
	}
	handler := next
	for _, fn := range wrapFns {
		handler = fn(handler)
	}
	return handler
}
