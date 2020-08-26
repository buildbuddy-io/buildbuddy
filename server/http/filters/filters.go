package filters

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc"
)

func RedirectHTTPS(env environment.Env, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")

		protocol := r.Header.Get("X-Forwarded-Proto") // Set by load balancer
		if sslConfig := env.GetConfigurator().GetSSLConfig(); sslConfig != nil && sslConfig.EnableSSL && protocol == "http" {
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
		ctx := env.GetAuthenticator().AuthenticateHTTPRequest(w, r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := uuid.SetInContext(r.Context())
		if err != nil {
			// Should never happen, but just in case, fail safe.
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.LogHTTPRequest(r.Context(), r.URL.Path, time.Now().Sub(start), nil)
	})
}

type wrapFn func(http.Handler) http.Handler

func WrapExternalHandler(env environment.Env, next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	wrapFns := []wrapFn{
		Gzip,
		func(h http.Handler) http.Handler { return RedirectHTTPS(env, h) },
		LogRequest,
		RequestID,
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
		func(h http.Handler) http.Handler { return RedirectHTTPS(env, h) },
		LogRequest,
		RequestID,
	}
	handler := next
	for _, fn := range wrapFns {
		handler = fn(handler)
	}
	return handler
}

func ServeGRPCOverHTTPPort(grpcServer *grpc.Server, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(
			r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			next.ServeHTTP(w, r)
		}
	})
}
