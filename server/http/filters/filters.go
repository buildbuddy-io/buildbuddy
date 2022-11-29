package filters

import (
	"compress/gzip"
	"encoding/base64"
	"flag"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/role_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

var (
	upgradeInsecure = flag.Bool("ssl.upgrade_insecure", false, "True if http requests should be redirected to https")

	uuidV4Regexp = regexp.MustCompile("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
)

func SetSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		next.ServeHTTP(w, r)
	})
}

func RedirectIfNotForwardedHTTPS(env environment.Env, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol := r.Header.Get("X-Forwarded-Proto") // Set by load balancer
		// Our k8s healthchecks set "server-type" header, but Google LB healthchecks don't support them so we check the UA.
		isHealthCheck := r.Header.Get("server-type") != "" || strings.HasPrefix(r.Header.Get("User-Agent"), "GoogleHC/")
		if *upgradeInsecure && !isHealthCheck && protocol != "https" {
			http.Redirect(w, r, "https://"+r.Host+r.URL.String(), http.StatusMovedPermanently)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// gzip, courtesy of https://gist.github.com/CJEnright/bc2d8b8dc0c1389a9feeddb110f822d7
var gzPool = sync.Pool{
	New: func() interface{} {
		w := gzip.NewWriter(io.Discard)
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
		ctx := env.GetAuthenticator().AuthenticatedHTTPContext(w, r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func AuthorizeSelectedGroupRole(env environment.Env, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := role_filter.AuthorizeRPC(ctx, env, r.URL.Path); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
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

func parseRequestContext(r *http.Request) (*ctxpb.RequestContext, error) {
	q := r.URL.Query()
	if !q.Has("request_context") {
		return nil, nil
	}
	val := q.Get("request_context")
	b, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return nil, err
	}
	reqCtx := &ctxpb.RequestContext{}
	if err := proto.Unmarshal(b, reqCtx); err != nil {
		return nil, err
	}
	return reqCtx, nil
}

func RequestContextFromURL(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCtx, err := parseRequestContext(r)
		if err != nil {
			log.Warningf("Failed to parse request_context param: %s", err)
		} else if reqCtx != nil {
			ctx := requestcontext.ContextWithProtoRequestContext(r.Context(), reqCtx)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// instrumentedResponseWriter wraps http.ResponseWriter, recording info needed for
// Prometheus metrics.
type instrumentedResponseWriter struct {
	http.ResponseWriter
	statusCode        int
	responseSizeBytes int
}

func (w *instrumentedResponseWriter) Write(bytes []byte) (int, error) {
	w.responseSizeBytes += len(bytes)
	return w.ResponseWriter.Write(bytes)
}
func (w *instrumentedResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m := method(r)
		rt := routeLabel(r)
		recordRequestMetrics(rt, m)
		irw := &instrumentedResponseWriter{
			ResponseWriter: w,
			// net/http defaults to 200 if not written.
			statusCode: 200,
		}
		next.ServeHTTP(irw, r)
		duration := time.Since(start)
		log.LogHTTPRequest(r.Context(), r.URL.Path, duration, irw.statusCode)
		recordResponseMetrics(rt, m, irw.statusCode, irw.responseSizeBytes, duration)
	})
}

func routeLabel(r *http.Request) string {
	// Note: This function intentionally returns a small, fixed set of static
	// string constants, to avoid excessive metric cardinality.
	path := r.URL.Path
	if !utf8.ValidString(path) {
		return "[INVALID]"
	}
	if path == "" || path == "/" {
		return "/"
	}
	if strings.HasPrefix(path, "/image/") {
		return "/image/[...]"
	}
	if strings.HasPrefix(path, "/favicon/") {
		return "/favicon/[...]"
	}
	if strings.HasPrefix(path, "/rpc/BuildBuddyService/") {
		return "/rpc/BuildBuddyService/[...]"
	}
	if strings.HasPrefix(path, "/invocation/") {
		return "/invocation/[...]"
	}
	return "[OTHER]"
}

func method(r *http.Request) string {
	if r.Method == "" {
		return "GET"
	}
	return r.Method
}

func recordRequestMetrics(route, method string) {
	metrics.HTTPRequestCount.With(prometheus.Labels{
		metrics.HTTPRouteLabel:  route,
		metrics.HTTPMethodLabel: method,
	}).Inc()
}

func recordResponseMetrics(route, method string, statusCode, responseSizeBytes int, duration time.Duration) {
	labels := prometheus.Labels{
		metrics.HTTPRouteLabel:        route,
		metrics.HTTPMethodLabel:       method,
		metrics.HTTPResponseCodeLabel: strconv.Itoa(statusCode),
	}
	metrics.HTTPRequestHandlerDurationUsec.With(labels).Observe(float64(duration.Microseconds()))
	metrics.HTTPResponseSizeBytes.With(labels).Observe(float64(responseSizeBytes))
}

type wrapFn func(http.Handler) http.Handler

func wrapHandler(env environment.Env, next http.Handler, wrapFns *[]wrapFn) http.Handler {
	handler := next
	for _, fn := range *wrapFns {
		handler = fn(handler)
	}
	return handler
}

func WrapAuthenticatedExternalProtoletHandler(env environment.Env, httpPrefix string, handlers *protolet.HTTPHandlers) http.Handler {
	return wrapHandler(env, handlers.RequestHandler, &[]wrapFn{
		Gzip,
		func(h http.Handler) http.Handler { return AuthorizeSelectedGroupRole(env, h) },
		func(h http.Handler) http.Handler { return Authenticate(env, h) },
		// The request message is parsed before authentication since the request_context
		// field needs to be authenticated if it's present.
		func(h http.Handler) http.Handler {
			return http.StripPrefix(httpPrefix, handlers.BodyParserMiddleware(h))
		},
		func(h http.Handler) http.Handler { return SetSecurityHeaders(h) },
		LogRequest,
		RequestID,
	})
}

func WrapExternalHandler(env environment.Env, next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	return wrapHandler(env, next, &[]wrapFn{
		Gzip,
		func(h http.Handler) http.Handler { return SetSecurityHeaders(h) },
		LogRequest,
		RequestID,
	})
}

func WrapAuthenticatedExternalHandler(env environment.Env, next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	return wrapHandler(env, next, &[]wrapFn{
		Gzip,
		func(h http.Handler) http.Handler { return Authenticate(env, h) },
		RequestContextFromURL,
		func(h http.Handler) http.Handler { return SetSecurityHeaders(h) },
		LogRequest,
		RequestID,
	})
}
