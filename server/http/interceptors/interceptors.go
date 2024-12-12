package interceptors

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/capabilities_filter"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/features"
	"github.com/buildbuddy-io/buildbuddy/server/http/csp"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/region"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/prometheus/client_golang/prometheus"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

var (
	upgradeInsecure  = flag.Bool("ssl.upgrade_insecure", false, "True if http requests should be redirected to https. Assumes http traffic is served on port 80 and https traffic is served on port 443 (typically via an ingress / load balancer).")
	strictCspEnabled = flag.Bool("app.strict_csp_enabled", false, "If set, set a strict CSP header. Violations are logged at warning level.")
)

const contentSecurityPolicyReportingEndpointName = "csp-endpoint"

func getContentSecurityPolicyHeaderValue(nonce string) string {
	var regionConnectSrcs []string
	for _, r := range region.Protos() {
		regionConnectSrcs = append(regionConnectSrcs, r.Subdomains)
	}
	nonceSrc := fmt.Sprintf("'nonce-%s'", nonce)
	var styleNonceSrc string
	var workerSrcs string
	// Allow inline styles to support the Monaco code editor.
	// https://github.com/microsoft/monaco-editor/issues/271
	if *features.CodeEditorEnabled || *features.CodeEditorV2Enabled {
		// unsafe-inline takes effect.
		styleNonceSrc = ""
		// Set via MonacoEnvironment.getWorkerUrl.
		workerSrcs = "data:"
	} else {
		// A nonce source automatically overrides unsafe-inline.
		styleNonceSrc = nonceSrc
		workerSrcs = "'none'"
	}
	return strings.Join([]string{
		"default-src 'self'",
		// Monaco editor dynamically loads fonts from its CDN.
		"font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/",
		// We directly embed profile images from Google accounts and don't control their URLs.
		"img-src 'self' https: data:",
		"form-action 'self'",
		"frame-src 'none'",
		"worker-src " + workerSrcs,
		"frame-ancestors 'none'",
		"base-uri 'none'",
		"block-all-mixed-content",
		// libsodium.js requires data: for wasm.
		"connect-src 'self' data: https://*.google-analytics.com https://*.analytics.google.com https://*.googletagmanager.com https://registry.build " + strings.Join(regionConnectSrcs, " "),
		"report-to " + contentSecurityPolicyReportingEndpointName,
		"report-uri " + csp.ReportingEndpoint,
		// libsodium.js requires 'wasm-unsafe-eval' to avoid a fallback to asm.js.
		fmt.Sprintf("script-src %s 'strict-dynamic' 'wasm-unsafe-eval' 'self' https: 'unsafe-inline'", nonceSrc),
		fmt.Sprintf("style-src %s 'self' https://fonts.googleapis.com/css 'unsafe-inline'", styleNonceSrc),
	}, ";")
}

func setContentSecurityPolicy(h http.Header) string {
	nonceBytes := make([]byte, 16)
	_, err := rand.Read(nonceBytes)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate nonce: %s", err))
	}
	nonce := base64.StdEncoding.EncodeToString(nonceBytes)
	h.Set("Content-Security-Policy", getContentSecurityPolicyHeaderValue(nonce))
	return nonce
}

func SetSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "deny")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		if *strictCspEnabled {
			nonce := setContentSecurityPolicy(w.Header())
			w.Header().Set("Reporting-Endpoints", fmt.Sprintf("%s=%q", contentSecurityPolicyReportingEndpointName, csp.ReportingEndpoint))
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), csp.Nonce{}, nonce)))
		} else {
			next.ServeHTTP(w, r)
		}
	})
}

func RedirectIfNotForwardedHTTPS(next http.Handler) http.Handler {
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

// A wrapped http.ResponseWriter that first bounces the write through a
// compressor or decompressor (thereby changing the file length).  This
// deletes Content-length headers so that there isn't a mismatch due to
// file compression.
type wrappedCompressionResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *wrappedCompressionResponseWriter) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
}

func (w *wrappedCompressionResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w *wrappedCompressionResponseWriter) Flush() {
	if f, ok := w.Writer.(flusherWithError); ok {
		f.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type flusherWithError interface {
	Flush() error
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

		// If the client is telling us that the stored payload is already
		// gzipped, make sure we don't double-gzip. Note that we still set the
		// gzip headers above so that the browser can automatically decompress
		// the response.
		if r.Header.Get("X-Stored-Encoding-Hint") == "gzip" {
			next.ServeHTTP(w, r)
			return
		}

		gz := gzPool.Get().(*gzip.Writer)
		defer gzPool.Put(gz)

		gz.Reset(w)
		defer gz.Close()

		next.ServeHTTP(&wrappedCompressionResponseWriter{ResponseWriter: w, Writer: gz}, r)
	})
}

func Zstd(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The client is telling us that the stored file is compressed with
		// zstd.  Browser support for zstd is experimental-only, so we
		// decompress the file for the client.
		if r.Header.Get("X-Stored-Encoding-Hint") != "zstd" {
			next.ServeHTTP(w, r)
			return
		}

		// Note that unlike the gzip code above, zstd decompressors are pooled
		// under the hood of this function call.
		zstd, err := compression.NewZstdDecompressor(w)
		if err != nil {
			http.Error(w, "Failed to initialize decompressor", http.StatusInternalServerError)
			return
		}
		defer zstd.Close()

		next.ServeHTTP(&wrappedCompressionResponseWriter{ResponseWriter: w, Writer: zstd}, r)
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
		if err := capabilities_filter.AuthorizeRPC(ctx, env, r.URL.Path); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func AuthorizeIP(env environment.Env, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if irs := env.GetIPRulesService(); irs != nil {
			if err := irs.AuthorizeHTTPRequest(r.Context(), r); err != nil {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func ClientIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Forwarded-For"); v != "" {
			ctx, ok := clientip.SetFromXForwardedForHeader(r.Context(), v)
			if ok {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		clientIP := r.RemoteAddr
		if ip, _, err := net.SplitHostPort(clientIP); err == nil {
			clientIP = ip
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientip.ContextKey, clientIP)))
	})
}

func Subdomain(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(subdomain.SetHost(r.Context(), r.Host)))
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

func (w *instrumentedResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func alertOnPanic(err any) {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	alert.UnexpectedEvent("recovered_panic", "%v\n%s", err, buf[:n])
}

func RecoverAndAlert(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				http.Error(w, "A panic occurred", http.StatusInternalServerError)
				alertOnPanic(panicErr)
			}
		}()
		next.ServeHTTP(w, r)
	})
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
		log.LogHTTPRequest(r.Context(), r.Method, r.URL.Path, duration, irw.statusCode)
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
	if path == "/api/v1/metrics" {
		return "/api/v1/metrics"
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
		func(h http.Handler) http.Handler { return AuthorizeIP(env, h) },
		func(h http.Handler) http.Handler { return Authenticate(env, h) },
		// The request message is parsed before authentication since the request_context
		// field needs to be authenticated if it's present.
		func(h http.Handler) http.Handler { return handlers.BodyParserMiddleware(h) },
		func(h http.Handler) http.Handler { return SetSecurityHeaders(h) },
		LogRequest,
		RequestID,
		ClientIP,
		Subdomain,
		region.CORS,
		RecoverAndAlert,
	})
}

func WrapExternalHandler(env environment.Env, next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	return wrapHandler(env, next, &[]wrapFn{
		Zstd,
		Gzip,
		func(h http.Handler) http.Handler { return SetSecurityHeaders(h) },
		LogRequest,
		RequestID,
		ClientIP,
		Subdomain,
		RecoverAndAlert,
	})
}

func WrapAuthenticatedExternalHandler(env environment.Env, next http.Handler) http.Handler {
	// NB: These are called in reverse order, so the 0th element will be
	// called last before the handler itself is called.
	return wrapHandler(env, next, &[]wrapFn{
		Zstd,
		Gzip,
		func(h http.Handler) http.Handler { return AuthorizeIP(env, h) },
		func(h http.Handler) http.Handler { return Authenticate(env, h) },
		RequestContextFromURL,
		func(h http.Handler) http.Handler { return SetSecurityHeaders(h) },
		LogRequest,
		RequestID,
		ClientIP,
		Subdomain,
		RecoverAndAlert,
	})
}

type RedirectOnError func(http.ResponseWriter, *http.Request) error

func (f RedirectOnError) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := f(w, r)
	if err != nil {
		log.Warning(err.Error())
		http.Redirect(w, r, "/?error="+url.QueryEscape(err.Error()), http.StatusTemporaryRedirect)
	}
}
