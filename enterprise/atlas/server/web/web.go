// Package web serves the atlas UI resources.
package web

import (
	_ "embed"
	"html/template"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/csp"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	atlaspb "github.com/buildbuddy-io/buildbuddy/proto/atlas"
)

//go:embed index.html
var indexHTML string

const (
	rpcPrefix      = "/rpc/AtlasService/"
	serviceName    = "atlas.AtlasService"
	bundleHashFile = "sha.sum"
)

type Options struct {
	// AppFS holds the built frontend: app_bundle/, style.css and sha.sum.
	AppFS fs.FS
	// Service answers the RPCs.
	Service atlaspb.AtlasServiceServer
	// GRPCServer has Service registered on it; streaming RPCs over HTTP are
	// routed through it.
	GRPCServer *grpc.Server
	// ClusterName is the cluster this instance indexes.
	ClusterName string
	// ClusterLinks lists every instance, this one included, for the UI's
	// cluster picker.
	ClusterLinks []*atlaspb.ClusterLink
}

type templateData struct {
	StylePath        string
	JsEntryPointPath string
	// Config is the FrontendConfig proto as JSON.
	Config template.JS
	// Nonce is the Content-Security-Policy nonce, when one is in force.
	Nonce string
}

// Handler returns the root handler serving the UI and its API.
func Handler(env environment.Env, opts Options) (http.Handler, error) {
	hashBytes, err := fs.ReadFile(opts.AppFS, bundleHashFile)
	if err != nil {
		return nil, status.InternalErrorf("reading the app bundle hash: %s", err)
	}
	bundleHash := strings.TrimSpace(string(hashBytes))
	tpl, err := template.New("index").Parse(indexHTML)
	if err != nil {
		return nil, status.InternalErrorf("parsing the index template: %s", err)
	}
	rpc, err := protolet.GenerateHTTPHandlers(rpcPrefix, serviceName, opts.Service, opts.GRPCServer)
	if err != nil {
		return nil, status.InternalErrorf("generating RPC handlers: %s", err)
	}

	configJSON, err := protojson.Marshal(&atlaspb.FrontendConfig{
		AppBundleHash: bundleHash,
		Version:       version.Tag(),
		ClusterName:   opts.ClusterName,
		ClusterLinks:  opts.ClusterLinks,
	})
	if err != nil {
		return nil, err
	}
	data := templateData{
		StylePath:        "/app/style.css?hash=" + bundleHash,
		JsEntryPointPath: "/app/app_bundle/app.js?hash=" + bundleHash,
		Config:           template.JS(configJSON),
	}

	mux := http.NewServeMux()
	mux.Handle(rpcPrefix, chain(rpc.BodyParserMiddleware(rpc.RequestHandler),
		interceptors.Gzip,
		interceptors.SetSecurityHeaders,
		interceptors.LogRequest,
		interceptors.RequestID,
		interceptors.ClientIP,
		interceptors.RecoverAndAlert,
	))
	mux.Handle("/app/", interceptors.WrapExternalHandler(env,
		http.StripPrefix("/app", cacheByHash(assetServer(opts.AppFS), bundleHash))))
	mux.Handle("/", interceptors.WrapExternalHandler(env, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Serve the static UI resources and let the UI handle the routes client
		// side, as necessary. The only exception are things that look like
		// files (e.g. favicon.ico) in which case we serve a 404. /object/ is an
		// exception because resource names may contain dots.

		if path.Ext(r.URL.Path) != "" && !strings.HasPrefix(r.URL.Path, "/object/") {
			http.NotFound(w, r)
			return
		}
		d := data
		d.Nonce, _ = r.Context().Value(csp.Nonce{}).(string)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tpl.Execute(w, &d); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})))
	return mux, nil
}

// chain wraps h in the given interceptors, executing in the passed-in order.
func chain(h http.Handler, wrappers ...func(http.Handler) http.Handler) http.Handler {
	for _, wrapper := range wrappers {
		h = wrapper(h)
	}
	return h
}

// assetServer serves files from the bundle, without directory listings.
func assetServer(appFS fs.FS) http.Handler {
	files := http.FileServerFS(appFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		files.ServeHTTP(w, r)
	})
}

// cacheByHash lets assets requested with the current bundle hash be cached
// forever, since a new bundle comes with a new hash.
func cacheByHash(h http.Handler, bundleHash string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch hash := r.URL.Query().Get("hash"); {
		case hash == bundleHash:
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		case hash != "":
			w.Header().Set("Cache-Control", "no-cache")
		}
		h.ServeHTTP(w, r)
	})
}
