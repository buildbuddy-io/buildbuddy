package static

import (
	"html/template"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"

	cfgpb "proto/config"

	"github.com/buildbuddy-io/buildbuddy/server/environment"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
)

// StaticFileServer implements a static file http server that serves static
// files out of the runfiles bundled with this application.
type StaticFileServer struct {
	handler http.Handler
}

var indexTemplateFilename = "index.html"
var versionFilename = "VERSION"

// NewStaticFileServer returns a new static file server that will serve the
// content in relpath, optionally stripping the prefix.
func NewStaticFileServer(env environment.Env, relPath string, rootPaths []string) (*StaticFileServer, error) {
	// Figure out where our runfiles (static content bundled with the binary) live.
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		return nil, err
	}
	// Handle "/static/*" requests by serving those static files out of the bundled runfiles.
	pkgStaticDir := filepath.Join(rfp, relPath)
	handler := http.FileServer(http.Dir(pkgStaticDir))
	if len(rootPaths) > 0 {
		template, err := template.ParseFiles(filepath.Join(pkgStaticDir, indexTemplateFilename))
		if err != nil {
			return nil, err
		}
		versionBytes, err := ioutil.ReadFile(filepath.Join(rfp, versionFilename))
		if err != nil {
			return nil, err
		}

		handler = handleRootPaths(env, relPath, rootPaths, template, string(versionBytes), handler)
	}
	return &StaticFileServer{
		handler: handler,
	}, nil
}

// ServeHTTP implements the HTTP HandlerFunc interface.
func (s *StaticFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func handleRootPaths(env environment.Env, relPath string, rootPaths []string, template *template.Template, version string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, rootPath := range rootPaths {
			if strings.HasPrefix(r.URL.Path, rootPath) {
				r.URL.Path = "/"
			}
		}

		if r.URL.Path == "/" {
			serveIndexTemplate(env, template, version, w)
			return
		}

		h.ServeHTTP(w, r)
	})
}

func serveIndexTemplate(env environment.Env, template *template.Template, version string, w http.ResponseWriter) {
	issuers := make([]string, 0)
	for _, provider := range env.GetConfigurator().GetAuthOauthProviders() {
		issuers = append(issuers, provider.IssuerURL)
	}
	err := template.ExecuteTemplate(w, indexTemplateFilename, &cfgpb.FrontendConfig{
		Version:           version,
		ConfiguredIssuers: issuers,
		AssistanceEnabled: env.GetConfigurator().GetIntegrationsSlackConfig().HelpWebhookURL != "",
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
