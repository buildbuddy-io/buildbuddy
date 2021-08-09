package index

import (
	"context"
	"flag"
	"html/template"
	"io/fs"
	"net/http"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	cfgpb "github.com/buildbuddy-io/buildbuddy/proto/config"
)

const (
	indexTemplateFilename = "index.html"
)

var (
	jsEntryPointPath = flag.String("js_entry_point_path", "/app/app_bundle/app.js?hash={APP_BUNDLE_HASH}", "Absolute URL path of the app JS entry point")
	disableGA        = flag.Bool("disable_ga", false, "If true; ga will be disabled")
)

// StaticFileServer implements a static file http server that serves static
// files out of the runfiles bundled with this application.
type IndexFileServer struct {
	env       environment.Env
	handler   http.Handler
	template  *template.Template
	jsPath    string
	version   string
	rootPaths []string
}

// NewStaticFileServer returns a new static file server that will serve the
// content in relpath, optionally stripping the prefix.
func NewIndexFileServer(env environment.Env, fs fs.FS, rootPaths []string, appBundleHash string) (*IndexFileServer, error) {
	// Handle "/static/*" requests by serving those static files out of the bundled runfiles.
	handler := http.FileServer(http.FS(fs))
	template, err := template.ParseFS(fs, indexTemplateFilename)
	if err != nil {
		return nil, err
	}

	jsPath := *jsEntryPointPath
	if appBundleHash != "" {
		jsPath = strings.ReplaceAll(jsPath, "{APP_BUNDLE_HASH}", appBundleHash)
	}

	if strings.HasPrefix(jsPath, "http://") || strings.HasPrefix(jsPath, "https://") {
		env.GetHealthChecker().AddHealthCheck("app_static_file_server", &healthChecker{jsPath: jsPath})
	}

	return &IndexFileServer{
		env:       env,
		handler:   handler,
		rootPaths: rootPaths,
		template:  template,
		version:   version.AppVersion(),
		jsPath:    jsPath,
	}, nil
}

// ServeHTTP implements the HTTP HandlerFunc interface.
func (s *IndexFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, rootPath := range s.rootPaths {
		if r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, rootPath) {
			s.serveIndexTemplate(w)
			return
		}
	}

	s.handler.ServeHTTP(w, r)
}

func (s *IndexFileServer) serveIndexTemplate(w http.ResponseWriter) {
	issuers := make([]string, 0)
	ssoEnabled := false
	// Assemble a slice of the supported issuers. Omit "private" issuers, which have a slug,
	// and set ssoEnabled = true if any private issuers are present in the config.
	for _, provider := range s.env.GetConfigurator().GetAuthOauthProviders() {
		if provider.Slug == "" {
			issuers = append(issuers, provider.IssuerURL)
		} else {
			ssoEnabled = true
		}
	}

	userOwnedExecutorsEnabled := false
	executorKeyCreationEnabled := false
	workflowsEnabled := false
	if reConf := s.env.GetConfigurator().GetRemoteExecutionConfig(); reConf != nil {
		userOwnedExecutorsEnabled = reConf.EnableUserOwnedExecutors
		executorKeyCreationEnabled = reConf.EnableExecutorKeyCreation
		workflowsEnabled = reConf.EnableWorkflows
	}

	config := cfgpb.FrontendConfig{
		Version:                    s.version,
		ConfiguredIssuers:          issuers,
		DefaultToDenseMode:         s.env.GetConfigurator().GetDefaultToDenseMode(),
		GithubEnabled:              s.env.GetConfigurator().GetGithubConfig() != nil,
		AnonymousUsageEnabled:      s.env.GetConfigurator().GetAnonymousUsageEnabled(),
		TestDashboardEnabled:       s.env.GetConfigurator().EnableTargetTracking(),
		UserOwnedExecutorsEnabled:  userOwnedExecutorsEnabled,
		ExecutorKeyCreationEnabled: executorKeyCreationEnabled,
		WorkflowsEnabled:           workflowsEnabled,
		CodeEditorEnabled:          s.env.GetConfigurator().GetCodeEditorEnabled(),
		RemoteExecutionEnabled:     s.env.GetConfigurator().GetRemoteExecutionConfig() != nil,
		SsoEnabled:                 ssoEnabled,
		GlobalFilterEnabled:        s.env.GetConfigurator().GetAppGlobalFilterEnabled(),
	}
	err := s.template.ExecuteTemplate(w, indexTemplateFilename, &cfgpb.FrontendTemplateData{
		Config:           &config,
		JsEntryPointPath: s.jsPath,
		GaEnabled:        !*disableGA,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func AppBundleHash(bundleFS fs.FS) (string, error) {
	hashBytes, err := fs.ReadFile(bundleFS, "sha.sum")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(hashBytes)), nil
}

type healthChecker struct {
	jsPath string
}

func (c *healthChecker) Check(ctx context.Context) error {
	resp, err := http.Get(c.jsPath)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return status.UnavailableErrorf("Failed to fetch static app js content from url %s. HTTP error code: %s", c.jsPath, resp.Status)
	}
	return err
}
