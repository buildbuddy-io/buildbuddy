package static

import (
	"context"
	"flag"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
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

func FSFromRelPath(relPath string) (fs.FS, error) {
	// Figure out where our runfiles (static content bundled with the binary) live.
	rfp, err := bazel.RunfilesPath()
	if err != nil {
		return nil, err
	}
	dirFS := os.DirFS(filepath.Join(rfp, relPath))
	return dirFS, nil
}

// StaticFileServer implements a static file http server that serves static
// files out of the runfiles bundled with this application.
type StaticFileServer struct {
	handler http.Handler
}

// NewStaticFileServer returns a new static file server that will serve the
// content in relpath, optionally stripping the prefix.
func NewStaticFileServer(env environment.Env, fs fs.FS, rootPaths []string, appBundleHash string) (*StaticFileServer, error) {
	// Handle "/static/*" requests by serving those static files out of the bundled runfiles.
	handler := http.FileServer(http.FS(fs))
	if len(rootPaths) > 0 {
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

		handler = handleRootPaths(env, rootPaths, template, version.AppVersion(), jsPath, handler)
	}
	return &StaticFileServer{
		handler: handler,
	}, nil
}

// ServeHTTP implements the HTTP HandlerFunc interface.
func (s *StaticFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func handleRootPaths(env environment.Env, rootPaths []string, template *template.Template, version string, jsPath string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, rootPath := range rootPaths {
			if strings.HasPrefix(r.URL.Path, rootPath) {
				r.URL.Path = "/"
			}
		}

		if r.URL.Path == "/" {
			serveIndexTemplate(env, template, version, jsPath, w)
			return
		}

		h.ServeHTTP(w, r)
	})
}

func serveIndexTemplate(env environment.Env, tpl *template.Template, version string, jsPath string, w http.ResponseWriter) {
	issuers := make([]string, 0)
	ssoEnabled := false
	// Assemble a slice of the supported issuers. Omit "private" issuers, which have a slug,
	// and set ssoEnabled = true if any private issuers are present in the config.
	for _, provider := range env.GetConfigurator().GetAuthOauthProviders() {
		if provider.Slug == "" {
			issuers = append(issuers, provider.IssuerURL)
		} else {
			ssoEnabled = true
		}
	}

	userOwnedExecutorsEnabled := false
	executorKeyCreationEnabled := false
	workflowsEnabled := false
	if reConf := env.GetConfigurator().GetRemoteExecutionConfig(); reConf != nil {
		userOwnedExecutorsEnabled = reConf.EnableUserOwnedExecutors
		executorKeyCreationEnabled = reConf.EnableExecutorKeyCreation
		workflowsEnabled = reConf.EnableWorkflows
	}

	config := cfgpb.FrontendConfig{
		Version:                    version,
		ConfiguredIssuers:          issuers,
		DefaultToDenseMode:         env.GetConfigurator().GetDefaultToDenseMode(),
		GithubEnabled:              env.GetConfigurator().GetGithubConfig() != nil,
		AnonymousUsageEnabled:      env.GetConfigurator().GetAnonymousUsageEnabled(),
		TestDashboardEnabled:       env.GetConfigurator().EnableTargetTracking(),
		UserOwnedExecutorsEnabled:  userOwnedExecutorsEnabled,
		ExecutorKeyCreationEnabled: executorKeyCreationEnabled,
		WorkflowsEnabled:           workflowsEnabled,
		CodeEditorEnabled:          env.GetConfigurator().GetCodeEditorEnabled(),
		ChunkedEventLogsEnabled:    env.GetConfigurator().GetStorageEnableChunkedEventLogs(),
		RemoteExecutionEnabled:     env.GetConfigurator().GetRemoteExecutionConfig() != nil,
		SsoEnabled:                 ssoEnabled,
	}
	err := tpl.ExecuteTemplate(w, indexTemplateFilename, &cfgpb.FrontendTemplateData{
		Config:           &config,
		JsEntryPointPath: jsPath,
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
