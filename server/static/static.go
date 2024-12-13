package static

import (
	"context"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/features"
	"github.com/buildbuddy-io/buildbuddy/server/http/csp"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/region"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/protobuf/encoding/protojson"

	cfgpb "github.com/buildbuddy-io/buildbuddy/proto/config"
	iss_config "github.com/buildbuddy-io/buildbuddy/server/invocation_stat_service/config"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/server/remote_execution/config"
	scheduler_server_config "github.com/buildbuddy-io/buildbuddy/server/scheduling/scheduler_server/config"
)

const (
	indexTemplateFilename = "index.html"
	stylePathTemplate     = "/app/style.css?hash={APP_BUNDLE_HASH}"
)

// TODO: Move these flags to the features package to allow other packages to
// access them without creating dependency cycles.
var (
	defaultToDenseMode                     = flag.Bool("app.default_to_dense_mode", false, "Enables the dense UI mode by default.")
	userManagementEnabled                  = flag.Bool("app.user_management_enabled", true, "If set, the user management page will be enabled in the UI.", flag.Deprecated("This flag has no effect and will be removed in the future."))
	testGridV2Enabled                      = flag.Bool("app.test_grid_v2_enabled", true, "Whether to enable test grid V2")
	usageEnabled                           = flag.Bool("app.usage_enabled", false, "If set, the usage page will be enabled in the UI.")
	expandedSuggestionsEnabled             = flag.Bool("app.expanded_suggestions_enabled", false, "If set, enable more build suggestions in the UI.")
	enableWorkflows                        = flag.Bool("remote_execution.enable_workflows", false, "Whether to enable BuildBuddy workflows.")
	enableExecutorKeyCreation              = flag.Bool("remote_execution.enable_executor_key_creation", false, "If enabled, UI will allow executor keys to be created.")
	testOutputManifestsEnabled             = flag.Bool("app.test_output_manifests_enabled", true, "If set, the target page will render the contents of test output zips.")
	patternFilterEnabled                   = flag.Bool("app.pattern_filter_enabled", true, "If set, allow filtering by pattern in the client.")
	executionSearchEnabled                 = flag.Bool("app.execution_search_enabled", true, "If set, fetch lists of executions from the OLAP DB in the trends UI.")
	trendsSummaryEnabled                   = flag.Bool("app.trends_summary_enabled", false, "If set, show the new 'summary' section at the top of the trends UI.")
	customerManagedEncryptionKeysEnabled   = flag.Bool("app.customer_managed_encryption_keys_enabled", false, "If set, show customer-managed encryption configuration UI.")
	tagsUIEnabled                          = flag.Bool("app.tags_ui_enabled", false, "If set, expose tags data and let users filter by tag.")
	timeseriesChartsInTimingProfileEnabled = flag.Bool("app.timeseries_charts_in_timing_profile_enabled", true, "If set, charts with sampled time series data (such as CPU and memory usage) will be shown")
	auditLogsUIEnabled                     = flag.Bool("app.audit_logs_ui_enabled", false, "If set, the audit logs UI will be accessible from the sidebar.")
	newTrendsUIEnabled                     = flag.Bool("app.new_trends_ui_enabled", false, "DEPRECATED: If set, show a new trends UI with a bit more organization.")
	trendsRangeSelectionEnabled            = flag.Bool("app.trends_range_selection", true, "If set, let users drag to select time ranges in the trends UI.")
	ipRulesUIEnabled                       = flag.Bool("app.ip_rules_ui_enabled", false, "If set, show the IP rules tab in settings page.")
	traceViewerEnabled                     = flag.Bool("app.trace_viewer_enabled", false, "Whether the new trace viewer is enabled.")
	popupAuthEnabled                       = flag.Bool("app.popup_auth_enabled", false, "Whether popup windows should be used for authentication.")
	streamingHTTPEnabled                   = flag.Bool("app.streaming_http_enabled", false, "Whether to support server-streaming http requests between server and web UI.")
	codeReviewEnabled                      = flag.Bool("app.code_review_enabled", false, "If set, show the code review UI.")
	codeSearchEnabled                      = flag.Bool("app.codesearch_enabled", false, "If set, show the code search UI.")
	orgAdminApiKeyCreationEnabled          = flag.Bool("app.org_admin_api_key_creation_enabled", false, "If set, SCIM API keys will be able to be created in the UI.")
	readerWriterRolesEnabled               = flag.Bool("app.reader_writer_roles_enabled", true, "If set, Reader/Writer roles will be enabled in the user management UI.")
	invocationLogStreamingEnabled          = flag.Bool("app.invocation_log_streaming_enabled", false, "If set, the UI will stream invocation logs instead of polling.")
	targetFlakesUIEnabled                  = flag.Bool("app.target_flakes_ui_enabled", false, "If set, show some fancy new features for analyzing flakes.")
	bazelButtonsEnabled                    = flag.Bool("app.bazel_buttons_enabled", false, "If set, show remote bazel buttons in the UI.")
	communityLinksEnabled                  = flag.Bool("app.community_links_enabled", true, "If set, show links to BuildBuddy community in the UI.")
	defaultLoginSlug                       = flag.String("app.default_login_slug", "", "If set, the login page will default to using this slug.")

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

		jsPath := strings.ReplaceAll(*jsEntryPointPath, "{APP_BUNDLE_HASH}", appBundleHash)
		stylePath := strings.ReplaceAll(stylePathTemplate, "{APP_BUNDLE_HASH}", appBundleHash)

		if strings.HasPrefix(jsPath, "http://") || strings.HasPrefix(jsPath, "https://") {
			env.GetHealthChecker().AddHealthCheck("app_static_file_server", &healthChecker{jsPath: jsPath})
		}

		handler = handleRootPaths(env, rootPaths, template, version.Tag(), jsPath, stylePath, appBundleHash, handler)
	}
	return &StaticFileServer{
		handler: setCacheHeaders(handler),
	}, nil
}

// ServeHTTP implements the HTTP HandlerFunc interface.
func (s *StaticFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func handleRootPaths(env environment.Env, rootPaths []string, template *template.Template, version, jsPath, stylePath, appBundleHash string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, rootPath := range rootPaths {
			if strings.HasPrefix(r.URL.Path, rootPath) {
				r.URL.Path = "/"
			}
		}

		if r.URL.Path == "/" {
			serveIndexTemplate(r.Context(), env, template, version, jsPath, stylePath, appBundleHash, w)
			return
		}

		h.ServeHTTP(w, r)
	})
}

// Set cache headers if a static file request has a `hash` query parameter.
func setCacheHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.URL.Query().Get("hash")) != "" {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable") // 1 year
		}
		h.ServeHTTP(w, r)
	})
}

type FrontendTemplateData struct {
	// StylePath is the path to the main styles for the app.
	StylePath string
	// JsEntryPointPath is the path to the main script that bootstraps the app.
	JsEntryPointPath string
	// GaEnabled decides whether to render the Google Analytics script.
	GaEnabled bool
	// Config is the FrontendConfig proto serialized using jsonpb.
	Config template.JS
	// Nonce is the Content-Security-Policy nonce value.
	Nonce string
}

func serveIndexTemplate(ctx context.Context, env environment.Env, tpl *template.Template, version, jsPath, stylePath, appBundleHash string, w http.ResponseWriter) {
	nonce, _ := ctx.Value(csp.Nonce{}).(string)
	config := cfgpb.FrontendConfig{
		Version:                                version,
		AppBundleHash:                          appBundleHash,
		ConfiguredIssuers:                      env.GetAuthenticator().PublicIssuers(),
		DefaultToDenseMode:                     *defaultToDenseMode,
		GithubEnabled:                          github.IsLegacyOAuthAppEnabled(),
		GithubAppEnabled:                       env.GetGitHubApp() != nil,
		GithubAuthEnabled:                      github.AuthEnabled(env),
		AnonymousUsageEnabled:                  env.GetAuthenticator().AnonymousUsageEnabled(ctx),
		TestDashboardEnabled:                   target_tracker.TargetTrackingEnabled(),
		UserOwnedExecutorsEnabled:              remote_execution_config.RemoteExecutionEnabled() && scheduler_server_config.UserOwnedExecutorsEnabled(),
		ExecutorKeyCreationEnabled:             remote_execution_config.RemoteExecutionEnabled() && *enableExecutorKeyCreation,
		WorkflowsEnabled:                       remote_execution_config.RemoteExecutionEnabled() && *enableWorkflows,
		CodeEditorEnabled:                      *features.CodeEditorEnabled || *features.CodeEditorV2Enabled,
		RemoteExecutionEnabled:                 remote_execution_config.RemoteExecutionEnabled(),
		SsoEnabled:                             env.GetAuthenticator().SSOEnabled(),
		GlobalFilterEnabled:                    true,
		UsageEnabled:                           *usageEnabled,
		ForceUserOwnedDarwinExecutors:          remote_execution_config.RemoteExecutionEnabled() && scheduler_server_config.ForceUserOwnedDarwinExecutors(),
		TestGridV2Enabled:                      *testGridV2Enabled,
		DetailedCacheStatsEnabled:              hit_tracker.DetailedStatsEnabled(),
		ExpandedSuggestionsEnabled:             *expandedSuggestionsEnabled,
		QuotaManagementEnabled:                 env.GetQuotaManager() != nil,
		SecretsEnabled:                         env.GetSecretService() != nil,
		TestOutputManifestsEnabled:             *testOutputManifestsEnabled,
		UserOwnedKeysEnabled:                   env.GetAuthDB() != nil && env.GetAuthDB().GetUserOwnedKeysEnabled(),
		TrendsHeatmapEnabled:                   iss_config.TrendsHeatmapEnabled() && env.GetOLAPDBHandle() != nil,
		PatternFilterEnabled:                   *patternFilterEnabled,
		BotSuggestionsEnabled:                  env.GetSuggestionService() != nil,
		MultipleSuggestionProviders:            env.GetSuggestionService() != nil && env.GetSuggestionService().MultipleProvidersConfigured(),
		ExecutionSearchEnabled:                 *executionSearchEnabled,
		TrendsSummaryEnabled:                   *trendsSummaryEnabled,
		CustomerManagedEncryptionKeysEnabled:   *customerManagedEncryptionKeysEnabled,
		TagsUiEnabled:                          *tagsUIEnabled,
		TimeseriesChartsInTimingProfileEnabled: *timeseriesChartsInTimingProfileEnabled,
		AuditLogsUiEnabled:                     *auditLogsUIEnabled,
		TrendsRangeSelectionEnabled:            *trendsRangeSelectionEnabled && env.GetOLAPDBHandle() != nil,
		SubdomainsEnabled:                      subdomain.Enabled(),
		CustomerSubdomain:                      subdomain.Get(ctx) != "",
		Domain:                                 build_buddy_url.Domain(),
		IpRulesEnabled:                         *ipRulesUIEnabled,
		Regions:                                region.Protos(),
		PopupAuthEnabled:                       *popupAuthEnabled,
		StreamingHttpEnabled:                   *streamingHTTPEnabled,
		CodeReviewEnabled:                      *codeReviewEnabled,
		CodeSearchEnabled:                      *codeSearchEnabled,
		OrgAdminApiKeyCreationEnabled:          *orgAdminApiKeyCreationEnabled,
		ReaderWriterRolesEnabled:               *readerWriterRolesEnabled,
		InvocationLogStreamingEnabled:          *invocationLogStreamingEnabled,
		TargetFlakesUiEnabled:                  *targetFlakesUIEnabled && env.GetOLAPDBHandle() != nil,
		CodeEditorV2Enabled:                    *features.CodeEditorV2Enabled,
		BazelButtonsEnabled:                    *bazelButtonsEnabled,
		CspNonce:                               nonce,
		CommunityLinksEnabled:                  *communityLinksEnabled,
		DefaultLoginSlug:                       *defaultLoginSlug,
	}

	configJSON, err := protojson.Marshal(&config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	err = tpl.ExecuteTemplate(w, indexTemplateFilename, &FrontendTemplateData{
		StylePath:        stylePath,
		JsEntryPointPath: jsPath,
		GaEnabled:        !*disableGA,
		Config:           template.JS(configJSON),
		Nonce:            nonce,
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
	jsPath                   string
	hasSuccessfullyFetchedJS bool
}

func (c *healthChecker) Check(ctx context.Context) error {
	if c.hasSuccessfullyFetchedJS {
		return nil
	}
	resp, err := http.Get(c.jsPath)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return status.UnavailableErrorf("Failed to fetch static app js content from url %s. HTTP error code: %s", c.jsPath, resp.Status)
	}
	c.hasSuccessfullyFetchedJS = true
	return nil
}
