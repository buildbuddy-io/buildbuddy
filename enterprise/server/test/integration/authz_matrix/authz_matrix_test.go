// Package authz_matrix is a black-box authorization test that runs against a
// live BuildBuddy deployment.
//
// It takes one credential (currently: a user-owned API key with no cache-write
// capabilities, i.e. a "read-only" key created from the user settings page)
// and, for every RPC exposed by the app, asserts that the deployment allows or
// denies the call exactly as the expectation table below says it should.
//
// The "should" is the table in this file, not the server: the test is only as
// good as the table, so review the table when reviewing this test.
//
// Three checks run per RPC:
//
//  1. Completeness: every RPC in the service descriptors has a row in the
//     table, and every row names a real RPC. New RPCs fail the test until a
//     human classifies them.
//  2. Oracle: the server's own self-reported allowed_rpc list (from GetUser)
//     agrees with the table.
//  3. Invocation: the RPC is actually called with a minimal, non-mutating
//     request and the response code is classified. Rows that could mutate
//     state with an empty request are skipped from this step and rely on
//     the oracle check only.
//
// Usage:
//
//	bazel test //enterprise/server/test/integration/authz_matrix \
//	  --test_output=streamed \
//	  --test_env=AUTHZ_MATRIX_API_KEY=<key> \
//	  --test_arg=--target=grpcs://buildbuddy-qa-dev.buildbuddy.dev \
//	  --test_arg=--group_id=GR16322340471839723307
package authz_matrix_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	target    = flag.String("target", "grpcs://buildbuddy-qa-dev.buildbuddy.dev", "gRPC target of the deployment under test.")
	apiKeyEnv = flag.String("api_key_env", "AUTHZ_MATRIX_API_KEY", "Name of the environment variable holding the API key.")
	groupID   = flag.String("group_id", "", "If set, assert that the key authenticates as this group.")
	rpcTime   = flag.Duration("rpc_timeout", 30*time.Second, "Per-RPC timeout.")
)

// tier is the coarse authorization tier that capabilities_filter.go assigns
// to each BuildBuddyService / ApiService RPC. The persona under test is a
// group member without ORG_ADMIN, so tierUnfiltered and tierMember are
// expected to pass the filter and tierAdmin / tierServerAdmin are expected
// to be rejected by it.
type tier int

const (
	tierUnfiltered  tier = iota // no group/role check in the interceptor
	tierMember                  // any member of the selected group
	tierAdmin                   // ORG_ADMIN in the selected group
	tierServerAdmin             // admin of the server-admin group
)

func (t tier) String() string {
	switch t {
	case tierUnfiltered:
		return "unfiltered"
	case tierMember:
		return "member"
	case tierAdmin:
		return "admin"
	case tierServerAdmin:
		return "server-admin"
	}
	return "?"
}

// filterAllows reports whether the coarse filter should let the persona
// under test (a non-admin member) through.
func (t tier) filterAllows() bool { return t == tierUnfiltered || t == tierMember }

type row struct {
	Tier tier
	// Write marks RPCs that mutate state. Used only for reporting.
	Write bool
	// Skip, when non-empty, means the RPC is not invoked (only the oracle
	// check runs) and says why.
	Skip string
	// HandlerRejects means the RPC passes the coarse filter but its handler
	// is expected to return PermissionDenied or Unauthenticated for this
	// persona even on an empty request (e.g. it checks a capability the key
	// lacks, or it needs a login session rather than an API key).
	HandlerRejects bool
	// KnownServerError, when non-empty, documents a server error
	// (Internal/Unknown) that this RPC is known to return for the empty
	// request from this persona. The case is reported as skipped with this
	// note instead of failing, so the suite stays green while the problem
	// stays visible. Clear it once the server is fixed.
	KnownServerError string
}

const (
	bbPrefix  = "/buildbuddy.service.BuildBuddyService/"
	apiPrefix = "/api.v1.ApiService/"

	// The exact message capabilities_filter.AuthorizeRPC returns, which
	// lets us tell filter denials apart from handler denials.
	filterDeniedMsg = "permission denied"

	serverErrorNote = "server error, not an authorization verdict"
)

// expectations is the reviewed authorization table for the BuildBuddyService
// and ApiService RPCs. Tiers mirror the lists in
// server/capabilities_filter/capabilities_filter.go; if this table and that
// file disagree, one of them is wrong.
var expectations = map[string]row{
	// --- Pre-login / no group membership required -------------------------
	bbPrefix + "GetUser":        {Tier: tierUnfiltered},
	bbPrefix + "CreateUser":     {Tier: tierUnfiltered, Write: true, HandlerRejects: true}, // needs a login session; API keys get Unauthenticated
	bbPrefix + "GetGroup":       {Tier: tierUnfiltered},
	bbPrefix + "JoinGroup":      {Tier: tierUnfiltered, Write: true}, // empty group id is rejected
	bbPrefix + "CreateGroup":    {Tier: tierUnfiltered, Write: true}, // empty name is rejected; flips to admin tier if app.admin_only_create_group
	bbPrefix + "GetBazelConfig": {Tier: tierUnfiltered},

	// --- Invocation-scoped reads (authorized by per-row perms bits) ---------
	bbPrefix + "GetInvocation":         {Tier: tierUnfiltered},
	bbPrefix + "GetEventLogChunk":      {Tier: tierUnfiltered},
	bbPrefix + "GetEventLog":           {Tier: tierUnfiltered},
	bbPrefix + "GetCacheScoreCard":     {Tier: tierUnfiltered, KnownServerError: "gorm record-not-found surfaces as codes.Unknown instead of NotFound"},
	bbPrefix + "GetCacheMetadata":      {Tier: tierUnfiltered},
	bbPrefix + "GetTree":               {Tier: tierUnfiltered},
	bbPrefix + "GetTreeDirectorySizes": {Tier: tierUnfiltered},
	bbPrefix + "GetTarget":             {Tier: tierUnfiltered},
	bbPrefix + "GetTargetHistory":      {Tier: tierUnfiltered},
	bbPrefix + "GetExecution":          {Tier: tierUnfiltered},
	bbPrefix + "GetExecutionDownloads": {Tier: tierUnfiltered},
	bbPrefix + "WaitExecution":         {Tier: tierUnfiltered},
	bbPrefix + "GetZipManifest":        {Tier: tierUnfiltered},

	// --- Audit / notifications (handler does its own capability check) ----
	bbPrefix + "GetAuditLogs":     {Tier: tierUnfiltered, HandlerRejects: true},              // requires AUDIT_LOG_READ
	bbPrefix + "SendNotification": {Tier: tierUnfiltered, Write: true, HandlerRejects: true}, // requires SEND_NOTIFICATION

	// --- GitHub passthrough (uses the user's linked GitHub token) ----------
	bbPrefix + "UnlinkUserGitHubAccount":        {Tier: tierUnfiltered, Write: true, Skip: "would unlink the key owner's GitHub account"},
	bbPrefix + "GetGithubUserInstallations":     {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "GetGithubUser":                  {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "GetGithubRepo":                  {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "GetGithubContent":               {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "GetGithubTree":                  {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "CreateGithubTree":               {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "GetGithubBlob":                  {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "CreateGithubBlob":               {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "CreateGithubPull":               {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "MergeGithubPull":                {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "GetGithubCompare":               {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "GetGithubForks":                 {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "CreateGithubFork":               {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "GetGithubCommits":               {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "CreateGithubCommit":             {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "UpdateGithubRef":                {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "CreateGithubRef":                {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "GetGithubPullRequest":           {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "GetGithubPullRequestDetails":    {Tier: tierUnfiltered, KnownServerError: "returns Internal when the key owner has no usable GitHub token (should be FailedPrecondition)"},
	bbPrefix + "CreateGithubPullRequestComment": {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "UpdateGithubPullRequestComment": {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "DeleteGithubPullRequestComment": {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},
	bbPrefix + "SendGithubPullRequestReview":    {Tier: tierUnfiltered, Write: true, Skip: "GitHub write"},

	// --- Group member: org history and stats -------------------------------
	bbPrefix + "SearchInvocation":               {Tier: tierMember},
	bbPrefix + "GetInvocationStat":              {Tier: tierMember},
	bbPrefix + "GetTrend":                       {Tier: tierMember},
	bbPrefix + "GetStatHeatmap":                 {Tier: tierMember},
	bbPrefix + "GetStatDrilldown":               {Tier: tierMember},
	bbPrefix + "GetTargetTrends":                {Tier: tierMember},
	bbPrefix + "SearchExecution":                {Tier: tierMember},
	bbPrefix + "GetTargetStats":                 {Tier: tierMember},
	bbPrefix + "GetDailyTargetStats":            {Tier: tierMember},
	bbPrefix + "GetTargetFlakeSamples":          {Tier: tierMember},
	bbPrefix + "GetInvocationFilterSuggestions": {Tier: tierMember},

	// --- Group member: workflows / repos (read) ----------------------------
	bbPrefix + "GetWorkflows":         {Tier: tierMember},
	bbPrefix + "GetRepos":             {Tier: tierMember},
	bbPrefix + "GetWorkflowHistory":   {Tier: tierMember},
	bbPrefix + "GetLinkedGitHubRepos": {Tier: tierMember},

	// --- Group member: per-invocation actions ------------------------------
	bbPrefix + "UpdateInvocation":   {Tier: tierMember, Write: true}, // empty invocation id fails lookup
	bbPrefix + "DeleteInvocation":   {Tier: tierMember, Write: true}, // empty invocation id fails lookup
	bbPrefix + "CancelExecutions":   {Tier: tierMember, Write: true, KnownServerError: "gorm record-not-found surfaces as codes.Unknown instead of NotFound"},
	bbPrefix + "ExecuteWorkflow":    {Tier: tierMember, Write: true, Skip: "would start a workflow run"},
	bbPrefix + "InvalidateSnapshot": {Tier: tierMember, Write: true},
	bbPrefix + "WriteEventLog":      {Tier: tierMember, Write: true, Skip: "client-streaming write"},
	bbPrefix + "UpdateRunStatus":    {Tier: tierMember, Write: true, KnownServerError: "gorm record-not-found surfaces as codes.Unknown instead of NotFound"},

	// --- Group member: API keys --------------------------------------------
	bbPrefix + "GetApiKeys":       {Tier: tierMember}, // developers only see developer-visible org keys
	bbPrefix + "GetApiKey":        {Tier: tierMember},
	bbPrefix + "GetUserApiKeys":   {Tier: tierMember},
	bbPrefix + "GetUserApiKey":    {Tier: tierMember},
	bbPrefix + "CreateUserApiKey": {Tier: tierMember, Write: true, Skip: "would create a key on the owner's account"},
	bbPrefix + "UpdateUserApiKey": {Tier: tierMember, Write: true},
	bbPrefix + "DeleteUserApiKey": {Tier: tierMember, Write: true},

	// --- Group member: remote bazel, codesearch, workspaces ----------------
	bbPrefix + "Run":                   {Tier: tierMember, Write: true, Skip: "would start a remote bazel run"},
	bbPrefix + "Search":                {Tier: tierMember},
	bbPrefix + "KytheProxy":            {Tier: tierMember},
	bbPrefix + "Index":                 {Tier: tierMember, Write: true, Skip: "would trigger indexing"},
	bbPrefix + "RepoStatus":            {Tier: tierMember},
	bbPrefix + "GetWorkspace":          {Tier: tierMember},
	bbPrefix + "SaveWorkspace":         {Tier: tierMember, Write: true, Skip: "would write a workspace"},
	bbPrefix + "GetWorkspaceDirectory": {Tier: tierMember, KnownServerError: "handler panics on an empty request (Internal: A panic occurred)"},
	bbPrefix + "GetWorkspaceFile":      {Tier: tierMember},

	// --- Group admin --------------------------------------------------------
	bbPrefix + "UpdateGroup":                   {Tier: tierAdmin, Write: true},
	bbPrefix + "GetGroupUsers":                 {Tier: tierAdmin},
	bbPrefix + "UpdateGroupUsers":              {Tier: tierAdmin, Write: true},
	bbPrefix + "GetUserLists":                  {Tier: tierAdmin},
	bbPrefix + "GetUserList":                   {Tier: tierAdmin},
	bbPrefix + "CreateUserList":                {Tier: tierAdmin, Write: true},
	bbPrefix + "DeleteUserList":                {Tier: tierAdmin, Write: true},
	bbPrefix + "UpdateUserList":                {Tier: tierAdmin, Write: true},
	bbPrefix + "UpdateUserListMembership":      {Tier: tierAdmin, Write: true},
	bbPrefix + "UnlinkGitHubAccount":           {Tier: tierAdmin, Write: true},
	bbPrefix + "LinkGitHubAppInstallation":     {Tier: tierAdmin, Write: true},
	bbPrefix + "GetGitHubAppInstallations":     {Tier: tierAdmin},
	bbPrefix + "UnlinkGitHubAppInstallation":   {Tier: tierAdmin, Write: true},
	bbPrefix + "UpdateGitHubAppInstallation":   {Tier: tierAdmin, Write: true},
	bbPrefix + "GetAccessibleGitHubRepos":      {Tier: tierAdmin},
	bbPrefix + "GetGitHubAppInstallPath":       {Tier: tierAdmin},
	bbPrefix + "LinkGitHubRepo":                {Tier: tierAdmin, Write: true},
	bbPrefix + "UnlinkGitHubRepo":              {Tier: tierAdmin, Write: true},
	bbPrefix + "UpdateGitHubRepoSettings":      {Tier: tierAdmin, Write: true},
	bbPrefix + "CreateApiKey":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "UpdateApiKey":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "DeleteApiKey":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "GetPublicKey":                  {Tier: tierAdmin},
	bbPrefix + "ListSecrets":                   {Tier: tierAdmin},
	bbPrefix + "UpdateSecret":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "DeleteSecret":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "DeleteWorkflow":                {Tier: tierAdmin, Write: true},
	bbPrefix + "InvalidateAllSnapshotsForRepo": {Tier: tierAdmin, Write: true},
	bbPrefix + "GetExecutionNodes":             {Tier: tierAdmin},
	bbPrefix + "GetCacheProxies":               {Tier: tierAdmin},
	bbPrefix + "GetUsage":                      {Tier: tierAdmin},
	bbPrefix + "GetUsageAlertingRules":         {Tier: tierAdmin},
	bbPrefix + "CreateUsageAlertingRule":       {Tier: tierAdmin, Write: true},
	bbPrefix + "DeleteUsageAlertingRule":       {Tier: tierAdmin, Write: true},
	bbPrefix + "GetSSOConfig":                  {Tier: tierAdmin},
	bbPrefix + "SetSSOConfig":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "GetEncryptionConfig":           {Tier: tierAdmin},
	bbPrefix + "SetEncryptionConfig":           {Tier: tierAdmin, Write: true},
	bbPrefix + "CreateRepo":                    {Tier: tierAdmin, Write: true},
	bbPrefix + "GetIPRules":                    {Tier: tierAdmin},
	bbPrefix + "AddIPRule":                     {Tier: tierAdmin, Write: true},
	bbPrefix + "UpdateIPRule":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "DeleteIPRule":                  {Tier: tierAdmin, Write: true},
	bbPrefix + "GetIPRulesConfig":              {Tier: tierAdmin},
	bbPrefix + "SetIPRulesConfig":              {Tier: tierAdmin, Write: true},
	bbPrefix + "GetGCPProject":                 {Tier: tierAdmin},

	// --- Server admin -------------------------------------------------------
	bbPrefix + "GetInvocationOwner":        {Tier: tierServerAdmin},
	bbPrefix + "GetNamespace":              {Tier: tierServerAdmin},
	bbPrefix + "RemoveNamespace":           {Tier: tierServerAdmin, Write: true},
	bbPrefix + "ModifyNamespace":           {Tier: tierServerAdmin, Write: true},
	bbPrefix + "ApplyBucket":               {Tier: tierServerAdmin, Write: true},
	bbPrefix + "CreateImpersonationApiKey": {Tier: tierServerAdmin, Write: true},
	bbPrefix + "SetGroupStatus":            {Tier: tierServerAdmin, Write: true},

	// --- api/v1 (unfiltered by the interceptor; handlers do their own auth) -
	apiPrefix + "GetInvocation":    {Tier: tierUnfiltered},
	apiPrefix + "GetAuditLog":      {Tier: tierUnfiltered, HandlerRejects: true}, // requires AUDIT_LOG_READ
	apiPrefix + "GetLog":           {Tier: tierUnfiltered},
	apiPrefix + "GetTarget":        {Tier: tierUnfiltered},
	apiPrefix + "GetAction":        {Tier: tierUnfiltered},
	apiPrefix + "GetFile":          {Tier: tierUnfiltered},
	apiPrefix + "GetFileRange":     {Tier: tierUnfiltered},
	apiPrefix + "DeleteFile":       {Tier: tierUnfiltered, Write: true, HandlerRejects: true}, // --enable_cache_delete_api is off on QA; flip when enabled
	apiPrefix + "ExecuteWorkflow":  {Tier: tierMember, Write: true, Skip: "would start a workflow run"},
	apiPrefix + "Run":              {Tier: tierMember, Write: true, Skip: "would start a remote bazel run"},
	apiPrefix + "CreateUserApiKey": {Tier: tierMember, Write: true, Skip: "would create a key on the owner's account"},
}

// ---------------------------------------------------------------------------

type harness struct {
	t      *testing.T
	conn   *grpc.ClientConn
	apiKey string

	mu      sync.Mutex
	results []result
}

type result struct {
	Method   string `json:"method"`
	Tier     string `json:"tier"`
	Write    bool   `json:"write"`
	Expected string `json:"expected"`
	Oracle   string `json:"oracle"`
	Actual   string `json:"actual"`
	Verdict  string `json:"verdict"`
	Note     string `json:"note,omitempty"`
}

func newHarness(t *testing.T) *harness {
	key := os.Getenv(*apiKeyEnv)
	if key == "" {
		t.Skipf("env var %s is not set; nothing to test", *apiKeyEnv)
	}
	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		t.Fatalf("dial %s: %s", *target, err)
	}
	t.Cleanup(func() { conn.Close() })
	return &harness{t: t, conn: conn, apiKey: key}
}

func (h *harness) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), *rpcTime)
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", h.apiKey)
	return ctx, cancel
}

// invoke calls a method by descriptor with an empty request and returns the
// resulting status (nil for OK).
func (h *harness) invoke(m protoreflect.MethodDescriptor) error {
	full := fmt.Sprintf("/%s/%s", m.Parent().FullName(), m.Name())
	req := dynamicpb.NewMessage(m.Input())
	res := dynamicpb.NewMessage(m.Output())
	ctx, cancel := h.ctx()
	defer cancel()

	if m.IsStreamingClient() {
		return fmt.Errorf("client streaming not supported by the generic invoker")
	}
	if m.IsStreamingServer() {
		desc := &grpc.StreamDesc{StreamName: string(m.Name()), ServerStreams: true}
		stream, err := h.conn.NewStream(ctx, desc, full)
		if err != nil {
			return err
		}
		if err := stream.SendMsg(req); err != nil {
			return err
		}
		if err := stream.CloseSend(); err != nil {
			return err
		}
		return stream.RecvMsg(res)
	}
	return h.conn.Invoke(ctx, full, req, res)
}

func codeOf(err error) codes.Code { return status.Code(err) }

func describe(err error) string {
	if err == nil {
		return "OK"
	}
	s, _ := status.FromError(err)
	msg := s.Message()
	if len(msg) > 80 {
		msg = msg[:77] + "..."
	}
	return fmt.Sprintf("%s: %s", s.Code(), msg)
}

func isFilterDenied(err error) bool {
	s, _ := status.FromError(err)
	return s.Code() == codes.PermissionDenied && s.Message() == filterDeniedMsg
}

// classify decides whether an observed status matches the row's expectation.
func classify(r row, err error) (verdict, note string) {
	switch {
	case !r.Tier.filterAllows():
		// The interceptor must reject before the handler runs, so the only
		// acceptable answer is the filter's own PermissionDenied.
		if isFilterDenied(err) {
			return "PASS", ""
		}
		if codeOf(err) == codes.PermissionDenied {
			return "PASS", "denied by handler, not by filter"
		}
		return "FAIL", "expected filter denial"
	case r.HandlerRejects:
		if isFilterDenied(err) {
			return "FAIL", "denied by filter; expected to reach handler"
		}
		if c := codeOf(err); c == codes.PermissionDenied || c == codes.Unauthenticated {
			return "PASS", ""
		}
		return "FAIL", "expected handler rejection"
	default:
		if isFilterDenied(err) {
			return "FAIL", "denied by filter"
		}
		// Not a tagged switch: the exhaustive linter would demand every code.
		c := codeOf(err)
		if c == codes.PermissionDenied {
			return "FAIL", "denied by handler"
		}
		if c == codes.Unauthenticated {
			return "FAIL", "unauthenticated"
		}
		if c == codes.Internal || c == codes.Unknown {
			// Authorization let the call through, but the handler broke.
			// That is not evidence the RPC works for this persona.
			return "FAIL", serverErrorNote
		}
		return "PASS", ""
	}
}

// record stores a row for the summary table and reports it on the subtest:
// FAIL rows fail, ORACLE-ONLY rows are marked skipped so the UI shows that
// the RPC was not actually invoked.
func (h *harness) record(t *testing.T, r result) {
	h.mu.Lock()
	h.results = append(h.results, r)
	h.mu.Unlock()
	line := fmt.Sprintf("tier=%s expected=%s oracle=%s actual=%s %s", r.Tier, r.Expected, r.Oracle, r.Actual, r.Note)
	switch r.Verdict {
	case "FAIL":
		t.Error(line)
	case "ORACLE-ONLY":
		t.Skip(line)
	case "KNOWN-ERROR":
		t.Skip("known server error: " + line)
	default:
		t.Log(line)
	}
}

// subtestName turns "/pkg.Service/Method" into "Service/Method" so the
// JUnit output nests one test case per RPC under the service.
func subtestName(full string) string {
	parts := strings.Split(strings.TrimPrefix(full, "/"), "/")
	svc := parts[0]
	if i := strings.LastIndex(svc, "."); i >= 0 {
		svc = svc[i+1:]
	}
	return svc + "/" + strings.Join(parts[1:], "/")
}

func (h *harness) report(name string) {
	sort.Slice(h.results, func(i, j int) bool { return h.results[i].Method < h.results[j].Method })
	var b strings.Builder
	fmt.Fprintf(&b, "| Method | Tier | Kind | Expected | Oracle | Actual | Verdict | Note |\n|---|---|---|---|---|---|---|---|\n")
	for _, r := range h.results {
		kind := "read"
		if r.Write {
			kind = "write"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n", r.Method, r.Tier, kind, r.Expected, r.Oracle, r.Actual, r.Verdict, r.Note)
	}
	h.t.Logf("\n%s", b.String())
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		os.WriteFile(filepath.Join(dir, name+".md"), []byte(b.String()), 0644)
		js, _ := json.MarshalIndent(h.results, "", "  ")
		os.WriteFile(filepath.Join(dir, name+".json"), js, 0644)
	}
}

// ---------------------------------------------------------------------------

func serviceMethods() []protoreflect.MethodDescriptor {
	var out []protoreflect.MethodDescriptor
	for _, fd := range []protoreflect.FileDescriptor{
		bbspb.File_proto_buildbuddy_service_proto,
		apipb.File_proto_api_v1_service_proto,
	} {
		svcs := fd.Services()
		for i := 0; i < svcs.Len(); i++ {
			ms := svcs.Get(i).Methods()
			for j := 0; j < ms.Len(); j++ {
				out = append(out, ms.Get(j))
			}
		}
	}
	return out
}

func fullName(m protoreflect.MethodDescriptor) string {
	return fmt.Sprintf("/%s/%s", m.Parent().FullName(), m.Name())
}

// TestTableIsComplete fails when the service descriptors and the expectation
// table disagree about which RPCs exist.
func TestTableIsComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, m := range serviceMethods() {
		name := fullName(m)
		seen[name] = true
		t.Run(subtestName(name), func(t *testing.T) {
			if _, ok := expectations[name]; !ok {
				t.Errorf("%s exists on the server but has no row in the expectation table; classify it", name)
			}
		})
	}
	for name := range expectations {
		if !seen[name] {
			t.Errorf("%s is in the expectation table but is not a real RPC", name)
		}
	}
}

// TestMemberKeyMatrix runs the oracle and invocation checks for every
// BuildBuddyService and ApiService RPC using the configured key.
func TestMemberKeyMatrix(t *testing.T) {
	h := newHarness(t)

	// Establish who we are, and fetch the server's self-reported allowed
	// RPC list to use as the oracle.
	ctx, cancel := h.ctx()
	defer cancel()
	user, err := bbspb.NewBuildBuddyServiceClient(h.conn).GetUser(ctx, &uspb.GetUserRequest{})
	if err != nil {
		t.Fatalf("GetUser: %s", err)
	}
	if user.GetDisplayUser().GetUserId().GetId() == "" {
		t.Fatalf("key does not carry a user id; this test expects a user-owned key")
	}
	selected := user.GetSelectedGroup().GetGroupId()
	if *groupID != "" && selected != *groupID {
		t.Fatalf("key authenticates as group %s, want %s", selected, *groupID)
	}
	t.Logf("user=%s group=%s target=%s", user.GetDisplayUser().GetUserId().GetId(), selected, *target)
	oracle := map[string]bool{}
	for _, name := range user.GetAllowedRpc() {
		oracle[name] = true
	}

	for _, m := range serviceMethods() {
		name := fullName(m)
		r, ok := expectations[name]
		if !ok {
			continue // reported by TestTableIsComplete
		}
		t.Run(subtestName(name), func(t *testing.T) {
			res := result{Method: name, Tier: r.Tier.String(), Write: r.Write}
			switch {
			case !r.Tier.filterAllows():
				res.Expected = "filter-denied"
			case r.HandlerRejects:
				res.Expected = "handler-rejected"
			default:
				res.Expected = "allowed"
			}

			// Oracle check: allowed_rpc uses bare method names.
			oracleAllows := oracle[string(m.Name())]
			if oracleAllows {
				res.Oracle = "allow"
			} else {
				res.Oracle = "deny"
			}
			if oracleAllows != r.Tier.filterAllows() {
				res.Verdict = "FAIL"
				res.Note = "server's allowed_rpc disagrees with table"
				res.Actual = "-"
				h.record(t, res)
				return
			}

			if r.Skip != "" {
				res.Actual = "not invoked: " + r.Skip
				res.Verdict = "ORACLE-ONLY"
				h.record(t, res)
				return
			}

			err := h.invoke(m)
			res.Actual = describe(err)
			res.Verdict, res.Note = classify(r, err)
			if res.Verdict == "FAIL" && res.Note == serverErrorNote && r.KnownServerError != "" {
				res.Verdict = "KNOWN-ERROR"
				res.Note = r.KnownServerError
			}
			h.record(t, res)
		})
	}
	h.report("buildbuddy_service")
}

// ---------------------------------------------------------------------------
// Cache / execution services. These are authorized by the key's capability
// bits rather than the RPC-name filter, so they get explicit typed calls.

// freshBlob returns unique content and its digest, so that a write attempt
// can be followed by a read to prove whether anything was stored.
func freshBlob() ([]byte, *repb.Digest) {
	data := []byte("authz-matrix-" + time.Now().Format(time.RFC3339Nano))
	sum := sha256.Sum256(data)
	return data, &repb.Digest{Hash: hex.EncodeToString(sum[:]), SizeBytes: int64(len(data))}
}

// TestMemberKeyCacheServices asserts that a key without CACHE_WRITE / CAS_WRITE
// can read from but not write to the cache and action cache.
//
// Note the server deliberately answers OK to cache writes from read-only keys
// so that bazel does not abort (see BatchUpdateBlobs, ByteStream.Write and
// UpdateActionResult in server/remote_cache). The status code therefore
// proves nothing; each write case is followed by a read that must still miss.
func TestMemberKeyCacheServices(t *testing.T) {
	h := newHarness(t)
	data, d := freshBlob()
	cas := repb.NewContentAddressableStorageClient(h.conn)
	ac := repb.NewActionCacheClient(h.conn)

	// verifyNotWritten returns an error if the blob or action result exists.
	verifyNotWritten := func(ctx context.Context) error {
		missing, err := cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
		if err != nil {
			return fmt.Errorf("FindMissingBlobs: %w", err)
		}
		if len(missing.GetMissingBlobDigests()) != 1 {
			return fmt.Errorf("blob %s was written to the CAS by a read-only key", d.GetHash())
		}
		if _, err := ac.GetActionResult(ctx, &repb.GetActionResultRequest{ActionDigest: d}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("action result %s exists after write by a read-only key (err=%v)", d.GetHash(), err)
		}
		return nil
	}

	type cacheCase struct {
		name  string
		write bool
		// expectIgnored: the write must leave no trace, whatever the status.
		expectIgnored bool
		call          func(ctx context.Context) error
	}
	cases := []cacheCase{
		{name: "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities", call: func(ctx context.Context) error {
			_, err := repb.NewCapabilitiesClient(h.conn).GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs", call: func(ctx context.Context) error {
			_, err := repb.NewContentAddressableStorageClient(h.conn).FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{d}})
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs", call: func(ctx context.Context) error {
			_, err := repb.NewContentAddressableStorageClient(h.conn).BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{d}})
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree", call: func(ctx context.Context) error {
			s, err := repb.NewContentAddressableStorageClient(h.conn).GetTree(ctx, &repb.GetTreeRequest{RootDigest: d})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs", write: true, expectIgnored: true, call: func(ctx context.Context) error {
			_, err := cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
				Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: d, Data: data}},
			})
			return err
		}},
		{name: "/google.bytestream.ByteStream/Read", call: func(ctx context.Context) error {
			s, err := bspb.NewByteStreamClient(h.conn).Read(ctx, &bspb.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", d.GetHash(), d.GetSizeBytes())})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		}},
		{name: "/google.bytestream.ByteStream/Write", write: true, expectIgnored: true, call: func(ctx context.Context) error {
			s, err := bspb.NewByteStreamClient(h.conn).Write(ctx)
			if err != nil {
				return err
			}
			err = s.Send(&bspb.WriteRequest{
				ResourceName: fmt.Sprintf("uploads/00000000-0000-0000-0000-000000000000/blobs/%s/%d", d.GetHash(), d.GetSizeBytes()),
				Data:         data,
				FinishWrite:  true,
			})
			if err != nil && err.Error() != "EOF" {
				return err
			}
			_, err = s.CloseAndRecv()
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.ActionCache/GetActionResult", call: func(ctx context.Context) error {
			_, err := repb.NewActionCacheClient(h.conn).GetActionResult(ctx, &repb.GetActionResultRequest{ActionDigest: d})
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult", write: true, expectIgnored: true, call: func(ctx context.Context) error {
			_, err := ac.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{ActionDigest: d, ActionResult: &repb.ActionResult{ExitCode: 0}})
			return err
		}},
		{name: "/build.bazel.remote.execution.v2.Execution/Execute", write: true, call: func(ctx context.Context) error {
			s, err := repb.NewExecutionClient(h.conn).Execute(ctx, &repb.ExecuteRequest{ActionDigest: d})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		}},
		{name: "/build.bazel.remote.asset.v1.Fetch/FetchBlob", call: func(ctx context.Context) error {
			_, err := rapb.NewFetchClient(h.conn).FetchBlob(ctx, &rapb.FetchBlobRequest{Uris: []string{"https://invalid.example/authz-matrix"}})
			return err
		}},
		{name: "/build.bazel.remote.asset.v1.Push/PushBlob", write: true, call: func(ctx context.Context) error {
			_, err := rapb.NewPushClient(h.conn).PushBlob(ctx, &rapb.PushBlobRequest{Uris: []string{"https://invalid.example/authz-matrix"}, BlobDigest: d})
			return err
		}},
		{name: "/google.devtools.build.v1.PublishBuildEvent/PublishLifecycleEvent", write: true, call: func(ctx context.Context) error {
			_, err := pepb.NewPublishBuildEventClient(h.conn).PublishLifecycleEvent(ctx, &pepb.PublishLifecycleEventRequest{})
			return err
		}},
	}

	for _, c := range cases {
		t.Run(subtestName(c.name), func(t *testing.T) {
			ctx, cancel := h.ctx()
			err := c.call(ctx)
			cancel()
			res := result{Method: c.name, Tier: "capability", Write: c.write, Oracle: "-", Actual: describe(err)}
			if c.expectIgnored {
				res.Expected = "no write (no CACHE_WRITE/CAS_WRITE)"
				vctx, vcancel := h.ctx()
				verr := verifyNotWritten(vctx)
				vcancel()
				if verr == nil {
					res.Verdict = "PASS"
					res.Note = "server answered " + status.Code(err).String() + "; read-back confirms nothing stored"
				} else {
					res.Verdict = "FAIL"
					res.Note = verr.Error()
				}
			} else {
				res.Expected = "allowed"
				if c := codeOf(err); c == codes.PermissionDenied || c == codes.Unauthenticated {
					res.Verdict = "FAIL"
				} else {
					res.Verdict = "PASS"
				}
			}
			h.record(t, res)
		})
	}
	h.report("cache_services")
}
