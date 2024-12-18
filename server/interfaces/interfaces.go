package interfaces

import (
	"context"
	"crypto/tls"
	"database/sql"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/golang-jwt/jwt"
	"github.com/google/go-github/v59/github"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc/credentials"
	"gorm.io/gorm"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	gcpb "github.com/buildbuddy-io/buildbuddy/proto/gcp"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	csinpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	qpb "github.com/buildbuddy-io/buildbuddy/proto/quota"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rppb "github.com/buildbuddy-io/buildbuddy/proto/repo"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	cssrpb "github.com/buildbuddy-io/buildbuddy/proto/search"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
	stpb "github.com/buildbuddy-io/buildbuddy/proto/stats"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	supb "github.com/buildbuddy-io/buildbuddy/proto/suggestion"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	wspb "github.com/buildbuddy-io/buildbuddy/proto/workspace"
	zipb "github.com/buildbuddy-io/buildbuddy/proto/zip"
	dto "github.com/prometheus/client_model/go"
	hlpb "google.golang.org/grpc/health/grpc_health_v1"
)

// An interface representing a mux for handling/serving http requests.
type HttpServeMux interface {
	Handle(pattern string, handler http.Handler)
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

// An interface representing the user info gleaned from an authorization header.
type UserToken interface {
	GetIssuer() string
	GetSubscriber() string

	// Returns a fq string usable as an ID for this issuer + subscriber.
	GetSubID() string
}

// An interface representing the user info gleaned from http basic auth, which is
// often set for GRPC requests.
type BasicAuthToken interface {
	GetUser() string
	GetPassword() string
}

// GroupMembership represents a user's membership within a group as well as
// their role within that group.
type GroupMembership struct {
	GroupID      string                   `json:"group_id"`
	Capabilities []akpb.ApiKey_Capability `json:"capabilities"`
	// DEPRECATED. Check Capabilities instead.
	Role role.Role `json:"role"`
}

type UserInfo interface {
	jwt.Claims

	// ID of the API Key used to authenticate the request or empty if an API
	// key was not used.
	GetAPIKeyID() string
	// ID of the authenticated user. Empty if authenticated using an Org API
	// key.
	GetUserID() string
	GetGroupID() string
	// IsImpersonating returns whether the group ID is being impersonated by the
	// user. This means that the user is not actually a member of the group, but
	// is temporarily acting as a group member. Only server admins have this
	// capability.
	IsImpersonating() bool
	// GetAllowedGroups returns the IDs of the groups of which the user is a
	// member.
	// DEPRECATED: Use GetGroupMemberships instead.
	GetAllowedGroups() []string
	// GetGroupMemberships returns the user's group memberships.
	GetGroupMemberships() []*GroupMembership
	// GetCapabilities returns the user's capabilities.
	GetCapabilities() []akpb.ApiKey_Capability
	HasCapability(akpb.ApiKey_Capability) bool
	GetUseGroupOwnedExecutors() bool
	GetCacheEncryptionEnabled() bool
	GetEnforceIPRules() bool
	IsSAML() bool
}

// Authenticator constants
const (
	AuthContextUserErrorKey = "auth.error"

	// AuthAnonymousUser is the identifier for unauthenticated users in installations that allow anonymous users.
	AuthAnonymousUser = "ANON"
)

type InstallationAuthenticator interface {
	// The ID of the admin group
	AdminGroupID() string
	// Whether or not anonymous usage is enabled
	AnonymousUsageEnabled(ctx context.Context) bool
	// Return a slice containing the providers
	PublicIssuers() []string
}

type HTTPAuthenticator interface {
	// Redirect to configured authentication provider.
	Login(w http.ResponseWriter, r *http.Request) error
	// Clear any logout state.
	Logout(w http.ResponseWriter, r *http.Request) error
	// Handle a callback from authentication provider.
	Auth(w http.ResponseWriter, r *http.Request) error

	// AuthenticatedHTTPContext authenticates the user using the credentials present in the HTTP request and creates a
	// child context that contains the results.
	//
	// This function is called automatically for every HTTP request via a filter and the new context is passed to
	// application code.
	//
	// Application code can retrieve the stored information by calling AuthenticatedUser.
	AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context

	// Whether SSO is enabled for this authenticator
	SSOEnabled() bool
}

type GRPCAuthenticator interface {
	// AuthenticatedGRPCContext authenticates the user using the credentials present in the gRPC metadata and creates a
	// child context that contains the result.
	//
	// This function is called automatically for every gRPC request via a filter and the new context is passed to
	// application code.
	//
	// Application code that retrieve the stored information by calling AuthenticatedUser.
	AuthenticatedGRPCContext(ctx context.Context) context.Context

	// AuthenticateGRPCRequest authenticates the user using the credentials present in the gRPC metadata and returns the
	// result.
	//
	// You should only use this function if you need fresh information (for example to re-validate credentials during a
	// long running operation). For all other cases it is better to use the information cached in the context
	// retrieved via AuthenticatedUser.
	AuthenticateGRPCRequest(ctx context.Context) (UserInfo, error)
}

type UserAuthenticator interface {
	// FillUser may be used to construct an initial tables.User object. It
	// is filled based on information from the authenticator's JWT.
	FillUser(ctx context.Context, user *tables.User) error

	// AuthenticatedUser returns the UserInfo stored in the context.
	//
	// See AuthenticatedHTTPContext/AuthenticatedGRPCContext for a description of how the context is created.
	AuthenticatedUser(ctx context.Context) (UserInfo, error)
}
type APIKeyAuthenticator interface {
	// Returns a context containing the given API key.
	AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context

	// TrustedJWTFromAuthContext returns a JWT from the authenticated context,
	// or empty string if the context is not authenticated.
	TrustedJWTFromAuthContext(ctx context.Context) string

	// AuthContextFromTrustedJWT returns an authenticated context using a JWT
	// which has been previously authenticated.
	AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context
}

type Authenticator interface {
	InstallationAuthenticator
	UserAuthenticator
	HTTPAuthenticator
	GRPCAuthenticator
	APIKeyAuthenticator
}

type BuildBuddyServer interface {
	bbspb.BuildBuddyServiceServer
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

type SSLService interface {
	IsEnabled() bool
	IsCertGenerationEnabled() bool
	ConfigureTLS(mux http.Handler) (*tls.Config, http.Handler)
	GetGRPCSTLSCreds() (credentials.TransportCredentials, error)
	GenerateCerts(apiKeyID string) (string, string, error)
	ValidateCert(certString string) (string, string, error)
}

type BuildEventChannel interface {
	Context() context.Context
	FinalizeInvocation(iid string) error
	HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error
	GetNumDroppedEvents() uint64
	GetInitialSequenceNumber() int64
	Close()
}

type BuildEventHandler interface {
	OpenChannel(ctx context.Context, iid string) BuildEventChannel
}

type GitHubStatusService interface {
	GetStatusClient(accessToken string) GitHubStatusClient
}

type GitHubStatusClient interface {
	CreateStatus(ctx context.Context, ownerRepo, commitSHA string, payload *github.RepoStatus) error
}

// A Blobstore must allow for reading, writing, and deleting blobs.
type Blobstore interface {
	BlobExists(ctx context.Context, blobName string) (bool, error)
	ReadBlob(ctx context.Context, blobName string) ([]byte, error)
	WriteBlob(ctx context.Context, blobName string, data []byte) (int, error)

	// DeleteBlob does not return an error if the blob does not exist; some
	// blobstores do not distinguish on return between deleting an existing blob
	// and calling delete on a non-existent blob, so this is the only way to
	// provide a consistent interface.
	DeleteBlob(ctx context.Context, blobName string) error
	Writer(ctx context.Context, blobName string) (CommittedWriteCloser, error)
}

type CacheMetadata struct {
	// Size of the cached data. If the data was compressed, this is the compressed size
	StoredSizeBytes    int64
	DigestSizeBytes    int64
	LastAccessTimeUsec int64
	LastModifyTimeUsec int64
}

// Similar to a blobstore, a cache allows for reading and writing data, but
// additionally it is responsible for deleting data that is past TTL to keep to
// a manageable size.
// Similar to the Cache above, a digest cache allows for more intelligent
// storing of blob data based on its size.
type Cache interface {
	// Normal cache-like operations
	Contains(ctx context.Context, r *rspb.ResourceName) (bool, error)
	Metadata(ctx context.Context, r *rspb.ResourceName) (*CacheMetadata, error)
	FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error)
	Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error)
	GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error)
	Set(ctx context.Context, r *rspb.ResourceName, data []byte) error
	SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error
	Delete(ctx context.Context, r *rspb.ResourceName) error

	// Low level interface used for seeking and stream-writing.
	Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error)
	Writer(ctx context.Context, r *rspb.ResourceName) (CommittedWriteCloser, error)

	// SupportsCompressor returns whether the cache supports storing data compressed with the given compressor
	SupportsCompressor(compressor repb.Compressor_Value) bool
	SupportsEncryption(ctx context.Context) bool
}

type StoppableCache interface {
	Cache
	Stop() error
}

type PooledByteStreamClient interface {
	StreamBytestreamFile(ctx context.Context, url *url.URL, writer io.Writer) error
	FetchBytestreamZipManifest(ctx context.Context, url *url.URL) (*zipb.Manifest, error)
	StreamSingleFileFromBytestreamZip(ctx context.Context, url *url.URL, entry *zipb.ManifestEntry, out io.Writer) error
}

type DBOptions interface {
	WithStaleReads() DBOptions
	ReadOnly() bool
	AllowStaleReads() bool
}

type DBResult struct {
	Error        error
	RowsAffected int64
}

type DBRawQuery interface {
	// Take executes the query and scans the resulting row into the target
	// struct. An error is returned if no records match. To check for this
	// error use db.IsRecordNotFound.
	Take(dest interface{}) error
	// Exec executes the raw modification query and returns the result.
	Exec() DBResult
	// IterateRaw executes the select query and iterates over the raw result
	// set, executing the passed function for every row. If iterating over
	// GORM types, use the db.ScanRows convenience function.
	IterateRaw(fn func(ctx context.Context, row *sql.Rows) error) error
}

type DBQuery interface {
	// Create inserts a new row using the passed GORM-annotated struct.
	Create(val interface{}) error
	// Update updates an existing row using the primary key of the given
	// GORM-annotated struct. Returns gorm.ErrRecordNotFound if a matching
	// row does not exist.
	Update(val interface{}) error
	// Raw prepares a raw query.
	Raw(sql string, values ...interface{}) DBRawQuery
}

type DB interface {
	// NewQuery creates a new query handle with the given name. The name is
	// included in exported metrics and so should be a constant.
	NewQuery(ctx context.Context, name string) DBQuery

	// GORM returns a raw handle to the GORM API. New code should prefer to
	// avoid using this.
	GORM(ctx context.Context, name string) *gorm.DB

	NowFunc() time.Time

	// TODO(jdhollen): convert this to an enum instead of depending on the
	// GORM value.
	DialectName() string
}

type TxRunner func(tx *gorm.DB) error
type NewTxRunner func(tx DB) error

// DBHandle is the API for interacting with the database.
//
// Every interaction should start with a call to NewQuery in order to provide
// a name that is used in monitoring. This name should be fixed as it will
// be exported as a metric label value.
//
// Creating a new record:
//
//	myFoo := &tables.Foo{}
//	err := dbh.NewQuery(ctx, "foo_package_new_foo").Create(myFoo);
//
// Updating a record by primary key:
//
//	myFoo := &tables.Foo{ID: "foo", SomeCol: "bar"}
//	err := dbh.NewQuery(ctx, "foo_package_update_foo").Update(myFoo);
//
// Getting a single record:
//
//	myFoo := &tables.Foo{}
//	rq := dbh.NewQuery(ctx, "foo_package_get_foo").Raw(`
//	   SELECT * FROM "Foos" WHERE id = ?`, id)
//	err := pq.Take(myFoo)
//
// Iterating over multiple records:
//
//	 rq := dbh.NewQuery(ctx, "foo_package_get_foos").Raw(`
//		   SELECT * FROM "Foos" WHERE foo = 'bar'`)
//	 err := db.ScanRows(rq, func(ctx context.Context, foo *tables.Foo) error {
//	   // process foo
//	   return nil
//	})
type DBHandle interface {
	DB

	Transaction(ctx context.Context, txn NewTxRunner) error
	TransactionWithOptions(ctx context.Context, opts DBOptions, txn NewTxRunner) error

	NewQueryWithOpts(ctx context.Context, name string, opts DBOptions) DBQuery
	DateFromUsecTimestamp(fieldName string, timezoneOffsetMinutes int32) string
	SelectForUpdateModifier() string
	SetNowFunc(now func() time.Time)
	IsDuplicateKeyError(err error) bool
	IsDeadlockError(err error) bool
}

// OLAPDBHandle is a DB Handle for Online Analytical Processing(OLAP) DB
type OLAPDBHandle interface {
	DB

	DateFromUsecTimestamp(fieldName string, timezoneOffsetMinutes int32) string
	FlushInvocationStats(ctx context.Context, ti *tables.Invocation) error
	FlushExecutionStats(ctx context.Context, inv *sipb.StoredInvocation, executions []*repb.StoredExecution) error
	FlushTestTargetStatuses(ctx context.Context, entries []*schema.TestTargetStatus) error
	InsertAuditLog(ctx context.Context, entry *schema.AuditLog) error
	BucketFromUsecTimestamp(fieldName string, loc *time.Location, interval string) (string, []interface{})
}

type InvocationDB interface {
	// Invocations API
	CreateInvocation(ctx context.Context, in *tables.Invocation) (bool, error)
	UpdateInvocation(ctx context.Context, in *tables.Invocation) (bool, error)
	UpdateInvocationACL(ctx context.Context, authenticatedUser *UserInfo, invocationID string, acl *aclpb.ACL) error
	LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error)
	LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error)
	LookupGroupIDFromInvocation(ctx context.Context, invocationID string) (string, error)
	LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error)
	LookupChildInvocations(ctx context.Context, parentRunID string) ([]string, error)
	DeleteInvocation(ctx context.Context, invocationID string) error
	DeleteInvocationWithPermsCheck(ctx context.Context, authenticatedUser *UserInfo, invocationID string) error
	FillCounts(ctx context.Context, log *telpb.TelemetryStat) error
	SetNowFunc(now func() time.Time)
}

type APIKeyGroup interface {
	GetCapabilities() int32
	GetAPIKeyID() string
	GetUserID() string
	GetGroupID() string
	GetChildGroupIDs() []string
	GetUseGroupOwnedExecutors() bool
	GetCacheEncryptionEnabled() bool
	GetEnforceIPRules() bool
}

type AuthDB interface {
	InsertOrUpdateUserSession(ctx context.Context, sessionID string, session *tables.Session) error
	ReadSession(ctx context.Context, sessionID string) (*tables.Session, error)
	ClearSession(ctx context.Context, sessionID string) error
	GetAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (APIKeyGroup, error)
	GetAPIKeyGroupFromAPIKeyID(ctx context.Context, apiKeyID string) (APIKeyGroup, error)
	LookupUserFromSubID(ctx context.Context, subID string) (*tables.User, error)

	// GetAPIKeyForInternalUseOnly returns any group-level API key for the
	// group. It is only to be used in situations where the user has a
	// pre-authorized grant to access resources on behalf of the org, such as a
	// publicly shared invocation. The returned API key must only be used to
	// access internal resources and must not be returned to the caller.
	GetAPIKeyForInternalUseOnly(ctx context.Context, groupID string) (*tables.APIKey, error)

	// API Keys API.
	//
	// All of these functions authenticate the user if applicable and
	// authorize access to the relevant user IDs, group IDs, and API keys,
	// taking the group role into account.
	//
	// Any operations involving user-level keys return an error if user-level
	// keys are not enabled by the org.

	// GetAPIKeys returns group-level API keys that the user is authorized to
	// access.
	GetAPIKeys(ctx context.Context, groupID string) ([]*tables.APIKey, error)

	// CreateAPIKey creates a group-level API key.
	CreateAPIKey(ctx context.Context, groupID string, label string, capabilities []akpb.ApiKey_Capability, visibleToDevelopers bool) (*tables.APIKey, error)

	// CreateAPIKeyWithoutAuthCheck creates a group-level API key without
	// checking that the user has admin rights on the group. This should only
	// be used when a new group is being created.
	CreateAPIKeyWithoutAuthCheck(ctx context.Context, tx DB, groupID string, label string, capabilities []akpb.ApiKey_Capability, visibleToDevelopers bool) (*tables.APIKey, error)

	// CreateImpersonationAPIKey creates a short-lived API key for the target
	// group ID.
	CreateImpersonationAPIKey(ctx context.Context, groupID string) (*tables.APIKey, error)

	// GetUserOwnedKeysEnabled returns whether user-owned keys are enabled.
	GetUserOwnedKeysEnabled() bool

	// GetUserAPIKeys returns all user-owned API keys within a group.
	GetUserAPIKeys(ctx context.Context, userID, groupID string) ([]*tables.APIKey, error)

	// CreateUserAPIKey creates a user-owned API key within the group. The given
	// user must be a member of the group. If the request is not authenticated
	// as the given user, then the authenticated user or API key must have
	// ORG_ADMIN capability.
	CreateUserAPIKey(ctx context.Context, groupID, userID, label string, capabilities []akpb.ApiKey_Capability) (*tables.APIKey, error)

	// GetAPIKey returns an API key by ID. The key may be user-owned or
	// group-owned.
	GetAPIKey(ctx context.Context, apiKeyID string) (*tables.APIKey, error)

	// UpdateAPIKey updates an API key by ID. The key may be user-owned or
	// group-owned.
	UpdateAPIKey(ctx context.Context, key *tables.APIKey) error

	// DeleteAPIKey deletes an API key by ID. The key may be user-owned or
	// group-owned.
	DeleteAPIKey(ctx context.Context, apiKeyID string) error
}

type UserDB interface {
	// User API
	InsertUser(ctx context.Context, u *tables.User) error
	// GetUser will return the registered user's information or
	// an error if no registered user was found. It requires that a
	// valid authenticator is present in the environment and will return
	// a UserToken given the provided context.
	GetUser(ctx context.Context) (*tables.User, error)
	GetUserByID(ctx context.Context, id string) (*tables.User, error)
	GetUserByIDWithoutAuthCheck(ctx context.Context, id string) (*tables.User, error)
	// GetUserByEmail lookups a user with the given e-mail address within the
	// currently authenticated group.
	//
	// An error will be returned if there are multiple users with the same
	// e-mail address. Normally this should not be the case, but it can occur
	// if a user transitions between auth providers (e.g. oidc to saml).
	GetUserByEmail(ctx context.Context, email string) (*tables.User, error)
	UpdateUser(ctx context.Context, u *tables.User) error
	// DeleteUser deletes a user and associated data.
	DeleteUser(ctx context.Context, id string) error
	// GetImpersonatedUser will return the authenticated user's information
	// with a single group membership corresponding to the group they are trying
	// to impersonate. It requires that the authenticated user has impersonation
	// permissions and is requesting to impersonate a group.
	GetImpersonatedUser(ctx context.Context) (*tables.User, error)
	FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error

	// Creates the DEFAULT group, for on-prem usage where there is only
	// one group and all users are implicitly a part of it.
	CreateDefaultGroup(ctx context.Context) error

	// Groups API

	// CreateGroup creates a new group, adds the authenticated user to it,
	// and creates an initial API key for the group.
	CreateGroup(ctx context.Context, g *tables.Group) (string, error)
	UpdateGroup(ctx context.Context, g *tables.Group) (string, error)
	GetGroupByID(ctx context.Context, groupID string) (*tables.Group, error)
	GetGroupByURLIdentifier(ctx context.Context, urlIdentifier string) (*tables.Group, error)

	// RequestToJoinGroup performs an attempt for the authenticated user to join
	// the given group. If the user email matches the group's owned domain, the
	// user is immediately added to the group. Otherwise, a pending membership
	// request is submitted. The return value is the user's new membership
	// status within the group after the transaction is performed (either
	// REQUESTED or MEMBER).
	RequestToJoinGroup(ctx context.Context, groupID string) (grpb.GroupMembershipStatus, error)

	GetGroupUsers(ctx context.Context, groupID string, statuses []grpb.GroupMembershipStatus) ([]*grpb.GetGroupUsersResponse_GroupUser, error)
	UpdateGroupUsers(ctx context.Context, groupID string, updates []*grpb.UpdateGroupUsersRequest_Update) error
	DeleteGroupGitHubToken(ctx context.Context, groupID string) error
	// DeleteUserGitHubToken deletes the authenticated user's GitHub token.
	DeleteUserGitHubToken(ctx context.Context) error

	// Secrets API

	GetOrCreatePublicKey(ctx context.Context, groupID string) (string, error)
}

// A webhook can be called when a build is completed.
type Webhook interface {
	NotifyComplete(ctx context.Context, invocation *inpb.Invocation) error
}

// Allows aggregating invocation statistics.
type InvocationStatService interface {
	GetInvocationStat(ctx context.Context, req *inpb.GetInvocationStatRequest) (*inpb.GetInvocationStatResponse, error)
	GetTrend(ctx context.Context, req *stpb.GetTrendRequest) (*stpb.GetTrendResponse, error)
	GetStatHeatmap(ctx context.Context, req *stpb.GetStatHeatmapRequest) (*stpb.GetStatHeatmapResponse, error)
	GetStatDrilldown(ctx context.Context, req *stpb.GetStatDrilldownRequest) (*stpb.GetStatDrilldownResponse, error)
}

// Allows searching invocations.
type InvocationSearchService interface {
	IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error
	QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error)
	GetInvocationFilterSuggestions(ctx context.Context, req *inpb.GetInvocationFilterSuggestionsRequest) (*inpb.GetInvocationFilterSuggestionsResponse, error)
}

type UsageService interface {
	GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error)
}

type UsageTracker interface {
	// Increment adds the given usage counts to the current collection period
	// for the authenticated group ID. It is safe for concurrent access.
	Increment(ctx context.Context, labels *tables.UsageLabels, counts *tables.UsageCounts) error
	StartDBFlush()
	StopDBFlush()
}

type ApiService interface {
	apipb.ApiServiceServer
	GetFileHandler() http.Handler
	GetMetricsHandler() http.Handler
	CacheEnabled() bool
}

type WorkflowService interface {
	CreateWorkflow(ctx context.Context, req *wfpb.CreateWorkflowRequest) (*wfpb.CreateWorkflowResponse, error)
	DeleteWorkflow(ctx context.Context, req *wfpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error)
	GetWorkflows(ctx context.Context) (*wfpb.GetWorkflowsResponse, error)
	GetWorkflowHistory(ctx context.Context) (*wfpb.GetWorkflowHistoryResponse, error)
	ExecuteWorkflow(ctx context.Context, req *wfpb.ExecuteWorkflowRequest) (*wfpb.ExecuteWorkflowResponse, error)
	GetRepos(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error)
	ServeHTTP(w http.ResponseWriter, r *http.Request)

	// HandleRepositoryEvent handles a webhook event corresponding to the given
	// GitRepository by initiating any relevant workflow actions.
	HandleRepositoryEvent(ctx context.Context, repo *tables.GitRepository, wd *WebhookData, accessToken string) error

	// GetLinkedWorkflows returns any workflows linked with the given repo access
	// token.
	GetLinkedWorkflows(ctx context.Context, accessToken string) ([]string, error)

	// WorkflowsPoolName returns the name of the executor pool to use for workflow actions.
	WorkflowsPoolName() string

	// GetLegacyWorkflowIDForGitRepository generates an artificial workflow ID so that legacy Workflow structs
	// can be created from GitRepositories to play nicely with the pre-existing architecture
	// that expects the legacy format
	GetLegacyWorkflowIDForGitRepository(groupID string, repoURL string) string

	// InvalidateAllSnapshotsForRepo invalidates all snapshots for a repo. Any future workflow
	// runs will be executed on a clean runner.
	InvalidateAllSnapshotsForRepo(ctx context.Context, repoURL string) error
}

type WorkspaceService interface {
	GetWorkspace(ctx context.Context, req *wspb.GetWorkspaceRequest) (*wspb.GetWorkspaceResponse, error)
	SaveWorkspace(ctx context.Context, req *wspb.SaveWorkspaceRequest) (*wspb.SaveWorkspaceResponse, error)
	GetWorkspaceDirectory(ctx context.Context, req *wspb.GetWorkspaceDirectoryRequest) (*wspb.GetWorkspaceDirectoryResponse, error)
	GetWorkspaceFile(ctx context.Context, req *wspb.GetWorkspaceFileRequest) (*wspb.GetWorkspaceFileResponse, error)
}

type SnapshotService interface {
	InvalidateSnapshot(ctx context.Context, key *fcpb.SnapshotKey) (string, error)
}

type GitHubApp interface {
	// TODO(bduffany): Add webhook handler and repo management API

	LinkGitHubAppInstallation(context.Context, *ghpb.LinkAppInstallationRequest) (*ghpb.LinkAppInstallationResponse, error)
	GetGitHubAppInstallations(context.Context, *ghpb.GetAppInstallationsRequest) (*ghpb.GetAppInstallationsResponse, error)
	UnlinkGitHubAppInstallation(context.Context, *ghpb.UnlinkAppInstallationRequest) (*ghpb.UnlinkAppInstallationResponse, error)

	GetLinkedGitHubRepos(context.Context) (*ghpb.GetLinkedReposResponse, error)
	LinkGitHubRepo(context.Context, *ghpb.LinkRepoRequest) (*ghpb.LinkRepoResponse, error)
	UnlinkGitHubRepo(context.Context, *ghpb.UnlinkRepoRequest) (*ghpb.UnlinkRepoResponse, error)

	GetAccessibleGitHubRepos(context.Context, *ghpb.GetAccessibleReposRequest) (*ghpb.GetAccessibleReposResponse, error)

	CreateRepo(context.Context, *rppb.CreateRepoRequest) (*rppb.CreateRepoResponse, error)

	// GetInstallationTokenForStatusReportingOnly returns an installation token
	// for the installation associated with the given installation owner (GitHub
	// username or org name). It does not authorize the authenticated group ID,
	// so should be used for status reporting only.
	GetInstallationTokenForStatusReportingOnly(ctx context.Context, owner string) (*github.InstallationToken, error)

	// GetRepositoryInstallationToken returns an installation token for the given
	// GitRepository.
	GetRepositoryInstallationToken(ctx context.Context, repo *tables.GitRepository) (string, error)

	// WebhookHandler returns the GitHub webhook HTTP handler.
	WebhookHandler() http.Handler

	// OAuthHandler returns the OAuth flow HTTP handler.
	OAuthHandler() http.Handler

	// Passthroughs
	GetGithubUserInstallations(ctx context.Context, req *ghpb.GetGithubUserInstallationsRequest) (*ghpb.GetGithubUserInstallationsResponse, error)
	GetGithubUser(ctx context.Context, req *ghpb.GetGithubUserRequest) (*ghpb.GetGithubUserResponse, error)
	GetGithubRepo(ctx context.Context, req *ghpb.GetGithubRepoRequest) (*ghpb.GetGithubRepoResponse, error)
	GetGithubContent(ctx context.Context, req *ghpb.GetGithubContentRequest) (*ghpb.GetGithubContentResponse, error)
	GetGithubTree(ctx context.Context, req *ghpb.GetGithubTreeRequest) (*ghpb.GetGithubTreeResponse, error)
	CreateGithubTree(ctx context.Context, req *ghpb.CreateGithubTreeRequest) (*ghpb.CreateGithubTreeResponse, error)
	GetGithubBlob(ctx context.Context, req *ghpb.GetGithubBlobRequest) (*ghpb.GetGithubBlobResponse, error)
	CreateGithubBlob(ctx context.Context, req *ghpb.CreateGithubBlobRequest) (*ghpb.CreateGithubBlobResponse, error)
	CreateGithubPull(ctx context.Context, req *ghpb.CreateGithubPullRequest) (*ghpb.CreateGithubPullResponse, error)
	MergeGithubPull(ctx context.Context, req *ghpb.MergeGithubPullRequest) (*ghpb.MergeGithubPullResponse, error)
	GetGithubCompare(ctx context.Context, req *ghpb.GetGithubCompareRequest) (*ghpb.GetGithubCompareResponse, error)
	GetGithubForks(ctx context.Context, req *ghpb.GetGithubForksRequest) (*ghpb.GetGithubForksResponse, error)
	CreateGithubFork(ctx context.Context, req *ghpb.CreateGithubForkRequest) (*ghpb.CreateGithubForkResponse, error)
	GetGithubCommits(ctx context.Context, req *ghpb.GetGithubCommitsRequest) (*ghpb.GetGithubCommitsResponse, error)
	CreateGithubCommit(ctx context.Context, req *ghpb.CreateGithubCommitRequest) (*ghpb.CreateGithubCommitResponse, error)
	UpdateGithubRef(ctx context.Context, req *ghpb.UpdateGithubRefRequest) (*ghpb.UpdateGithubRefResponse, error)
	CreateGithubRef(ctx context.Context, req *ghpb.CreateGithubRefRequest) (*ghpb.CreateGithubRefResponse, error)
	GetGithubPullRequest(ctx context.Context, req *ghpb.GetGithubPullRequestRequest) (*ghpb.GetGithubPullRequestResponse, error)
	GetGithubPullRequestDetails(ctx context.Context, req *ghpb.GetGithubPullRequestDetailsRequest) (*ghpb.GetGithubPullRequestDetailsResponse, error)
	CreateGithubPullRequestComment(ctx context.Context, req *ghpb.CreateGithubPullRequestCommentRequest) (*ghpb.CreateGithubPullRequestCommentResponse, error)
	UpdateGithubPullRequestComment(ctx context.Context, req *ghpb.UpdateGithubPullRequestCommentRequest) (*ghpb.UpdateGithubPullRequestCommentResponse, error)
	DeleteGithubPullRequestComment(ctx context.Context, req *ghpb.DeleteGithubPullRequestCommentRequest) (*ghpb.DeleteGithubPullRequestCommentResponse, error)
	SendGithubPullRequestReview(ctx context.Context, req *ghpb.SendGithubPullRequestReviewRequest) (*ghpb.SendGithubPullRequestReviewResponse, error)
}

type RunnerService interface {
	Run(ctx context.Context, req *rnpb.RunRequest) (*rnpb.RunResponse, error)
}

type GCPService interface {
	Link(w http.ResponseWriter, r *http.Request) error
	GetGCPProject(ctx context.Context, request *gcpb.GetGCPProjectRequest) (*gcpb.GetGCPProjectResponse, error)
}

type GitProviders []GitProvider

type GitProvider interface {
	// MatchRepoURL returns whether a given repo URL should be handled by this
	// provider. If multiple providers match, the first one in the GitProviders
	// list is chosen to handle the repo URL.
	MatchRepoURL(u *url.URL) bool

	// MatchWebhookRequest returns whether a given webhook request was sent by
	// this git provider. If multiple providers match, the first one in the
	// GitProviders list is chosen to handle the webhook.
	MatchWebhookRequest(req *http.Request) bool

	// ParseWebhookData parses webhook data from the given HTTP request sent to
	// a webhook endpoint. It should only be called if MatchWebhookRequest returns
	// true.
	ParseWebhookData(req *http.Request) (*WebhookData, error)

	// RegisterWebhook registers the given webhook URL to listen for push and
	// pull request (also called "merge request") events.
	RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error)

	// UnregisterWebhook unregisters the webhook with the given ID from the repo.
	UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error

	// GetFileContents fetches a single file's contents from the repo. It should
	// return status.NotFoundError if the file does not exist. It should NOT
	// return status.NotFoundError in any other case, particularly if the repo
	// doesn't exist or is inaccessible.
	GetFileContents(ctx context.Context, accessToken, repoURL, filePath, ref string) ([]byte, error)

	// IsTrusted returns whether the given user is a trusted collaborator on the
	// repository.
	IsTrusted(ctx context.Context, accessToken, repoURL, user string) (bool, error)

	// CreateStatus publishes a status payload to the given repo at the given
	// commit SHA.
	CreateStatus(ctx context.Context, accessToken, repoURL, commitSHA string, payload any) error

	// TODO(bduffany): ListRepos
}

// WebhookData represents the data extracted from a Webhook event.
type WebhookData struct {
	// EventName is the canonical event name that this data was created from.
	EventName string

	// PushedRepoURL is the canonical URL of the repo containing the pushed branch.
	// For pull request events from forked repos, this is the URL of the forked repo.
	// For other events, this is the repo URL used.
	// Ex: "https://github.com/some-untrusted-user/acme-fork"
	PushedRepoURL string

	// PushedBranch is the name of the branch in the pushed repo that triggered
	// the event when pushed.
	// Ex: "my-cool-feature"
	PushedBranch string

	// SHA is the commit SHA of the branch that was pushed.
	SHA string

	// TargetRepoURL is the canonical URL of the repo containing the TargetBranch.
	// For non-pull request events, this can be empty, or can equal `PushedRepoURL`.
	// Ex: "https://github.com/acme-inc/acme"
	TargetRepoURL string

	// TargetRepoDefaultBranch is the default / main branch of the target repo.
	// The default branch can be configured in the repo settings on GitHub.
	// Ex: "main"
	TargetRepoDefaultBranch string

	// TargetBranch is the base branch into which the PR branch is being merged.
	// For non-pull request events, this can be empty, or can equal `PushedBranch`.
	// Ex: "main"
	TargetBranch string

	// IsTargetRepoPublic reflects whether the target repo is publicly visible via
	// the git provider.
	IsTargetRepoPublic bool

	// PullRequestNumber is the PR number if applicable.
	// Ex: 123
	PullRequestNumber int64

	// PullRequestAuthor is the user name of the author of the pull request,
	// if applicable.
	// Ex: "externaldev123"
	PullRequestAuthor string

	// PullRequestApprover is the user name of a user that approved the pull
	// request, if applicable.
	// Ex: "acmedev123"
	PullRequestApprover string
}

type SplashPrinter interface {
	PrintSplashScreen(hostname string, port, grpcPort int)
}

type RemoteExecutionService interface {
	Dispatch(ctx context.Context, req *repb.ExecuteRequest) (string, error)
	Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error
	WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error
	PublishOperation(stream repb.Execution_PublishOperationServer) error
	MarkExecutionFailed(ctx context.Context, taskID string, reason error) error
	Cancel(ctx context.Context, invocationID string) error
	RedisAvailabilityMonitoringEnabled() bool
}

type FileCache interface {
	FastLinkFile(ctx context.Context, f *repb.FileNode, outputPath string) bool
	DeleteFile(ctx context.Context, f *repb.FileNode) bool
	AddFile(ctx context.Context, f *repb.FileNode, existingFilePath string) error
	ContainsFile(ctx context.Context, node *repb.FileNode) bool
	WaitForDirectoryScanToComplete()

	Read(ctx context.Context, node *repb.FileNode) ([]byte, error)
	Write(ctx context.Context, node *repb.FileNode, b []byte) (n int, err error)

	// TempDir returns a directory that is guaranteed to be on the same device
	// as the filecache. The directory is not unique per call. Callers should
	// generate globally unique file names under this directory.
	TempDir() string
}

// PoolType represents the user's requested executor pool type for an executed
// action.
type PoolType int

const (
	PoolTypeDefault    PoolType = 1 // Respect org preference.
	PoolTypeShared     PoolType = 2 // Use shared executors.
	PoolTypeSelfHosted PoolType = 3 // Use self-hosted executors.
)

type SchedulerService interface {
	RegisterAndStreamWork(stream scpb.Scheduler_RegisterAndStreamWorkServer) error
	LeaseTask(stream scpb.Scheduler_LeaseTaskServer) error
	ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error)
	CancelTask(ctx context.Context, taskID string) (bool, error)
	ExistsTask(ctx context.Context, taskID string) (bool, error)
	EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error)
	ReEnqueueTask(ctx context.Context, req *scpb.ReEnqueueTaskRequest) (*scpb.ReEnqueueTaskResponse, error)
	GetExecutionNodes(ctx context.Context, req *scpb.GetExecutionNodesRequest) (*scpb.GetExecutionNodesResponse, error)
	GetPoolInfo(ctx context.Context, os, requestedPool, workflowID string, poolType PoolType) (*PoolInfo, error)
}

// PoolInfo holds high level metadata for an executor pool.
type PoolInfo struct {
	// GroupID is the group that owns the executor. This will be set even for
	// shared executors, in which case it will be the shared owner group ID.
	GroupID string

	// Name is the pool name.
	Name string

	// IsSelfHosted is whether the pool consists of self-hosted executors.
	IsSelfHosted bool

	// True if the GroupID corresponds to the shared executor group ID.
	IsShared bool
}

type ExecutionService interface {
	GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error)
	WaitExecution(req *espb.WaitExecutionRequest, stream bbspb.BuildBuddyService_WaitExecutionServer) error
	WriteExecutionProfile(ctx context.Context, w io.Writer, executionID string) error
}

// An ExecutionNode that has been ranked for scheduling.
type RankedExecutionNode interface {
	GetExecutionNode() ExecutionNode

	// Returns whether or not this node is "preferred" in ranking for the
	// provided execution.
	IsPreferred() bool
}

type ExecutionNode interface {
	// GetExecutorId returns the ID for this execution node that uniquely
	// identifies it within a node pool.
	GetExecutorId() string

	// GetExecutorHostId returns the executor host ID for this execution node,
	// which persists across restarts of a given executor instance.
	GetExecutorHostId() string

	GetAssignableMilliCpu() int64
}

type ExecutionSearchService interface {
	SearchExecutions(ctx context.Context, req *espb.SearchExecutionRequest) (*espb.SearchExecutionResponse, error)
}

// TaskRouter decides which execution nodes should execute a task.
//
// Routing is namespaced by group ID (extracted from context) and remote instance
// name. Tasks with different namespaces are not guaranteed to be routed the same.
//
// It is the caller's responsibility to check whether any execution nodes
// passed via parameters are accessible by the authenticated group in the context.
type TaskRouter interface {
	// RankNodes returns a slice of the given nodes sorted in decreasing order of
	// their suitability for executing the given command. Nodes with equal
	// suitability are returned in random order (for load balancing purposes).
	//
	// If an error occurs, the input nodes should be returned in random order.
	RankNodes(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName string, nodes []ExecutionNode) []RankedExecutionNode

	// MarkComplete notifies the router that the command has been completed by the
	// given executor instance. Subsequent calls to RankNodes may assign a higher
	// rank to nodes with the given instance ID, given similar commands.
	MarkComplete(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName, executorInstanceID string)
}

// TaskSizer allows storing, retrieving, and predicting task size measurements for a task.
type TaskSizer interface {
	// Get returns the previously measured size for a task, or nil if this data
	// is not available.
	Get(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize

	// Predict returns a predicted task size using a model, or nil if no model
	// is configured.
	Predict(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize

	// Update records a measured task size.
	Update(ctx context.Context, action *repb.Action, cmd *repb.Command, md *repb.ExecutedActionMetadata) error
}

// ScheduledTask represents an execution task along with its scheduling metadata
// computed by the execution service.
type ScheduledTask struct {
	ExecutionTask      *repb.ExecutionTask
	SchedulingMetadata *scpb.SchedulingMetadata
}

// Runner represents an isolated execution environment.
//
// Runners are assigned a single task when they are retrieved from a Pool,
// and may only be used to execute that task. When using runner recycling,
// runners can be added back to the pool, then later retrieved from the pool
// to execute a new task.
type Runner interface {
	// PrepareForTask prepares the filesystem for the task assigned to the runner,
	// downloading the task's container image if applicable and cleaning up the
	// workspace state from the previously assigned task if applicable.
	PrepareForTask(ctx context.Context) error

	// DownloadInputs downloads any input files associated with the task assigned
	// to the runner.
	//
	// It populates the download stat fields in the given IOStats.
	DownloadInputs(ctx context.Context, ioStats *repb.IOStats) error

	// Run runs the task that is currently assigned to the runner.
	Run(ctx context.Context) *CommandResult

	// GracefulTerminate sends a graceful termination signal to the runner.
	GracefulTerminate(ctx context.Context) error

	// UploadOutputs uploads any output files and auxiliary logs associated with
	// the task assigned to the runner, as well as the result of the run.
	//
	// It populates the upload stat fields in the given IOStats.
	UploadOutputs(ctx context.Context, ioStats *repb.IOStats, executeResponse *repb.ExecuteResponse, cr *CommandResult) error

	// GetIsolationType returns the runner's effective isolation type as a
	// string, such as "none" or "podman".
	GetIsolationType() string
}

// Pool is responsible for assigning tasks to runners.
//
// Pool keeps track of paused runners and active runners, allowing incoming
// tasks to be assigned to paused runners (if runner recycling is enabled), as
// well as evicting paused runners to free up resources for new runners.
type RunnerPool interface {
	// Warmup prepares any resources needed for commonly used execution
	// environments, such as downloading container images.
	Warmup(ctx context.Context)

	// Get returns a runner bound to the the given task. The caller must call
	// TryRecycle on the returned runner when done using it.
	//
	// If the task has runner recycling enabled then it attempts to find a runner
	// from the pool that can execute the task. If runner recycling is disabled or
	// if there are no eligible paused runners, it creates and returns a new
	// runner.
	//
	// The returned runner is ready to execute tasks, and the caller is
	// responsible for walking the runner through the task lifecycle.
	Get(ctx context.Context, task *repb.ScheduledTask) (Runner, error)

	// TryRecycle attempts to add the runner to the pool for use by subsequent
	// tasks.
	//
	// If recycling is not enabled or if an error occurred while attempting to
	// recycle, the runner is not added to the pool and any resources associated
	// with the runner are freed up. Callers should never use the provided runner
	// after calling TryRecycle, since it may possibly be removed.
	//
	// If the runner did not finish cleanly (as determined by the caller of
	// runner.Run()) then it will not be recycled and instead forcibly removed,
	// even if runner recycling is enabled.
	TryRecycle(ctx context.Context, r Runner, finishedCleanly bool)

	// Shutdown removes all runners from the pool.
	Shutdown(ctx context.Context) error

	// Wait waits for all background cleanup jobs to complete. This is intended to
	// be called during shutdown, after all tasks have finished executing (to ensure
	// that no new cleanup jobs will be needed after this returns).
	Wait()

	// Returns the build root directory for this pool.
	GetBuildRoot() string
}

// CommandRunner is an interface for running command-line commands.
type CommandRunner interface {
	Run(ctx context.Context, command *repb.Command, workDir string, statsListener func(*repb.UsageStats), stdio *Stdio) *CommandResult
}

// Stdio specifies standard input / output readers for a command.
type Stdio struct {
	// Stdin is an optional stdin source for the executed process.
	Stdin io.Reader
	// Stdout is an optional stdout sink for the executed process.
	Stdout io.Writer
	// Stderr is an optional stderr sink for the executed process.
	Stderr io.Writer
}

// CommandResult captures the output and details of an executed command.
type CommandResult struct {
	// Error is populated only if the command was unable to be started, or if it was
	// started but never completed.
	//
	// In particular, if the command runs and returns a non-zero exit code (such as 1),
	// this is considered a successful execution, and this error will NOT be populated.
	//
	// In some cases, the command may have failed to start due to an issue unrelated
	// to the command itself. For example, the runner may execute the command in a
	// sandboxed environment but fail to create the sandbox. In these cases, the
	// Error field here should be populated with a gRPC error code indicating why the
	// command failed to start, and the ExitCode field should contain the exit code
	// from the sandboxing process, rather than the command itself.
	//
	// If the call to `exec.Cmd#Run` returned -1, meaning that the command was killed or
	// never exited, this field should be populated with a gRPC error code indicating the
	// reason, such as DEADLINE_EXCEEDED (if the command times out), UNAVAILABLE (if
	// there is a transient error that can be retried), or RESOURCE_EXHAUSTED (if the
	// command ran out of memory while executing).
	Error error
	// CommandDebugString indicates the command that was run, for debugging purposes only.
	CommandDebugString string
	// Stdout from the command. This may contain data even if there was an Error.
	Stdout []byte
	// Stderr from the command. This may contain data even if there was an Error.
	Stderr []byte
	// AuxiliaryLogs contain extra logs associated with the task that may be
	// useful to present to the user.
	AuxiliaryLogs map[string][]byte

	// DoNotRecycle indicates that a runner should not be reused after this
	// command has executed.
	DoNotRecycle bool

	// ExitCode is one of the following:
	// * The exit code returned by the executed command
	// * -1 if the process was killed or did not exit
	// * -2 (NoExitCode) if the exit code could not be determined because it returned
	//   an error other than exec.ExitError. This case typically means it failed to start.
	ExitCode int

	// UsageStats holds the command's measured resource usage. It may be nil if
	// resource measurement is not implemented by the command's isolation type.
	UsageStats *repb.UsageStats

	// VMMetadata associated with the VM that ran the task, if applicable.
	VMMetadata *fcpb.VMMetadata
}

type Subscriber interface {
	Close() error
	Chan() <-chan string
}

// A PubSub allows for sending messages between distributed (cross process,
// cross machine) processes. This may be implemented by a cloud-pubsub service,
// or something like redis.
type PubSub interface {
	Publish(ctx context.Context, channelName string, message string) error
	Subscribe(ctx context.Context, channelName string) Subscriber
}

// A MetricsCollector allows for storing ephemeral values globally.
//
// No guarantees are made about durability of MetricsCollectors -- they may be
// evicted from the backing store that maintains them (usually memcache or
// redis), so they should *not* be used for data that requires durability.
type MetricsCollector interface {
	IncrementCountsWithExpiry(ctx context.Context, key string, counts map[string]int64, expiry time.Duration) error
	IncrementCounts(ctx context.Context, key string, counts map[string]int64) error
	IncrementCountWithExpiry(ctx context.Context, key, field string, n int64, expiry time.Duration) error
	IncrementCount(ctx context.Context, key, field string, n int64) error
	SetAddWithExpiry(ctx context.Context, key string, expiry time.Duration, members ...string) error
	SetAdd(ctx context.Context, key string, members ...string) error
	SetGetMembers(ctx context.Context, key string) ([]string, error)
	Set(ctx context.Context, key, value string, expiration time.Duration) error
	GetAll(ctx context.Context, key ...string) ([]string, error)
	ListAppend(ctx context.Context, key string, values ...string) error
	ListRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	ReadCounts(ctx context.Context, key string) (map[string]int64, error)
	Delete(ctx context.Context, key string) error
	Expire(ctx context.Context, key string, duration time.Duration) error
	Flush(ctx context.Context) error
}

// A KeyValStore allows for storing ephemeral values globally.
//
// No guarantees are made about durability of KeyValStores -- they may be
// evicted from the backing store that maintains them (usually memcache or
// redis), so they should *not* be used in critical path code.
type KeyValStore interface {
	Set(ctx context.Context, key string, val []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// A RepoDownloader allows testing a git-repo to see if it's downloadable.
type RepoDownloader interface {
	TestRepoAccess(ctx context.Context, repoURL, username, accessToken string) error
}

type Checker interface {
	// Returns nil on success, error on failure. Returning an error will
	// indicate to the health checker that this service is unhealthy and
	// that the server is not ready to serve.
	Check(ctx context.Context) error
}
type CheckerFunc func(ctx context.Context) error

func (f CheckerFunc) Check(ctx context.Context) error {
	return f(ctx)
}

type HealthChecker interface {
	// AddHealthCheck adds a healthcheck -- the server's readiness is dependent on all
	// registered heathchecks passing.
	AddHealthCheck(name string, hc Checker)

	// RegisterShutdownFunction registers a function that will be called
	// when the server shuts down. This can be used for finalizing any
	// short work and freeing up resources. A CheckerFunc may block
	// shutdown for up to ~30 seconds or so, at which point the server
	// will terminate un-gracefully.
	RegisterShutdownFunction(hc CheckerFunc)

	// WaitForGracefulShutdown should be called as the last thing in a
	// main function -- it will block forever until a server receives a
	// shutdown signal.
	WaitForGracefulShutdown()

	// LivenessHandler returns "OK" as soon as the server is alive.
	LivenessHandler() http.Handler

	// ReadinessHandler returns "OK" when the server is ready to serve.
	// If a HealthCheck returns failure for some reason, the server will
	// stop returning OK and will instead return Service Unavailable error.
	ReadinessHandler() http.Handler

	// Shutdown initiates a shutdown of the server.
	// This is intended to be used by tests as normally shutdown is automatically initiated upon receipt of a SIGTERM
	// signal.
	Shutdown()

	// Implements the proto healthcheck interface.
	Check(ctx context.Context, req *hlpb.HealthCheckRequest) (*hlpb.HealthCheckResponse, error)
	Watch(req *hlpb.HealthCheckRequest, stream hlpb.Health_WatchServer) error
}

// Locates all Xcode versions installed on the host system.
type XcodeLocator interface {
	// Finds the Xcode that matches the given Xcode version.
	// Returns the developer directory for that Xcode and the SDK root for the given SDK.
	PathsForVersionAndSDK(xcodeVersion string, sdk string) (string, string, error)
}

// LRU implements a Least Recently Used cache.
type LRU[V any] interface {
	// Inserts a value into the LRU. A boolean is returned that indicates
	// if the value was successfully added.
	Add(key string, value V) bool

	// Inserts a value into the back of the LRU. A boolean is returned that
	// indicates if the value was successfully added.
	PushBack(key string, value V) bool

	// Gets a value from the LRU, returns a boolean indicating if the value
	// was present.
	Get(key string) (V, bool)

	// Returns a boolean indicating if the value is present in the LRU.
	Contains(key string) bool

	// Removes a value from the LRU, releasing resources associated with
	// that value. Returns a boolean indicating if the value was sucessfully
	// removed.
	Remove(key string) bool

	// Purge Remove()s all items in the LRU.
	Purge()

	// Returns the total "size" of the LRU.
	Size() int64

	// Returns the number of items in the LRU.
	Len() int

	// Remove()s the oldest value in the LRU. (See Remove() above).
	RemoveOldest() (V, bool)

	// Returns metrics about the status of the LRU.
	Metrics() string
}

// DistributedLock provides a way to serialize access to a resource, where the
// accessors may be running on different nodes.
//
// Implementations should expire the lock after an appropriate length of time to
// ensure that the lock will eventually be released even if this node goes down.
type DistributedLock interface {
	// Lock attempts to acquire the lock.
	//
	// Implementations may return ResourceExhaustedError if and only if the
	// lock is already held.
	Lock(ctx context.Context) error

	// Unlock releases the lock.
	Unlock(ctx context.Context) error
}

// QuotaManager manages quota.
type QuotaManager interface {
	// Allow checks whether a user (identified from the ctx) has exceeded a rate
	// limit inside the namespace.
	// If the rate limit has not been exceeded, the underlying storage is updated
	// by the supplied quantity.
	Allow(ctx context.Context, namespace string, quantity int64) (bool, error)

	GetNamespace(ctx context.Context, req *qpb.GetNamespaceRequest) (*qpb.GetNamespaceResponse, error)
	RemoveNamespace(ctx context.Context, req *qpb.RemoveNamespaceRequest) (*qpb.RemoveNamespaceResponse, error)
	ApplyBucket(ctx context.Context, req *qpb.ApplyBucketRequest) (*qpb.ApplyBucketResponse, error)
	ModifyNamespace(ctx context.Context, req *qpb.ModifyNamespaceRequest) (*qpb.ModifyNamespaceResponse, error)
}

// A Metadater implements the Metadata() method and returns a StorageMetadata
// proto representing the stored data. The result is only valid if Close has
// already been called. This is only used with MetadataWriteCloser.
type Metadater interface {
	Metadata() *rfpb.StorageMetadata
}

// A Committer implements the Commit method, to finalize a write.
// If this returns successfully, the caller is assured that the data has
// been successfully written to disk and any metadata stores.
// This is only used with CommittedMetadataWriteCloser.
type Committer interface {
	Commit() error
}

type CommittedWriteCloser interface {
	io.Writer
	io.Closer
	Committer
}

type MetadataWriteCloser interface {
	io.Writer
	io.Closer
	Metadater
}

type CommittedMetadataWriteCloser interface {
	io.Writer
	io.Closer
	Metadater
	Committer
}

// This interface is copied from tink:
// AEAD is the interface for authenticated encryption with associated data.
type AEAD interface {
	// Encrypt encrypts plaintext with associatedData as associated data.
	// The resulting ciphertext allows for checking authenticity and
	// integrity of associated data associatedData, but does not guarantee
	// its secrecy.
	Encrypt(plaintext, associatedData []byte) ([]byte, error)

	// Decrypt decrypts ciphertext with associatedData as associated data.
	// The decryption verifies the authenticity and integrity of the
	// associated data, but there are no guarantees with respect to secrecy
	// of that data.
	Decrypt(ciphertext, associatedData []byte) ([]byte, error)
}

type KMSType int

const (
	KMSTypeLocalInsecure KMSType = iota
	KMSTypeGCP
	KMSTypeAWS
)

// A KMS is a Key Managment Service (typically a cloud provider or external
// service) that manages keys that can be fetched and used to encrypt/decrypt
// data.
type KMS interface {
	FetchMasterKey() (AEAD, error)
	FetchKey(uri string) (AEAD, error)

	SupportedTypes() []KMSType
}

// SecretService manages secrets for an org.
type SecretService interface {
	GetPublicKey(ctx context.Context, req *skpb.GetPublicKeyRequest) (*skpb.GetPublicKeyResponse, error)
	ListSecrets(ctx context.Context, req *skpb.ListSecretsRequest) (*skpb.ListSecretsResponse, error)
	UpdateSecret(ctx context.Context, req *skpb.UpdateSecretRequest) (*skpb.UpdateSecretResponse, bool, error)
	DeleteSecret(ctx context.Context, req *skpb.DeleteSecretRequest) (*skpb.DeleteSecretResponse, error)

	// Internal use only -- fetches decoded secrets for use in running a command.
	GetSecretEnvVars(ctx context.Context, groupID string) ([]*repb.Command_EnvironmentVariable, error)
}

// ExecutionCollector keeps track of a list of Executions for each invocation ID.
type ExecutionCollector interface {
	AppendExecution(ctx context.Context, iid string, execution *repb.StoredExecution) error
	// GetExecutions fetches a range of executions for the given invocation ID.
	// The range start and stop indexes are both inclusive. If the stop index is out
	// of range, then the returned slice will contain as many executions are
	// available starting from the start index.
	GetExecutions(ctx context.Context, iid string, start, stop int64) ([]*repb.StoredExecution, error)
	DeleteExecutions(ctx context.Context, iid string) error
	AddInvocation(ctx context.Context, inv *sipb.StoredInvocation) error
	GetInvocation(ctx context.Context, iid string) (*sipb.StoredInvocation, error)
	AddInvocationLink(ctx context.Context, link *sipb.StoredInvocationLink) error
	GetInvocationLinks(ctx context.Context, execution_id string) ([]*sipb.StoredInvocationLink, error)
	DeleteInvocationLinks(ctx context.Context, execution_id string) error
}

// SuggestionService enables fetching of suggestions.
type SuggestionService interface {
	GetSuggestion(ctx context.Context, req *supb.GetSuggestionRequest) (*supb.GetSuggestionResponse, error)
	MultipleProvidersConfigured() bool
}

type Encryptor interface {
	CommittedWriteCloser
	Metadata() *rfpb.EncryptionMetadata
}

type Decryptor interface {
	io.ReadCloser
}

type Crypter interface {
	SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error)
	GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error)

	ActiveKey(ctx context.Context) (*rfpb.EncryptionMetadata, error)

	NewEncryptor(ctx context.Context, d *repb.Digest, w CommittedWriteCloser) (Encryptor, error)
	NewDecryptor(ctx context.Context, d *repb.Digest, r io.ReadCloser, em *rfpb.EncryptionMetadata) (Decryptor, error)
}

// Provides a duplicate function call suppression mechanism, just like the
// singleflight package.
type SingleFlightDeduper interface {
	Do(ctx context.Context, key string, work func() ([]byte, error)) ([]byte, error)
}

type PromQuerier interface {
	FetchMetrics(ctx context.Context, groupID string) ([]*dto.MetricFamily, error)
}

// ConfigSecretProvider provides secrets interpolation into configs.
type ConfigSecretProvider interface {
	GetSecret(ctx context.Context, name string) ([]byte, error)
}

type AuditLogger interface {
	Log(ctx context.Context, resource *alpb.ResourceID, action alpb.Action, request proto.Message)
	LogForGroup(ctx context.Context, groupID string, action alpb.Action, request proto.Message)
	LogForInvocation(ctx context.Context, invocationID string, action alpb.Action, request proto.Message)
	LogForSecret(ctx context.Context, secretName string, action alpb.Action, request proto.Message)
	GetLogs(ctx context.Context, req *alpb.GetAuditLogsRequest) (*alpb.GetAuditLogsResponse, error)
}

type IPRulesService interface {
	// Authorize checks whether the authenticated user in the context is allowed
	// to access the group identified in the context.
	Authorize(ctx context.Context) error

	// AuthorizeGroup checks whether the authenticated user in the context is
	// allowed to access the specified groupId. This function should not be used
	// in performance sensitive code paths.
	AuthorizeGroup(ctx context.Context, groupID string) error

	// AuthorizeHTTPRequest checks whether the specified HTTP request should be
	// allowed based on the authenticated user and group information in the
	// context.
	AuthorizeHTTPRequest(ctx context.Context, r *http.Request) error

	GetRule(ctx context.Context, groupID string, ruleID string) (*tables.IPRule, error)

	GetIPRuleConfig(ctx context.Context, request *irpb.GetRulesConfigRequest) (*irpb.GetRulesConfigResponse, error)
	SetIPRuleConfig(ctx context.Context, request *irpb.SetRulesConfigRequest) (*irpb.SetRulesConfigResponse, error)
	GetRules(ctx context.Context, req *irpb.GetRulesRequest) (*irpb.GetRulesResponse, error)
	AddRule(ctx context.Context, req *irpb.AddRuleRequest) (*irpb.AddRuleResponse, error)
	UpdateRule(ctx context.Context, req *irpb.UpdateRuleRequest) (*irpb.UpdateRuleResponse, error)
	DeleteRule(ctx context.Context, req *irpb.DeleteRuleRequest) (*irpb.DeleteRuleResponse, error)
}

type ClientIdentity struct {
	Origin string
	Client string
}

const (
	ClientIdentityExecutor       = "executor"
	ClientIdentityApp            = "app"
	ClientIdentityWorkflow       = "workflow"
	ClientIdentityInternalOrigin = "internal"
)

type ClientIdentityService interface {
	// AddIdentityToContext adds the identity of the current client to the
	// outgoing context.
	AddIdentityToContext(ctx context.Context) (context.Context, error)

	// IdentityHeader generates a signed header value for the specified
	// identity.
	IdentityHeader(si *ClientIdentity, expiration time.Duration) (string, error)

	// ValidateIncomingIdentity validates the incoming identity and adds the
	// authenticated identity information to the context. This function is
	// called automatically via an interceptor.
	ValidateIncomingIdentity(ctx context.Context) (context.Context, error)

	// IdentityFromContext returns the previously-validated context information
	// from the context. Returns a NotFound error if the context does not
	// contain validated server identity.
	IdentityFromContext(ctx context.Context) (*ClientIdentity, error)
}

// ImageCacheToken is a claim to be able to access a locally cached image.
type ImageCacheToken struct {
	// GroupID is the authenticated group ID.
	GroupID string
	// ImageRef is the remote image ref, e.g. "gcr.io/foo/bar:v1.0"
	ImageRef string
}

// ImageCacheAuthenticator validates access to locally cached images.
type ImageCacheAuthenticator interface {
	// IsAuthorized returns whether the given token is valid. If this returns
	// false, the image must be revalidated with the remote registry, and then
	// Refresh should be called in order to mark the token valid.
	IsAuthorized(token ImageCacheToken) bool

	// Refresh extends the TTL of the given token before it must be
	// re-authenticated with a remote registry.
	Refresh(token ImageCacheToken)
}

// Store models a block-level storage system, which is useful as a backend
type Store interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Sync() error
	SizeBytes() (int64, error)
}

// StoreReader returns an io.Reader that reads all bytes from the given store,
// starting at offset 0 and ending at SizeBytes.
func StoreReader(store Store) (io.Reader, error) {
	size, err := store.SizeBytes()
	if err != nil {
		return nil, err
	}
	return io.NewSectionReader(store, 0, size), nil
}

// ServerNotificationService provides a best-effort pubsub-style notification
// service for broadcasting information to other servers within the same
// service.
type ServerNotificationService interface {
	// Subscribe listens for notifications of the specified type. The specified
	// proto must be one of the fields defined on the Notification proto in
	// service_notification.proto
	//
	// e.g. subscribing to InvalidateIPRulesCache notifications can be done as:
	// service.Subscribe(&snpb.InvalidateIPRulesCache{})
	Subscribe(msgType proto.Message) <-chan proto.Message

	// Publish broadcasts a notification to all the servers in the service. The
	// specified proto msut be one of the fields defined on the Notification
	// proto in service_notification.proto
	// e.g. publishing an InvalidateIPRulesCache notification can be done as:
	// service.Publish(ctx, &snpb.InvalidateIPRulesCache{GroupID: "123"})
	Publish(ctx context.Context, msg proto.Message) error
}

type SCIMService interface {
	RegisterHandlers(mux HttpServeMux)
}

type GossipListener interface {
	OnEvent(eventType serf.EventType, event serf.Event)
}

type GossipService interface {
	ListenAddr() string
	JoinList() []string
	AddListener(listener GossipListener)
	LocalMember() serf.Member
	Members() []serf.Member
	SetTags(tags map[string]string) error
	SendUserEvent(name string, payload []byte, coalesce bool) error
	Query(name string, payload []byte, params *serf.QueryParam) (*serf.QueryResponse, error)
	Statusz(ctx context.Context) string
	Leave() error
	Shutdown() error
}

type CodesearchService interface {
	Search(ctx context.Context, req *cssrpb.SearchRequest) (*cssrpb.SearchResponse, error)
	Index(ctx context.Context, req *csinpb.IndexRequest) (*csinpb.IndexResponse, error)
	IngestAnnotations(ctx context.Context, req *csinpb.IngestAnnotationsRequest) (*csinpb.IngestAnnotationsResponse, error)
	KytheProxy(ctx context.Context, req *cssrpb.KytheRequest) (*cssrpb.KytheResponse, error)
}

type AuthService interface {
	Authenticate(ctx context.Context, req *authpb.AuthenticateRequest) (*authpb.AuthenticateResponse, error)
}

type RegistryService interface {
	RegisterHandlers(mux HttpServeMux)
}

type AtimeUpdater interface {
	Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value)
	EnqueueByResourceName(ctx context.Context, downloadString string)
	EnqueueByFindMissingRequest(ctx context.Context, req *repb.FindMissingBlobsRequest)
}
