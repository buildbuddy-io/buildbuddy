// Package tables defines schema and utility functions for the transactional SQL
// database.
package tables

import (
	"flag"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"gorm.io/gorm"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

var (
	dropInvocationPKCol = flag.Bool("drop_invocation_pk_cols", false, "If true, attempt to drop invocation PK cols")
)

const (
	sqliteDialect   = "sqlite"
	mysqlDialect    = "mysql"
	postgresDialect = "postgres"
)

type tableDescriptor struct {
	table interface{}
	// 2-letter table prefix
	prefix string
	// Table name (must match struct name).
	name string
}

// A Table must define the table name.
type Table interface {
	TableName() string
}

// All tables the DB knows about. If it's not here it doesn't count.
var (
	allTables []tableDescriptor
)

func GetAllTables() []interface{} {
	tableSlice := make([]interface{}, 0)
	for _, d := range allTables {
		tableSlice = append(tableSlice, d.table)
	}
	return tableSlice
}

func PrimaryKeyForTable(tableName string) (string, error) {
	for _, d := range allTables {
		if d.name == tableName {
			return fmt.Sprintf("%s%d", d.prefix, random.RandUint64()), nil
		}
	}
	return "", fmt.Errorf("unknown table: %s", tableName)
}

func registerTable(prefix string, t Table) {
	// TODO: check pk is defined.
	// TODO: check model is included.
	allTables = append(allTables, tableDescriptor{
		table:  t,
		prefix: prefix,
		name:   t.TableName(),
	})
}

// Making a new table? Please make sure you:
//   1) Include "model" (above) as an anonymous first member in your new table struct
//   2) Define a primary key value in your new table struct
//   3) Register your new table struct with the register function in init() below.

// NB: gorm can only work on exported fields.

type Model struct {
	CreatedAtUsec int64
	UpdatedAtUsec int64
}

// Timestamps are hard and differing sql implementations do... a lot. Too much.
// So, we handle this in go-code and set as the timestamp in microseconds.
func (m *Model) BeforeCreate(tx *gorm.DB) (err error) {
	nowUsec := tx.Config.NowFunc().UnixMicro()
	m.CreatedAtUsec = nowUsec
	m.UpdatedAtUsec = nowUsec
	return nil
}

func (m *Model) BeforeUpdate(tx *gorm.DB) (err error) {
	m.UpdatedAtUsec = tx.Config.NowFunc().UnixMicro()
	return nil
}

type Invocation struct {
	Role         string `gorm:"index:role_index"`
	InvocationID string `gorm:"primaryKey;"`
	UserID       string `gorm:"index:user_id"`
	GroupID      string `gorm:"index:group_id"`
	BlobID       string
	Pattern      string `gorm:"type:text;"`
	User         string `gorm:"index:user_index"`
	Command      string
	Host         string `gorm:"index:host_index"`
	RepoURL      string `gorm:"index:repo_url_index"`
	CommitSHA    string `gorm:"index:commit_sha_index"`
	LastChunkId  string
	BranchName   string `gorm:"index:branch_name_index"`
	Model
	DurationUsec                   int64
	UploadThroughputBytesPerSecond int64
	ActionCount                    int64
	Perms                          int32 `gorm:"index:perms;default:NULL"`
	CreatedWithCapabilities        int32
	RedactionFlags                 int32 `gorm:"default:NULL"`
	InvocationStatus               int64 `gorm:"index:invocation_status_idx"`
	ActionCacheHits                int64
	ActionCacheMisses              int64
	ActionCacheUploads             int64
	CasCacheHits                   int64
	CasCacheMisses                 int64
	CasCacheUploads                int64

	// TotalDownloadSizeBytes is the sum of digest sizes for all cache download
	// requests made by this invocation.
	TotalDownloadSizeBytes int64

	// TotalUploadSizeBytes is the sum of digest sizes for all cache upload
	// requests made by this invocation.
	TotalUploadSizeBytes int64

	// TotalDownloadTransferredSizeBytes is the sum of payload sizes (compressed,
	// if applicable) for all cache download requests made by this invocation.
	TotalDownloadTransferredSizeBytes int64

	// TotalUploadTransferredSizeBytes is the sum of payload sizes
	// (compressed, if applicable) for all cache upload requests made by this
	// invocation.
	TotalUploadTransferredSizeBytes int64

	TotalDownloadUsec int64
	TotalUploadUsec   int64

	// TotalCachedActionExecUsec is the sum of the original execution duration
	// of the action that hits the action cache.
	TotalCachedActionExecUsec int64
	// TotalUncachedActionExecUsec is the sum of the execution duration the action
	// that misses the action cache.
	TotalUncachedActionExecUsec int64

	DownloadThroughputBytesPerSecond int64
	InvocationUUID                   []byte `gorm:"size:16;default:NULL;uniqueIndex:invocation_invocation_uuid;unique"`
	Success                          bool
	Attempt                          uint64 `gorm:"not null;default:0"`
	RunID                            string
	BazelExitCode                    string

	// The user-specified setting of how to download outputs from remote cache.
	// The value maps to invocation.DownloadOutputsOption
	DownloadOutputsOption int64

	// The user-sepcified setting of whether to upload local results to remote
	// cache.
	UploadLocalResultsEnabled bool

	// The user's setting of whether remote execution is enabled.
	RemoteExecutionEnabled bool

	Tags string

	ParentRunID string `gorm:"index:parent_run_id_index"`
}

func (i *Invocation) TableName() string {
	return "Invocations"
}

type CacheEntry struct {
	EntryID string `gorm:"primaryKey;"`
	Model
	ExpirationTimeUsec int64
	SizeBytes          int64
	ReadCount          int64
}

func (c *CacheEntry) TableName() string {
	return "CacheEntries"
}

// NOTE: Do not use `url_identifier_index` as an index name for Group.
// It is removed as part of a migration.

type Group struct {
	// A unique URL segment that is displayed in group-related URLs.
	// e.g. "example-org" in app.buildbuddy.com/join/example-org or
	// "example-org.buildbuddy.com" if we support subdomains in the future.
	URLIdentifier string `gorm:"default:NULL;unique;uniqueIndex:url_identifier_unique_index;"`

	// The group ID -- a unique ID.
	GroupID string `gorm:"primaryKey;"`

	// The user that OWNS this group. Only this user may modify it.
	UserID string

	// The group name. This may be displayed to users.
	Name string

	// The "owned" domain. In enterprise/cloud version, we create a
	// group for a customer's domain, and new users that sign up with an
	// email belonging to that domain may be added to this group.
	OwnedDomain string `gorm:"index:owned_domain_index"`

	// The group access token. This token allows writing data for this
	// group.
	WriteToken string `gorm:"index:write_token_index"`

	// The group's Github API token.
	GithubToken *string
	Model

	SharingEnabled                    bool `gorm:"default:0"`
	UserOwnedKeysEnabled              bool `gorm:"not null;default:0"`
	BotSuggestionsEnabled             bool `gorm:"not null;default:1"`
	CodeSearchEnabled                 bool `gorm:"not null;default:0"`
	DeveloperOrgCreationEnabled       bool `gorm:"not null;default:1"`
	RestrictCleanWorkflowRunsToAdmins bool `gorm:"not null;default:0"`
	ExternalUserManagement            bool `gorm:"not null;default:0"`

	// If enabled, builds for this group will always use their own executors instead of the installation-wide shared
	// executors.
	UseGroupOwnedExecutors bool

	CacheEncryptionEnabled bool `gorm:"not null;default:0"`
	EnforceIPRules         bool `gorm:"not null;default:0"`

	// The SAML IDP Metadata URL for this group.
	SamlIdpMetadataUrl string `gorm:"index:group_saml_idp_metadata_url_idx"`

	InvocationWebhookURL string `gorm:"not null;default:''"`

	SuggestionPreference grpb.SuggestionPreference `gorm:"not null;default:1"`

	// The public key and encrypted private key. Used to upload secrets.
	PublicKey           string
	EncryptedPrivateKey string

	// When a Group is designated as a "parent" then any Admin keys from that
	// org also work for managing groups with the same SAML IDP Metadata URL.
	IsParent bool `gorm:"not null;default:0"`
}

func (g *Group) TableName() string {
	return "Groups"
}

type UserGroup struct {
	UserUserID   string `gorm:"primaryKey"`
	GroupGroupID string `gorm:"primaryKey"`

	// The user's role within the group.
	// Constants are defined in the perms package.
	Role uint32

	// The user's membership status.
	// Values correspond to `GroupMembershipStatus` enum values in `grp.proto`.
	MembershipStatus int32 `gorm:"index:membership_status_index"`
}

func (ug *UserGroup) TableName() string {
	return "UserGroups"
}

type GroupRole struct {
	Group
	Role uint32
}

type User struct {
	// The buildbuddy user ID.
	UserID string `gorm:"primaryKey;"`

	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	SubID string `gorm:"index:sub_id_index"`

	// Profile information etc.
	FirstName string
	LastName  string
	Email     string
	ImageURL  string

	// User-specific Github token (if linked).
	GithubToken string

	// Group roles are used to determine read/write permissions
	// for everything.
	Groups []*GroupRole `gorm:"-"`
	Model
}

func (u *User) TableName() string {
	return "Users"
}

func (u *User) ToProto() *uspb.DisplayUser {
	return &uspb.DisplayUser{
		UserId: &uspb.UserId{
			Id: u.UserID,
		},
		Name: &uspb.Name{
			Full:  strings.TrimSpace(u.FirstName + " " + u.LastName),
			First: u.FirstName,
			Last:  u.LastName,
		},
		ProfileImageUrl: u.ImageURL,
		Email:           u.Email,
		AccountType:     subIDToAccountType(u.SubID),
	}
}

func subIDToAccountType(s string) uspb.AccountType {
	if strings.HasPrefix(s, "https://accounts.google.com/") {
		return uspb.AccountType_GOOGLE
	}
	if strings.HasPrefix(s, "https://app.buildbuddy.io/saml/") {
		return uspb.AccountType_SAML
	}
	if strings.HasPrefix(s, "https://github.com/") {
		return uspb.AccountType_GITHUB
	}
	return uspb.AccountType_OIDC
}

type Token struct {
	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	SubID        string `gorm:"primaryKey"`
	AccessToken  string `gorm:"size:4096"`
	RefreshToken string `gorm:"size:4096"`
	Model
	ExpiryUsec int64
}

func (t *Token) TableName() string {
	return "Tokens"
}

type Session struct {
	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	SessionID    string `gorm:"primaryKey"`
	SubID        string `gorm:"index:session_sub_id_index"`
	AccessToken  string `gorm:"size:4096"`
	RefreshToken string `gorm:"size:4096"`
	Model
	ExpiryUsec int64
}

func (s *Session) TableName() string {
	return "Sessions"
}

type APIKey struct {
	Model

	APIKeyID string `gorm:"primaryKey"`
	// The user-specified description of the API key that helps them
	// remember what it's for.
	Label   string
	UserID  string
	GroupID string `gorm:"index:api_key_group_id_index"`
	// The API key token used for authentication.
	Value          string `gorm:"default:NULL;uniqueIndex:api_key_value_composite_index;index:api_key_unencrypted_value_index;"`
	EncryptedValue string `gorm:"default:'';uniqueIndex:api_key_value_composite_index;index:api_key_encrypted_value_index;"`
	Nonce          string `gorm:"default:NULL;uniqueIndex:api_key_nonce_index"`
	Perms          int32  `gorm:"default:NULL"`
	// Capabilities that are enabled for this key. Defaults to CACHE_WRITE.
	//
	// NOTE: If the default is changed, a DB migration may be required to
	// migrate old DB rows to reflect the new default.
	Capabilities        int32 `gorm:"default:1"`
	VisibleToDevelopers bool  `gorm:"not null;default:0"`
	// Indicates whether this key is used for impersonation.
	Impersonation bool `gorm:"not null;default:0"`
	// If set, the API key is not considered to be valid after this time.
	ExpiryUsec int64 `gorm:"not null;default:0"`
}

func (k *APIKey) TableName() string {
	return "APIKeys"
}

type Secret struct {
	UserID  string `gorm:"primaryKey"`
	GroupID string `gorm:"primaryKey"`
	Name    string `gorm:"primaryKey"`
	Value   string `gorm:"type:text"`
	Perms   int32  `gorm:"default:NULL"`
}

func (s *Secret) TableName() string {
	return "Secrets"
}

type Execution struct {
	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	ExecutionID string `gorm:"primaryKey"`
	UserID      string `gorm:"index:executions_user_id"`
	GroupID     string `gorm:"index:executions_group_id"`
	Worker      string
	// Command Snippet
	CommandSnippet          string
	InvocationID            string `gorm:"index:executions_invocation_id_stage"`
	StatusMessage           string
	SerializedStatusDetails []byte `gorm:"size:max"`

	SerializedOperation []byte `gorm:"size:max;type:text"` // deprecated
	Model

	Stage int64 `gorm:"index:executions_invocation_id_stage"`

	// IOStats
	FileDownloadCount        int64
	FileDownloadSizeBytes    int64
	FileDownloadDurationUsec int64
	FileUploadCount          int64
	FileUploadSizeBytes      int64
	FileUploadDurationUsec   int64

	// UsageStats
	PeakMemoryBytes int64
	CPUNanos        int64

	// Task sizing
	EstimatedMemoryBytes int64
	EstimatedMilliCPU    int64

	// ExecutedActionMetadata (in addition to Worker above)
	Perms                              int32 `gorm:"index:executions_perms;default:NULL"`
	QueuedTimestampUsec                int64
	WorkerStartTimestampUsec           int64
	WorkerCompletedTimestampUsec       int64
	InputFetchStartTimestampUsec       int64
	InputFetchCompletedTimestampUsec   int64
	ExecutionStartTimestampUsec        int64
	ExecutionCompletedTimestampUsec    int64
	OutputUploadStartTimestampUsec     int64
	OutputUploadCompletedTimestampUsec int64

	StatusCode int32
	ExitCode   int32

	CachedResult bool
	DoNotCache   bool
}

func (t *Execution) TableName() string {
	return "Executions"
}

// InvocationExecution means that the invocation sent this execution, and the
// type indicates if this resulted in actually running the execution or merging
// into another one. This corresponds to the proto
// stored_invocation.StoredInvocationLink.
type InvocationExecution struct {
	Model

	InvocationID string `gorm:"primaryKey"`
	ExecutionID  string `gorm:"primaryKey;index:execution_id_index"`
	Type         int8
}

func (t *InvocationExecution) TableName() string {
	return "InvocationExecutions"
}

type TelemetryLog struct {
	Hostname         string
	InstallationUUID string `gorm:"primaryKey"`
	InstanceUUID     string `gorm:"primaryKey"`
	TelemetryLogUUID string `gorm:"primaryKey"`
	AppVersion       string
	AppURL           string
	Model
	InvocationCount     int64
	RecordedAtUsec      int64
	RegisteredUserCount int64
	BazelUserCount      int64
	BazelHostCount      int64

	FeatureCacheEnabled bool
	FeatureRBEEnabled   bool
	FeatureAPIEnabled   bool
	FeatureAuthEnabled  bool
}

func (t *TelemetryLog) TableName() string {
	return "TelemetryLog"
}

type CacheLog struct {
	InvocationID       string `gorm:"primaryKey"`
	JoinKey            string `gorm:"primaryKey"`
	DigestHash         string
	RemoteInstanceName string
	SerializedProto    []byte `gorm:"size:max"`
	Model
}

func (c *CacheLog) TableName() string {
	return "CacheLogs"
}

type Target struct {
	RuleType string
	UserID   string `gorm:"index:target_user_id"`
	GroupID  string `gorm:"not null;index:target_group_id;uniqueIndex:target_target_id_group_id_idx,priority:2"`
	RepoURL  string
	Label    string
	Model
	Perms int32 `gorm:"index:target_perms;default:NULL"`
	// TargetID is made up of repoURL + label.
	TargetID int64 `gorm:"not null;uniqueIndex:target_target_id_group_id_idx,priority:1"`
}

func (t *Target) TableName() string {
	return "Targets"
}

// The Status of a target.
// Do not use "target_status_invocation_uuid" as an index name.
type TargetStatus struct {
	Model
	TargetID       int64  `gorm:"primaryKey;autoIncrement:false"`
	InvocationUUID []byte `gorm:"primaryKey;autoIncrement:false;size:16;index:target_status_invocation_uuid_idx"`
	TargetType     int32
	TestSize       int32
	Cached         bool `gorm:"not null;default:0"`
	Status         int32
	StartTimeUsec  int64
	DurationUsec   int64
}

func (ts *TargetStatus) TableName() string {
	return "TargetStatuses"
}

// GitHubAppInstallation represents a BuildBuddy GitHub App installation linked
// to an organization.
//
// We'll listen to app uninstallation webhook events to proactively remove these
// from the DB, but this is not 100% reliable, so GitHub is ultimately the
// source of truth for whether an installation is valid or not. Typically, the
// app can optimistically try to create a token for the installation ID, and
// GitHub will return an error due to the installation no longer existing or no
// longer being enabled for a particular repo.
type GitHubAppInstallation struct {
	Model

	// UserID is the user that registered this installation to BuildBuddy.
	UserID string `gorm:"not null"`

	// GroupID is the group linked to the GitHub app installation.
	GroupID string `gorm:"primaryKey"`
	Perms   int32  `gorm:"not null"`

	// InstallationID is the GitHub app installation ID.
	InstallationID int64 `gorm:"not null"`

	// Owner is the GitHub login of the installation (either a user or
	// organization name). This can be computed from the installation ID, but we
	// store it here so that we can run queries to associate repos with
	// installations.
	Owner string `gorm:"primaryKey"`
}

func (gh *GitHubAppInstallation) TableName() string {
	return "GitHubAppInstallations"
}

// GitRepository represents a Git repository linked to a BB organization.
type GitRepository struct {
	Model

	// RepoURL is the normalized repo URL.
	RepoURL string `gorm:"primaryKey;unique"`
	UserID  string `gorm:"not null"`
	GroupID string `gorm:"primaryKey"`
	Perms   int32  `gorm:"not null"`

	// InstanceNameSuffix is the remote instance name suffix to apply to any
	// BuildBuddy-initiated invocations within this repo, such as workflows or
	// remote Bazel.
	InstanceNameSuffix string `gorm:"not null;default:''"`

	// DefaultNonRootRunner decides whether workflow and remote bazel runs
	// within this repository should run as a non-root user by default.
	// TODO(http://go/b/3286): Remove this field after completing migration.
	DefaultNonRootRunner bool `gorm:"not null;default:0"`
}

func (g *GitRepository) TableName() string {
	return "GitRepositories"
}

// Workflow represents a set of BuildBuddy actions to be run in response to
// events published to a Git webhook.
//
// TODO(bduffany): figure out a migration path from Workflows (which are created
// via the legacy OAuth app integration) to GitRepositories (created using the
// new GitHub App integration).
type Workflow struct {
	RepoURL     string
	WorkflowID  string `gorm:"primaryKey"`
	UserID      string `gorm:"index:workflow_user_id"`
	GroupID     string `gorm:"index:workflow_group_id"`
	Name        string
	Username    string
	AccessToken string `gorm:"size:4096"`
	WebhookID   string `gorm:"default:NULL;unique;uniqueIndex:workflow_webhook_id_index;"`
	Model
	Perms int32 `gorm:"index:workflow_perms;default:NULL"`
	// InstanceNameSuffix is appended to the remote instance name for CI runner
	// actions associated with this workflow. It can be updated in order to
	// prevent reusing a bad workspace.
	InstanceNameSuffix string `gorm:"not null;default:''"`
	// GitProviderWebhookID is the ID returned from the Git provider API when
	// registering the webhook. This will only be set for the case where we
	// successfully auto-registered the webhook.
	GitProviderWebhookID string

	// Set if this workflow was adapted from a GitRepository.
	GitRepository *GitRepository `gorm:"-"`
}

func (wf *Workflow) TableName() string {
	return "Workflows"
}

type UsageCounts struct {
	Invocations            int64
	CASCacheHits           int64
	ActionCacheHits        int64
	TotalDownloadSizeBytes int64

	// NOTE: New fields added here should be annotated with
	// `gorm:"not null;default:0"`

	LinuxExecutionDurationUsec           int64 `gorm:"not null;default:0"`
	MacExecutionDurationUsec             int64 `gorm:"not null;default:0"`
	SelfHostedLinuxExecutionDurationUsec int64 `gorm:"not null;default:0"`
	SelfHostedMacExecutionDurationUsec   int64 `gorm:"not null;default:0"`
	TotalUploadSizeBytes                 int64 `gorm:"not null;default:0"`
	TotalCachedActionExecUsec            int64 `gorm:"not null;default:0"`
	CPUNanos                             int64 `gorm:"not null;default:0"`
	MemoryGBUsec                         int64 `gorm:"not null;default:0"`
}

type UsageLabels struct {
	// Origin identifies the network/location of the usage.
	Origin string `gorm:"not null;default:''"`

	// Client describes the type of client responsible for the usage, such as
	// "bazel" or "executor".
	Client string `gorm:"not null;default:''"`
}

// Usage holds usage counter values for a group during a particular time period.
type Usage struct {
	Model

	// TODO(bduffany): backfill this, then make it a primary key.
	UsageID string

	GroupID string `gorm:"not null;index:group_period_region_index_v2,priority:1"`

	// PeriodStartUsec is the time at which the usage period started, in
	// microseconds since the Unix epoch. The usage period duration is 1 hour.
	// Only usage data occurring in collection periods inside this 1 hour period
	// is included in this usage row.
	PeriodStartUsec int64 `gorm:"not null;index:group_period_region_index_v2,priority:2"`

	// Region is the region in which the usage data was originally gathered.
	// Since we have a global DB deployment but usage data is collected
	// per-region, this effectively partitions the usage table by region, allowing
	// the FinalBeforeUsec logic to work independently in each region.
	Region string `gorm:"not null;index:group_period_region_index_v2,priority:3"`

	// FinalBeforeUsec is the time before which all collection period data in
	// this usage period is finalized. This is used to guarantee that collection
	// period data is added to this row in strictly increasing order of
	// collection period start time.
	//
	// Consider the following diagram:
	//
	// [xxxxxxxxxxxxxxxxxxx)------------------)
	// ^ PeriodStart       ^ FinalBefore      ^ PeriodEnd = PeriodStart + 1hr
	//
	// Usage data collected during the x-marked region cannot be added to this
	// usage row any longer, since the usage period is "finalized" in this
	// region.
	//
	// When writing the next collection period's data, the FinalBefore timestamp
	// is updated as follows:
	//
	// [xxxxxxxxxxxxxxxxxxx[xxxxxx)-----------)
	//                     ^ FinalBefore (before update) = CollectionPeriodStart
	//                            ^ FinalBefore (after update) = CollectionPeriodEnd
	FinalBeforeUsec int64 `gorm:"default:0"`

	UsageCounts

	UsageLabels
}

func (*Usage) TableName() string {
	return "Usages"
}

type QuotaBucket struct {
	Model
	// The namespace indicates a single resource to be protected from abusive
	// usage.
	Namespace string `gorm:"primarykey"`

	// The name of the bucket, like "default", "banned", or "restricted".
	Name string `gorm:"primarykey"`

	// The maximum sustained rate of requests
	NumRequests        int64
	PeriodDurationUsec int64

	// The number of requests that will be allowed to exceed the rate in a single
	// burst and must be non-negative.
	MaxBurst int64
}

func (*QuotaBucket) TableName() string {
	return "QuotaBuckets"
}

// QuotaGroup defines the relationship between a QuotaBucket to a QuotaKey. For,
// example, user:X is in bucket:restricted.
type QuotaGroup struct {
	Model
	Namespace string `gorm:"primarykey"`
	// GroupID or IP address (for anon clients). Used to count quota for a single
	// user.
	QuotaKey string `gorm:"primarykey"`

	BucketName string
}

func (*QuotaGroup) TableName() string {
	return "QuotaGroups"
}

type EncryptionKey struct {
	Model
	EncryptionKeyID string `gorm:"primaryKey"`
	GroupID         string
}

func (*EncryptionKey) TableName() string {
	return "EncryptionKeys"
}

type EncryptionKeyVersion struct {
	Model
	EncryptionKeyID string `gorm:"primaryKey"`
	Version         int32  `gorm:"primaryKey"`

	// BuildBuddy portion of the composite key encrypted using the master key.
	MasterEncryptedKey []byte
	// URI of the group key in the external Key Management Service.
	GroupKeyURI string
	// Group portion of the composite key encrypted using the above group key.
	GroupEncryptedKey []byte

	// Last time we attempted to encrypt composite key portions using the KMS
	// keys.
	LastEncryptionAttemptAtUsec int64 `gorm:"index:last_encryption_attempt_idx"`
	// Last time the composite key portions were encrypted using the KMS keys.
	LastEncryptedAtUsec int64
}

func (*EncryptionKeyVersion) TableName() string {
	return "EncryptionKeyVersions"
}

type IPRule struct {
	Model
	IPRuleID    string `gorm:"primaryKey"`
	GroupID     string `gorm:"index:ip_rule_group_id_idx"`
	CIDR        string `gorm:"column:cidr"`
	Description string
}

func (*IPRule) TableName() string {
	return "IPRules"
}

type PostAutoMigrateLogic func() error

// Manual migration called before auto-migration.
//
// May return a list of functions to be executed after auto-migration.
// This is useful in cases where some logic needs to be executed in PostAutoMigrate,
// but some info is needed before the migration takes place in order to know what
// to do.
func PreAutoMigrate(db *gorm.DB) ([]PostAutoMigrateLogic, error) {
	postMigrate := make([]PostAutoMigrateLogic, 0)

	m := db.Migrator()

	// Initialize UserGroups.membership_status to 1 if the column doesn't exist.
	if m.HasTable("UserGroups") && !m.HasColumn(&UserGroup{}, "membership_status") {
		if err := db.Exec(`ALTER TABLE "UserGroups" ADD membership_status int`).Error; err != nil {
			return nil, err
		}
		if err := db.Exec(`UPDATE "UserGroups" SET membership_status = ?`, int32(grpb.GroupMembershipStatus_MEMBER)).Error; err != nil {
			return nil, err
		}
	}
	// Initialize UserGroups.role to Admin if the role column doesn't exist.
	if m.HasTable("UserGroups") && !m.HasColumn(&UserGroup{}, "role") {
		postMigrate = append(postMigrate, func() error {
			return db.Exec(`UPDATE "UserGroups" SET role = ?`, uint32(role.Admin)).Error
		})
	}

	// Prepare Groups.url_identifier for index update (non-unique index to unique index).
	if m.HasTable("Groups") {
		// Remove the old url_identifier_index.
		if m.HasIndex("Groups", "url_identifier_index") {
			if err := m.DropIndex("Groups", "url_identifier_index"); err != nil {
				return nil, err
			}
		}
		// Before creating a unique index, need to replace empty strings with NULL.
		if !m.HasIndex("Groups", "url_identifier_unique_index") {
			if err := db.Exec(`UPDATE "Groups" SET url_identifier = NULL WHERE url_identifier = ''`).Error; err != nil {
				return nil, err
			}
		}
	}

	// Migrate Groups.APIKey to APIKey rows.
	if m.HasTable("Groups") && m.HasColumn(&Group{}, "api_key") && !m.HasTable("APIKeys") {
		postMigrate = append(postMigrate, func() error {
			rows, err := db.Raw(`SELECT group_id, api_key FROM "Groups"`).Rows()
			if err != nil {
				return err
			}
			defer rows.Close()

			var g Group
			var apiKey string

			// These constants are already defined in perms.go, but we can't reference that
			// due to a circular dep (tables -> perms -> interfaces -> tables).
			// Probably not worth refactoring right now, since eventually we want to
			// release a new version with this migration logic removed.
			groupRead := 0o040
			groupWrite := 0o020
			apiKeyPerms := groupRead | groupWrite

			for rows.Next() {
				if err := rows.Scan(&g.GroupID, &apiKey); err != nil {
					return err
				}
				pk, err := PrimaryKeyForTable("APIKeys")
				if err != nil {
					return err
				}

				if err := db.Exec(
					`INSERT INTO "APIKeys" (api_key_id, group_id, perms, value, label) VALUES (?, ?, ?, ?, ?)`,
					pk, g.GroupID, apiKeyPerms, apiKey, "Default API key").Error; err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Populate invocation_uuid if the column doesn't exist.
	hasUUIDAsPK, err := hasPrimaryKey(db, &TargetStatus{}, "invocation_uuid")
	if err != nil {
		return nil, err
	}
	if m.HasTable("TargetStatuses") && !hasUUIDAsPK {
		switch db.Dialector.Name() {
		case sqliteDialect:
			// Rename the TargetStatuses table with invocation_pk as the primary key,
			// so that during auto migration, SQLite can create TargetStatuses table with new
			// primary keys.
			db.Migrator().RenameTable("TargetStatuses", "TargetStatusesOld")
			postMigrate = append(postMigrate, func() error {
				return postMigrateInvocationUUIDForSQLite(db)
			})
		case mysqlDialect:
			versionStr := ""
			if err := db.Raw("select version()").Scan(&versionStr).Error; err != nil {
				return nil, err
			}
			log.Debugf("MySQL Version: %q", versionStr)
			postMigrate = append(postMigrate, func() error {
				return postMigrateInvocationUUIDForMySQL(db)
			})
		default:
			log.Warningf("Unsupported sql dialect: %q", db.Dialector.Name())
		}
	}
	if db.Dialector.Name() == mysqlDialect && m.HasTable("TargetStatuses") && m.HasIndex("TargetStatuses", "target_status_invocation_pk") {
		// Drop the invocation_pk index; and set default to invocation_pk column.
		postMigrate = append(postMigrate, func() error {
			if err := m.DropIndex("TargetStatuses", "target_status_invocation_pk"); err != nil {
				return err
			}
			if err := db.Exec(`ALTER TABLE "TargetStatuses" ALTER invocation_pk SET DEFAULT 0`).Error; err != nil {
				return err
			}
			return nil
		})
	}

	if m.HasTable("APIKeys") {
		// Add nonce column w/o unique index so that we can backfill it before
		// gorm adds the unique index.
		if !m.HasColumn(&APIKey{}, "nonce") {
			stmt := `ALTER TABLE "APIKeys" ADD COLUMN "nonce" varchar(255) DEFAULT NULL`
			if err := db.Exec(stmt).Error; err != nil {
				return nil, err
			}
		}

		row := &struct{ Count int64 }{}
		err := db.Raw(`SELECT COUNT(*) AS count FROM "APIKeys" WHERE nonce is NULL or nonce = ''`).Take(row).Error
		if err != nil {
			return nil, err
		}

		if row.Count > 0 {
			backfill := `
				UPDATE "APIKeys"
				SET nonce = SUBSTR(value, 1, 6)
				WHERE nonce is NULL or nonce = ''
			`
			if err := db.Exec(backfill).Error; err != nil {
				return nil, err
			}
		}
	}

	return postMigrate, nil
}

func updateInBatches(db *gorm.DB, baseQuery string, batchSize int64) error {
	totalRows := int64(0)
	for {
		tx := db.Exec(baseQuery+" LIMIT ?", batchSize)
		if err := tx.Error; err != nil {
			return err
		}
		totalRows += tx.RowsAffected
		log.Infof("updated %d rows in total for base query %q", totalRows, baseQuery)
		if tx.RowsAffected == 0 {
			return nil
		}
	}
}

func postMigrateInvocationUUIDForMySQL(db *gorm.DB) error {
	updateInvocationStmt := `UPDATE "Invocations" SET invocation_uuid = UNHEX(REPLACE(invocation_id, "-","")) WHERE invocation_uuid IS NULL`
	if err := updateInBatches(db, updateInvocationStmt, 10000); err != nil {
		return err
	}
	updateTargetStatusStmt := `
		UPDATE "TargetStatuses" ts SET invocation_uuid=
			(SELECT invocation_uuid FROM "Invocations" i 
			WHERE i.invocation_pk=ts.invocation_pk)
		WHERE invocation_uuid IS NULL`
	if err := updateInBatches(db, updateTargetStatusStmt, 10000); err != nil {
		return err
	}

	// Delete all the target statuses without invocation uuid. This could happen when
	// a target status has a invocation_pk that's not in Invocations table. It's OK
	// to delete these target statuses because these target statuses are not showing
	// up in the UI right now.
	deleteTargetStatusStmt := `DELETE FROM "TargetStatuses" WHERE invocation_uuid IS NULL`
	if err := updateInBatches(db, deleteTargetStatusStmt, 10000); err != nil {
		return err
	}

	// Check whether primary keys exist for TargetStatuses before dropping the primary key;
	// Otherwise dropping primary keys will fail.
	var primaryKeyCount int32
	countPrimaryKeyStmt := `
		SELECT 
			COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			table_schema = schema() AND 
			column_key = 'PRI' AND 
			table_name='TargetStatuses'`

	if err := db.Raw(countPrimaryKeyStmt).Scan(&primaryKeyCount).Error; err != nil {
		return err
	}

	changePKStmt := `ALTER TABLE "TargetStatuses" `
	if primaryKeyCount > 0 {
		// Only drop primary keys when they exist.
		changePKStmt += `DROP PRIMARY KEY, `
	}
	changePKStmt += `ADD PRIMARY KEY(target_id, invocation_uuid), `

	isStrictModeEnabled, err := isStrictModeEnabled(db)
	if err != nil {
		return err
	}
	if isStrictModeEnabled {
		changePKStmt += `ALGORITHM=INPLACE, LOCK=NONE`
	} else {
		changePKStmt += `ALGORITHM=COPY`
	}
	if err := db.Exec(changePKStmt).Error; err != nil {
		return err
	}
	if db.Migrator().HasIndex("TargetStatuses", "target_status_invocation_uuid") {
		return db.Migrator().DropIndex("TargetStatuses", "target_status_invocation_uuid")
	}
	return nil
}

type invocationIDs struct {
	InvocationID string `gorm:"primarykey"`
	InvocationPK int64
}

func postMigrateInvocationUUIDForSQLite(db *gorm.DB) error {
	// SQLite doesn't have UNHEX function; so we need to calculate
	// invocationUUID in the app.
	var results []invocationIDs
	updateFunc := func(tx *gorm.DB, batch int) error {
		for _, invocation := range results {
			invocationUUID, err := uuid.StringToBytes(invocation.InvocationID)
			if err != nil {
				return err
			}
			if err := db.Exec(`UPDATE "TargetStatusesOld" SET invocation_uuid = ? WHERE invocation_pk = ? `, invocationUUID, invocation.InvocationPK).Error; err != nil {
				return err
			}
			if err := db.Exec(`UPDATE "Invocations" SET invocation_uuid = ? WHERE invocation_id = ? `, invocationUUID, invocation.InvocationID).Error; err != nil {
				return err
			}
		}
		return nil
	}
	res := db.Table("Invocations").Select("invocation_id", "invocation_pk").Where("invocation_uuid IS NULL").FindInBatches(&results, 100, updateFunc)
	if res.Error != nil {
		return res.Error
	}

	// Copy data from TargetStatusesOld to the new table
	insertStmt := `INSERT INTO "TargetStatuses"
			(target_id, invocation_uuid, target_type, test_size, status,
			start_time_usec, duration_usec, created_at_usec, updated_at_usec)
		SELECT 
			target_id, invocation_uuid, target_type, test_size, status,
			start_time_usec, duration_usec, created_at_usec, updated_at_usec 
		FROM 
			"TargetStatusesOld"`

	if err := db.Exec(insertStmt).Error; err != nil {
		return err
	}
	db.Migrator().DropTable("TargetStatusesOld")
	return nil
}

func dropIndexIfExists(m gorm.Migrator, table, indexName string) {
	if m.HasTable(table) && m.HasIndex(table, indexName) {
		if err := m.DropIndex(table, indexName); err != nil {
			log.Errorf("Error dropping index %q on table %q: %s", indexName, table, err)
		}
	}
}

// Manual migration called after auto-migration.
func PostAutoMigrate(db *gorm.DB) error {
	invocationIndices := map[string]string{
		"invocations_trends_query_index":      `("group_id", "updated_at_usec")`,
		"invocations_trends_query_role_index": `("group_id", "role", "updated_at_usec")`,
		"invocations_stats_group_id_index":    `("group_id", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
		"invocations_stats_user_index":        `("group_id", "user", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
		"invocations_stats_host_index":        `("group_id", "host", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
		"invocations_stats_repo_index":        `("group_id", "repo_url", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
		"invocations_stats_branch_index":      `("group_id", "branch_name", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
		"invocations_stats_commit_index":      `("group_id", "commit_sha", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
		"invocations_stats_role_index":        `("group_id", "role", "action_count", "duration_usec", "updated_at_usec", "success", "invocation_status")`,
	}
	prefixIndicesByDialect := map[string]map[string]string{
		mysqlDialect: {
			"invocations_test_grid_query_command_index": `("group_id" (25), "role" (10), "repo_url", "command" (10), "created_at_usec" DESC)`,
		},
		sqliteDialect: {
			"invocations_test_grid_query_command_index": `("group_id", "role", "repo_url", "command" , "created_at_usec" DESC)`,
		},
		postgresDialect: {
			"invocations_test_grid_query_command_index": `("group_id", "role", "repo_url", "command" , "created_at_usec" DESC)`,
		},
	}
	prefixIndexes, ok := prefixIndicesByDialect[db.Dialector.Name()]
	if ok {
		for name, cols := range prefixIndexes {
			invocationIndices[name] = cols
		}
	}

	executionIndices := map[string]string{
		"executions_created_at_usec_index": `("created_at_usec")`,
	}

	indicesByTable := map[string]map[string]string{
		"Invocations": invocationIndices,
		"Executions":  executionIndices,
	}

	m := db.Migrator()
	for tableName, indexes := range indicesByTable {
		if m.HasTable(tableName) {
			for indexName, cols := range indexes {
				if m.HasIndex(tableName, indexName) {
					continue
				}
				var query string
				if db.Dialector.Name() == mysqlDialect {
					query = fmt.Sprintf(`ALTER TABLE "%s" ADD INDEX "%s" %s, ALGORITHM=INPLACE, LOCK=NONE`, tableName, indexName, cols)
				} else {
					query = fmt.Sprintf(`CREATE INDEX "%s" ON "%s" %s`, indexName, tableName, cols)
				}
				err := db.Exec(query).Error
				if err != nil {
					log.Errorf("Error creating %s on table %q: %s", indexName, tableName, err)
				}
			}
		}
	}

	dropIndexIfExists(m, "Executions", "execution_invocation_id")
	dropIndexIfExists(m, "Targets", "target_target_id")
	dropIndexIfExists(m, "Invocations", "invocations_test_grid_query_index")
	// Drop the unique indexes on the value column. They were replaced by a
	// combination of a composite unique index on (value, encrypted_value) and
	// non-unique indexes each column.
	dropIndexIfExists(m, "APIKeys", "value")
	dropIndexIfExists(m, "APIKeys", "api_key_value_index")

	type ColRef struct {
		table  Table
		column string
	}

	colsToDelete := []ColRef{
		// Group.api_key has been migrated to a single row in the APIKeys table.
		{&Group{}, "api_key"},
	}

	// Dropping invocation_pk columns behind a flag. The columns
	// should be dropped only when all the servers in production
	// no longer use invocation_pk.
	// On-prem users could use this flag to control when to drop
	// the columns.
	if *dropInvocationPKCol {
		colsToDelete = append(colsToDelete,
			// Invocation.invocation_pk has been migrated to Invocation.invocation_uuid
			ColRef{&Invocation{}, "invocation_pk"},
			// TargetStatus.invocation_pk has been migrated to Invocation.invocation_uuid
			ColRef{&TargetStatus{}, "invocation_pk"})
	}

	// Drop old columns at the very end of the migration.
	for _, ref := range colsToDelete {
		if !m.HasColumn(ref.table, ref.column) {
			continue
		}
		var err error
		if db.Dialector.Name() == mysqlDialect {
			err = dropColumnInPlaceForMySQL(db, ref.table, ref.column)
		} else {
			err = m.DropColumn(ref.table, ref.column)
		}
		if err != nil {
			log.Warningf("Failed to drop column %s.%s: %s", ref.table.TableName(), ref.column, err)
		}
	}

	return nil
}

func dropColumnInPlaceForMySQL(db *gorm.DB, table Table, column string) error {
	return db.Exec(`ALTER TABLE "` + table.TableName() + `" DROP COLUMN ` + column + `, ALGORITHM=INPLACE, LOCK=NONE`).Error
}

func hasPrimaryKey(db *gorm.DB, table Table, key string) (bool, error) {
	checkPrimaryKeyStmt := ""
	switch dialect := db.Dialector.Name(); dialect {
	case sqliteDialect:
		checkPrimaryKeyStmt = `SELECT COUNT(*) FROM PRAGMA_TABLE_INFO(?) WHERE pk > 0 AND name = ?`
	case mysqlDialect:
		checkPrimaryKeyStmt = `SELECT COUNT(*) from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = SCHEMA() AND column_key = 'PRI' and table_name=? and column_name=?`
	case postgresDialect:
		checkPrimaryKeyStmt = `SELECT COUNT(*) from information_schema.table_constraints NATURAL JOIN information_schema.key_column_usage WHERE constraint_schema = current_schema() and constraint_type = 'PRIMARY KEY' AND table_name=? AND column_name=?`
	default:
		return false, status.InternalErrorf("unsupported db dialect %q", dialect)
	}

	count := 0
	if err := db.Raw(checkPrimaryKeyStmt, table.TableName(), key).Scan(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func isStrictModeEnabled(db *gorm.DB) (bool, error) {
	sqlModes := ""
	if err := db.Raw("select @@SESSION.sql_mode").Scan(&sqlModes).Error; err != nil {
		return false, err
	}
	log.Debugf("MySQL session sql_mode is %q", sqlModes)
	isStrictModeEnabled := strings.Contains(sqlModes, "STRICT_ALL_TABLES") || strings.Contains(sqlModes, "STRICT_TRANS_TABLES")
	return isStrictModeEnabled, nil
}

// LockExclusive locks the given tables for the scope of the given transaction,
// using an exclusive lock.
//
// The returned `unlock` function should be deferred-called within the
// transaction function to unlock the tables.
//
// Note that this function should only be called once per transaction, since in
// MySQL, a LOCK TABLES statement releases any existing held locks.
//
// Table-level locking should be avoided for tables that are frequently written
// to by multiple app instances, as it can hurt performance.
func LockExclusive(tx *gorm.DB, tables ...Table) (unlock func() error, err error) {
	// sqlite does not support table locks, and its transactional isolation is
	// usually strong enough to not need table-level locking.
	switch dialect := tx.Dialector.Name(); dialect {
	case sqliteDialect:
		// sqlite doesn't support table-level locks, but instead runs all
		// transactions with SERIALIZABLE isolation, so table-level locks should
		// not be necessary. For simplicity, just do nothing here.
		return func() error { return nil }, nil
	case mysqlDialect:
		// MySQL tables need to be explicitly unlocked at the end of the
		// transaction. Note that if the transaction fails due the connection
		// being lost, MySQL server will automatically unlock tables.
		var args []string
		for _, t := range tables {
			args = append(args, fmt.Sprintf("%q WRITE", t.TableName()))
		}
		if err := tx.Exec(fmt.Sprintf("LOCK TABLES %s", strings.Join(args, ", "))).Error; err != nil {
			return nil, err
		}
		return func() error { return tx.Exec("UNLOCK TABLES").Error }, nil
	case postgresDialect:
		var args []string
		for _, t := range tables {
			args = append(args, fmt.Sprintf("%q", t.TableName()))
		}
		if err := tx.Exec(fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", strings.Join(args, ", "))).Error; err != nil {
			return nil, err
		}
		return func() error { return nil }, nil
	default:
		return nil, status.UnimplementedErrorf("unsupported SQL dialect %q", dialect)
	}
}

func RegisterTables() {
	allTables = nil
	// Keep these sorted by two-letter prefix (and when adding new tables,
	// use a unique prefix if possible):
	registerTable("AK", &APIKey{})
	registerTable("CA", &CacheEntry{})
	registerTable("CL", &CacheLog{})
	registerTable("EK", &EncryptionKey{})
	registerTable("EV", &EncryptionKeyVersion{})
	registerTable("EX", &Execution{})
	registerTable("GH", &GitHubAppInstallation{})
	registerTable("GR", &Group{})
	registerTable("IE", &InvocationExecution{})
	registerTable("IN", &Invocation{})
	registerTable("IR", &IPRule{})
	registerTable("QB", &QuotaBucket{})
	registerTable("QG", &QuotaGroup{})
	registerTable("RE", &GitRepository{})
	registerTable("SE", &Session{})
	registerTable("SK", &Secret{})
	registerTable("TA", &Target{})
	registerTable("TL", &TelemetryLog{})
	registerTable("TO", &Token{})
	registerTable("TS", &TargetStatus{})
	registerTable("UA", &Usage{})
	registerTable("UG", &UserGroup{})
	registerTable("US", &User{})
	registerTable("WF", &Workflow{})
}
