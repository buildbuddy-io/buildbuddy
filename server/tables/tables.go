package tables

import (
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"gorm.io/gorm"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
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
	return "", fmt.Errorf("Unknown table: %s", tableName)
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
// So, we handle this in go-code and set these to time.Now().UnixNano and store
// as int64.
func (m *Model) BeforeCreate(tx *gorm.DB) (err error) {
	nowInt64 := int64(time.Now().UnixNano() / 1000)
	m.CreatedAtUsec = nowInt64
	m.UpdatedAtUsec = nowInt64
	return nil
}

func (m *Model) BeforeUpdate(tx *gorm.DB) (err error) {
	m.UpdatedAtUsec = int64(time.Now().UnixNano() / 1000)
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
	Model
	DurationUsec                     int64
	UploadThroughputBytesPerSecond   int64
	ActionCount                      int64
	Perms                            int   `gorm:"index:perms"`
	InvocationStatus                 int64 `gorm:"index:invocation_status_idx"`
	ActionCacheHits                  int64
	ActionCacheMisses                int64
	ActionCacheUploads               int64
	CasCacheHits                     int64
	CasCacheMisses                   int64
	CasCacheUploads                  int64
	TotalDownloadSizeBytes           int64
	TotalUploadSizeBytes             int64
	TotalDownloadUsec                int64
	TotalUploadUsec                  int64
	TotalCachedActionExecUsec        int64
	DownloadThroughputBytesPerSecond int64
	InvocationPK                     int64 `gorm:"uniqueIndex:invocation_invocation_pk"`
	Success                          bool
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
	URLIdentifier *string `gorm:"uniqueIndex:url_identifier_unique_index"`

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

	// The group's api key. This allows members of the group to make
	// API requests on the group's behalf.
	APIKey string `gorm:"index:api_key_index"`

	// The group's Github API token.
	GithubToken string
	Model

	SharingEnabled bool `gorm:"default:true"`

	// If enabled, builds for this group will always use their own executors instead of the installation-wide shared
	// executors.
	UseGroupOwnedExecutors bool
}

func (g *Group) TableName() string {
	return "Groups"
}

type UserGroup struct {
	UserUserID   string `gorm:"primaryKey"`
	GroupGroupID string `gorm:"primaryKey"`

	// The user's membership status.
	// Values correspond to `GroupMembershipStatus` enum values in `grp.proto`.
	MembershipStatus int32 `gorm:"index:membership_status_index"`
}

func (ug *UserGroup) TableName() string {
	return "UserGroups"
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

	// Groups are used to determine read/write permissions
	// for everything.
	Groups []*Group `gorm:"-"`
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
			Full:  u.FirstName + " " + u.LastName,
			First: u.FirstName,
			Last:  u.LastName,
		},
		ProfileImageUrl: u.ImageURL,
		Email:           u.Email,
	}
}

type Token struct {
	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	SubID        string `gorm:"primaryKey"`
	AccessToken  string
	RefreshToken string
	Model
	ExpiryUsec int64
}

func (t *Token) TableName() string {
	return "Tokens"
}

type APIKey struct {
	// The user-specified description of the API key that helps them
	// remember what it's for.
	Label    string
	APIKeyID string `gorm:"primaryKey"`
	UserID   string
	GroupID  string `gorm:"index:api_key_group_id_index"`
	// The API key token used for authentication.
	Value string `gorm:"uniqueIndex:api_key_value_index"`
	Model
	Perms int
	// Capabilities that are enabled for this key. Defaults to CACHE_WRITE.
	//
	// NOTE: If the default is changed, a DB migration may be required to
	// migrate old DB rows to reflect the new default.
	Capabilities int32 `gorm:"default:1"`
}

func (k *APIKey) TableName() string {
	return "APIKeys"
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
	InvocationID            string `gorm:"index:execution_invocation_id"`
	StatusMessage           string
	SerializedStatusDetails []byte `gorm:"size:max"`

	SerializedOperation []byte `gorm:"size:max"` // deprecated
	Model

	Stage int64

	// IOStats
	FileDownloadCount        int64
	FileDownloadSizeBytes    int64
	FileDownloadDurationUsec int64
	FileUploadCount          int64
	FileUploadSizeBytes      int64
	FileUploadDurationUsec   int64

	// ExecutedActionMetadata (in addition to Worker above)
	Perms                              int `gorm:"index:executions_perms"`
	QueuedTimestampUsec                int64
	WorkerStartTimestampUsec           int64
	WorkerCompletedTimestampUsec       int64
	InputFetchStartTimestampUsec       int64
	InputFetchCompletedTimestampUsec   int64
	ExecutionStartTimestampUsec        int64
	ExecutionCompletedTimestampUsec    int64
	OutputUploadStartTimestampUsec     int64
	OutputUploadCompletedTimestampUsec int64

	StatusCode   int32
	CachedResult bool
}

func (t *Execution) TableName() string {
	return "Executions"
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

type ExecutionNode struct {
	OS                string
	Host              string `gorm:"primaryKey"`
	Pool              string
	SchedulerHostPort string
	GroupID           string `gorm:"primaryKey;default:''"`
	Constraints       string
	Arch              string
	Model
	AssignableMemoryBytes int64
	AssignableMilliCPU    int64
	Port                  int32 `gorm:"primaryKey;autoIncrement:false"`
	Version               string
}

func (n *ExecutionNode) TableName() string {
	return "ExecutionNodes"
}

type ExecutionTask struct {
	TaskID         string `gorm:"primaryKey"`
	Arch           string
	Pool           string
	OS             string
	SerializedTask []byte `gorm:"size:max"`
	Model
	EstimatedMilliCPU    int64
	ClaimedAtUsec        int64
	AttemptCount         int64
	EstimatedMemoryBytes int64
}

func (n *ExecutionTask) TableName() string {
	return "ExecutionTasks"
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
	GroupID  string `gorm:"index:target_group_id"`
	RepoURL  string
	Label    string
	Model
	Perms int `gorm:"index:target_perms"`
	// TargetID is made up of repoURL + label.
	TargetID int64 `gorm:"uniqueIndex:target_target_id"`
}

func (t *Target) TableName() string {
	return "Targets"
}

// The Status of a target.
type TargetStatus struct {
	Model
	TargetID      int64 `gorm:"primaryKey;autoIncrement:false"`
	InvocationPK  int64 `gorm:"primaryKey;autoIncrement:false;index:target_status_invocation_pk"`
	TargetType    int32
	TestSize      int32
	Status        int32
	StartTimeUsec int64
	DurationUsec  int64
}

func (ts *TargetStatus) TableName() string {
	return "TargetStatuses"
}

// Workflow represents a set of BuildBuddy actions to be run in response to
// events published to a Git webhook.
type Workflow struct {
	RepoURL     string
	WorkflowID  string `gorm:"primaryKey"`
	UserID      string `gorm:"index:workflow_user_id"`
	GroupID     string `gorm:"index:workflow_group_id"`
	Name        string
	Username    string
	AccessToken string
	WebhookID   string `gorm:"uniqueIndex:workflow_webhook_id_index"`
	Model
	Perms int `gorm:"index:workflow_perms"`
	// GithubWebhookID is the ID returned from the GitHub API when
	// registering the webhook. This will only be set for GitHub URLs in the
	// case where we successfully auto-registered the webhook.
	GitHubWebhookID int64 `gorm:"column:github_webhook_id"`
}

func (wf *Workflow) TableName() string {
	return "Workflows"
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
		if err := db.Exec("ALTER TABLE UserGroups ADD membership_status int").Error; err != nil {
			return nil, err
		}
		if err := db.Exec("UPDATE UserGroups SET membership_status = ?", int32(grpb.GroupMembershipStatus_MEMBER)).Error; err != nil {
			return nil, err
		}
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
			if err := db.Exec(`UPDATE Groups SET url_identifier = NULL WHERE url_identifier = ""`).Error; err != nil {
				return nil, err
			}
		}
	}

	// Migrate Groups.APIKey to APIKey rows.
	if m.HasTable("Groups") && !m.HasTable("APIKeys") {
		postMigrate = append(postMigrate, func() error {
			rows, err := db.Raw(`SELECT group_id, api_key FROM ` + "`Groups`" + ``).Rows()
			if err != nil {
				return err
			}
			defer rows.Close()

			var g Group

			// These constants are already defined in perms.go, but we can't reference that
			// due to a circular dep (tables -> perms -> interfaces -> tables).
			// Probably not worth refactoring right now, since eventually we want to
			// release a new version with this migration logic removed.
			groupRead := 0o040
			groupWrite := 0o020
			apiKeyPerms := groupRead | groupWrite

			for rows.Next() {
				if err := rows.Scan(&g.GroupID, &g.APIKey); err != nil {
					return err
				}
				pk, err := PrimaryKeyForTable("APIKeys")
				if err != nil {
					return err
				}

				if err := db.Exec(
					`INSERT INTO APIKeys (api_key_id, group_id, perms, value, label) VALUES (?, ?, ?, ?, ?)`,
					pk, g.GroupID, apiKeyPerms, g.APIKey, "Default API key").Error; err != nil {
					return err
				}
			}
			return nil
		})
	}

	executorsTable := (&ExecutionNode{}).TableName()
	// If the table doesn't have group_id column yet, drop the whole table so that GORM re-creates it with a new
	// primary key that includes group_id.
	if m.HasTable(executorsTable) && !m.HasColumn(&ExecutionNode{}, "group_id") {
		if err := m.DropTable(executorsTable); err != nil {
			return nil, err
		}
	}

	return postMigrate, nil
}

// Manual migration called after auto-migration.
func PostAutoMigrate(db *gorm.DB) error {
	return nil
}

func init() {
	registerTable("IN", &Invocation{})
	registerTable("CA", &CacheEntry{})
	registerTable("US", &User{})
	registerTable("GR", &Group{})
	registerTable("UG", &UserGroup{})
	registerTable("AK", &APIKey{})
	registerTable("TO", &Token{})
	registerTable("EX", &Execution{})
	registerTable("TL", &TelemetryLog{})
	registerTable("EN", &ExecutionNode{})
	registerTable("ET", &ExecutionTask{})
	registerTable("CL", &CacheLog{})
	registerTable("TA", &Target{})
	registerTable("TS", &TargetStatus{})
	registerTable("WF", &Workflow{})
}
