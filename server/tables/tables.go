package tables

import (
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/jinzhu/gorm"

	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
)

const (
	mySQLDialect = "mysql"
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
	Model
	InvocationID           string `gorm:"primary_key;"`
	UserID                 string `gorm:"index:user_id"`
	GroupID                string `gorm:"index:group_id"`
	Perms                  int    `gorm:"index:perms"`
	Success                bool
	User                   string `gorm:"index:user_index"`
	DurationUsec           int64
	Host                   string `gorm:"index:host_index"`
	RepoURL                string `gorm:"index:repo_url_index"`
	CommitSHA              string `gorm:"index:commit_sha_index"`
	Role                   string `gorm:"index:role_index"`
	Command                string
	Pattern                string `gorm:"type:text;"`
	ActionCount            int64
	BlobID                 string
	InvocationStatus       int64 `gorm:"index:invocation_status_idx"`
	ActionCacheHits        int64
	ActionCacheMisses      int64
	ActionCacheUploads     int64
	CasCacheHits           int64
	CasCacheMisses         int64
	CasCacheUploads        int64
	TotalDownloadSizeBytes int64
	TotalUploadSizeBytes   int64
	TotalDownloadUsec      int64
	TotalUploadUsec        int64
}

func (i *Invocation) TableName() string {
	return "Invocations"
}

type CacheEntry struct {
	Model
	EntryID            string `gorm:"primary_key;"`
	ExpirationTimeUsec int64
	SizeBytes          int64
	ReadCount          int64
}

func (c *CacheEntry) TableName() string {
	return "CacheEntries"
}

type Group struct {
	Model
	// The group ID -- a unique ID.

	GroupID string `gorm:"primary_key;"`
	// The user that OWNS this group. Only this user may modify it.
	UserID string

	// The group name. This may be displayed to users.
	Name string

	// An unique URL segment that is displayed in group-related URLs.
	// e.g. "example-org" in app.buildbuddy.com/join/example-org or
	// "example-org.buildbuddy.com" if we support subdomains in the future.
	URLIdentifier string `gorm:"index:url_identifier_index"`

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
}

func (g *Group) TableName() string {
	return "Groups"
}

type UserGroup struct {
	UserUserID   string `gorm:"primary_key"`
	GroupGroupID string `gorm:"primary_key"`

	// The user's membership status.
	// Values correspond to `GroupMembershipStatus` enum values in `grp.proto`.
	MembershipStatus int32 `gorm:"index:membership_status_index"`
}

func (ug *UserGroup) TableName() string {
	return "UserGroups"
}

type User struct {
	Model

	// The buildbuddy user ID.
	UserID string `gorm:"primary_key;"`

	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	SubID string `gorm:"index:sub_id_index"`

	// Groups are used to determine read/write permissions
	// for everything.
	Groups []*Group `gorm:"-"` // gorm ignore

	// Profile information etc.
	FirstName string
	LastName  string
	Email     string
	ImageURL  string
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
	Model
	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	SubID        string `gorm:"primary_key"`
	AccessToken  string
	RefreshToken string
	ExpiryUsec   int64
}

func (t *Token) TableName() string {
	return "Tokens"
}

type Execution struct {
	Model
	// The subscriber ID, a concatenated string of the
	// auth Issuer ID and the subcriber ID string.
	ExecutionID string `gorm:"primary_key"`
	UserID      string `gorm:"index:user_id"`
	GroupID     string `gorm:"index:group_id"`
	Perms       int    `gorm:"index:perms"`

	Stage               int64
	SerializedOperation []byte `gorm:"size:max"` // deprecated.
	StatusCode          int32
	StatusMessage       string
	CachedResult        bool
	InvocationID        string `gorm:"index:execution_invocation_id"`

	// IOStats
	FileDownloadCount        int64
	FileDownloadSizeBytes    int64
	FileDownloadDurationUsec int64
	FileUploadCount          int64
	FileUploadSizeBytes      int64
	FileUploadDurationUsec   int64

	// ExecutedActionMetadata
	Worker                             string
	QueuedTimestampUsec                int64
	WorkerStartTimestampUsec           int64
	WorkerCompletedTimestampUsec       int64
	InputFetchStartTimestampUsec       int64
	InputFetchCompletedTimestampUsec   int64
	ExecutionStartTimestampUsec        int64
	ExecutionCompletedTimestampUsec    int64
	OutputUploadStartTimestampUsec     int64
	OutputUploadCompletedTimestampUsec int64
}

func (t *Execution) TableName() string {
	return "Executions"
}

type TelemetryLog struct {
	Model
	InstallationUUID    string `gorm:"primary_key"`
	InstanceUUID        string `gorm:"primary_key"`
	TelemetryLogUUID    string `gorm:"primary_key"`
	RecordedAtUsec      int64
	AppVersion          string
	AppURL              string
	Hostname            string
	InvocationCount     int64
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
	Model
	Host                  string `gorm:"primary_key"`
	Port                  int32  `gorm:"primary_key;auto_increment:false"`
	AssignableMemoryBytes int64
	AssignableMilliCPU    int64
	Constraints           string
}

func (n *ExecutionNode) TableName() string {
	return "ExecutionNodes"
}

type ExecutionTask struct {
	Model
	TaskID               string `gorm:"primary_key"`
	SerializedTask       []byte `gorm:"size:max"`
	EstimatedMemoryBytes int64
	EstimatedMilliCPU    int64
	ClaimedAtUsec        int64
	AttemptCount         int64
	OS                   string
}

func (n *ExecutionTask) TableName() string {
	return "ExecutionTasks"
}

type CacheLog struct {
	Model
	InvocationID       string `gorm:"primary_key"`
	JoinKey            string `gorm:"primary_key"`
	DigestHash         string
	RemoteInstanceName string
	SerializedProto    []byte `gorm:"size:max"`
}

func (c *CacheLog) TableName() string {
	return "CacheLogs"
}

func ManualMigrate(db *gorm.DB) error {
	// These types don't apply for sqlite -- just mysql.
	if db.Dialect().GetName() == mySQLDialect {
		db.Model(&Invocation{}).ModifyColumn("pattern", "text")
		db.Model(&Execution{}).ModifyColumn("serialized_operation", "text")
	}
	return nil
}

func init() {
	registerTable("IN", &Invocation{})
	registerTable("CA", &CacheEntry{})
	registerTable("US", &User{})
	registerTable("GR", &Group{})
	registerTable("UG", &UserGroup{})
	registerTable("TO", &Token{})
	registerTable("EX", &Execution{})
	registerTable("TL", &TelemetryLog{})
	registerTable("EN", &ExecutionNode{})
	registerTable("ET", &ExecutionTask{})
	registerTable("CL", &CacheLog{})
}
