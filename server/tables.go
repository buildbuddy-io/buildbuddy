package tables

import (
	"fmt"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/jinzhu/gorm"

	inpb "proto/invocation"
	uspb "proto/user"
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
	InvocationID     string `gorm:"primary_key;"`
	Success          bool
	User             string `gorm:"index:user_index"`
	DurationUsec     int64
	Host             string `gorm:"index:host_index"`
	Command          string
	Pattern          string
	ActionCount      int64
	BlobID           string
	InvocationStatus int64 `gorm:"index:invocation_status_idx"`
}

func (i *Invocation) TableName() string {
	return "Invocations"
}

func (i *Invocation) FromProtoAndBlobID(p *inpb.Invocation, blobID string) {
	i.InvocationID = p.InvocationId // Required.
	i.Success = p.Success
	i.User = p.User
	i.DurationUsec = p.DurationUsec
	i.Host = p.Host
	i.Command = p.Command
	if p.Pattern != nil {
		i.Pattern = strings.Join(p.Pattern, ", ")
	}
	i.ActionCount = p.ActionCount
	i.BlobID = blobID
	i.InvocationStatus = int64(p.InvocationStatus)
}

func (i *Invocation) ToProto() *inpb.Invocation {
	out := &inpb.Invocation{}
	out.InvocationId = i.InvocationID // Required.
	out.Success = i.Success
	out.User = i.User
	out.DurationUsec = i.DurationUsec
	out.Host = i.Host
	out.Command = i.Command
	if i.Pattern != "" {
		out.Pattern = strings.Split(i.Pattern, ", ")
	}
	out.ActionCount = i.ActionCount
	// BlobID is not present in output client proto.
	out.InvocationStatus = inpb.Invocation_InvocationStatus(i.InvocationStatus)
	return out
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

	// The "owned" domain. In enterprise/cloud version, we create a
	// group for a customer's domain, and new users that sign up with an
	// email belonging to that domain may be added to this group.
	OwnedDomain string `gorm:"index:owned_domain_index"`

	// The group access token. This token allows writing data for this
	// group.
	WriteToken string
}

func (g *Group) TableName() string {
	return "Groups"
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
	Groups []*Group `gorm:"many2many:user_groups;"`

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

func init() {
	registerTable("IN", &Invocation{})
	registerTable("CA", &CacheEntry{})
	registerTable("US", &User{})
	registerTable("GR", &Group{})
	registerTable("TO", &Token{})
}
