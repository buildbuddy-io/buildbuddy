package tables

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	inpb "proto/invocation"
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
	once      sync.Once
)

func GetAllTables() []interface{} {
	tableSlice := make([]interface{}, 0)
	for _, d := range allTables {
		tableSlice = append(tableSlice, d.table)
	}
	return tableSlice
}

func randUint64() uint64 {
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
		log.Printf("Seeded random with current time!")
	})

	return rand.Uint64()
}

func PrimaryKeyForTable(tableName string) (string, error) {
	for _, d := range allTables {
		if d.name == tableName {
			return fmt.Sprintf("%s%d", d.prefix, randUint64()), nil
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
	nowInt64 := int64(time.Now().UnixNano())
	m.CreatedAtUsec = nowInt64
	m.UpdatedAtUsec = nowInt64
	return nil
}

func (m *Model) BeforeUpdate(tx *gorm.DB) (err error) {
	m.UpdatedAtUsec = int64(time.Now().UnixNano())
	return nil
}

type Invocation struct {
	Model
	InvocationID string `gorm:"primary_key;"`
	Success      bool
	User         string
	DurationUsec int64
	Host         string
	Command      string
	Pattern      string
	ActionCount  int64
	BlobID       string
}

func (i *Invocation) TableName() string {
	return "Invocations"
}

func (i *Invocation) FromProto(p *inpb.Invocation) {
	i.InvocationID = p.InvocationId
	i.Success = p.Success
	i.User = p.User
	i.DurationUsec = p.DurationUsec
	i.Host = p.Host
	i.Command = p.Command
	i.Pattern = strings.Join(p.Pattern, ", ")
	i.ActionCount = p.ActionCount
}

func init() {
	registerTable("IN", &Invocation{})
}
