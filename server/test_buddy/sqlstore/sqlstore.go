// Package sqlstore stores TestBuddy state in a relational database.
package sqlstore

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

// The natural test key can be 2,627 bytes. Binary identity columns keep it
// below InnoDB's 3,072-byte index limit.
const mysqlTableOptions = "ENGINE=InnoDB DEFAULT CHARSET=binary"

type testRow struct {
	GroupID     string `gorm:"primaryKey;size:64;not null;index:test_cone_idx,priority:1"`
	Repository  string `gorm:"primaryKey;size:512;not null;index:test_cone_idx,priority:2"`
	TargetLabel string `gorm:"primaryKey;size:1539;not null"`
	CaseName    string `gorm:"primaryKey;size:512;not null"`
	PackagePath string `gorm:"size:1024;not null;index:test_cone_idx,priority:3"`
	Disposition int32  `gorm:"not null;default:0"`

	Health              string `gorm:"size:32;not null"`
	AnalyzerState       []byte `gorm:"size:max;not null"`
	PassCount           int64
	FailCount           int64
	TimeoutCount        int64
	BrokenCount         int64
	TotalDurationUsec   int64
	StateVersion        int64  `gorm:"not null"`
	AnalyzerRevision    int64  `gorm:"not null"`
	AnalysisReason      string `gorm:"size:64;not null"`
	EligibleSampleCount int64  `gorm:"not null"`
}

func (*testRow) TableName() string { return "Tests" }

type analyzerConfigRow struct {
	GroupID    string `gorm:"primaryKey;size:64;not null"`
	Repository string `gorm:"primaryKey;size:512;not null"`
	Revision   int64  `gorm:"not null"`
	Config     []byte `gorm:"size:max;not null"`
}

func (*analyzerConfigRow) TableName() string { return "TestAnalyzerConfigs" }

type stateChangeRow struct {
	GroupID      string `gorm:"primaryKey;size:64;not null"`
	Repository   string `gorm:"primaryKey;size:512;not null"`
	TargetLabel  string `gorm:"primaryKey;size:1539;not null"`
	CaseName     string `gorm:"primaryKey;size:512;not null"`
	StateVersion int64  `gorm:"primaryKey;autoIncrement:false;not null"`

	PreviousHealth      string `gorm:"size:32;not null"`
	Health              string `gorm:"size:32;not null"`
	TransitionTimeUsec  int64  `gorm:"not null"`
	AnalyzerRevision    int64  `gorm:"not null"`
	AnalysisReason      string `gorm:"size:64;not null"`
	EligibleSampleCount int64  `gorm:"not null"`
}

func (*stateChangeRow) TableName() string { return "TestStateChanges" }

func tables() []any {
	return []any{&testRow{}, &analyzerConfigRow{}, &stateChangeRow{}}
}

func Migrate(ctx context.Context, database interfaces.DBHandle) error {
	gdb := database.GORM(ctx, "test_buddy_migrate")
	if database.DialectName() == "mysql" {
		for _, table := range tables() {
			if gdb.Migrator().HasTable(table) {
				continue
			}
			if err := gdb.Set("gorm:table_options", " "+mysqlTableOptions).Migrator().CreateTable(table); err != nil {
				return err
			}
		}
	}
	return gdb.AutoMigrate(tables()...)
}
