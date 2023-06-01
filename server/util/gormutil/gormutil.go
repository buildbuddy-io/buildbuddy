package gormutil

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// InstrumentMetrics registers callback functions before and after other
// GORM callbacks run
func InstrumentMetrics(gdb *gorm.DB, beforeKey string, beforeFn func(*gorm.DB), afterKey string, afterFn func(*gorm.DB)) {
	// Add callback that runs before other callbacks that records when the operation began.
	// We use this to calculate how long a query takes to run.
	gdb.Callback().Create().Before("*").Register(beforeKey, beforeFn)
	gdb.Callback().Delete().Before("*").Register(beforeKey, beforeFn)
	gdb.Callback().Query().Before("*").Register(beforeKey, beforeFn)
	gdb.Callback().Raw().Before("*").Register(beforeKey, beforeFn)
	gdb.Callback().Row().Before("*").Register(beforeKey, beforeFn)
	gdb.Callback().Update().Before("*").Register(beforeKey, beforeFn)

	// Add callback that runs after other callbacks that records executed queries and their durations.
	gdb.Callback().Create().After("*").Register(afterKey, afterFn)
	gdb.Callback().Delete().After("*").Register(afterKey, afterFn)
	gdb.Callback().Query().After("*").Register(afterKey, afterFn)
	gdb.Callback().Raw().After("*").Register(afterKey, afterFn)
	gdb.Callback().Row().After("*").Register(afterKey, afterFn)
	gdb.Callback().Update().After("*").Register(afterKey, afterFn)
}

// RegisterLogSQLCallback adds a callback to the db that appends all SQL executed to a slice of strings
func RegisterLogSQLCallback(db *gorm.DB, s *[]string) error {
	// Replace every gorm raw SQL command with a function that appends the SQL string to a slice
	if err := db.Callback().Raw().Replace("gorm:raw", func(db *gorm.DB) {
		sqlToExecute := db.Statement.SQL.String()
		if strings.HasPrefix(sqlToExecute, "SELECT") {
			return
		}
		*s = append(*s, sqlToExecute)
	}); err != nil {
		return err
	}
	return nil
}

func PrintMigrationSchemaChanges(sqlStrings []string) {
	// Print schema changes to stdout
	// Logs go to stderr, so this output will be easy to isolate and parse
	if len(sqlStrings) > 0 {
		for _, sqlStr := range sqlStrings {
			// Filter out `CREATE TABLE` operations because they are fast, backwards-compatible schema changes that are
			// always safe for Gorm to auto-apply
			if strings.HasPrefix(sqlStr, "CREATE TABLE") {
				continue
			}
			fmt.Println(sqlStr)
		}
	}
}
