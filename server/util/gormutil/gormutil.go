package gormutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	gormutils "gorm.io/gorm/utils"
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

// Logger implements GORM's logger.Interface using zerolog.
type Logger struct {
	SlowThreshold time.Duration
	LogLevel      logger.LogLevel
}

func (l *Logger) Info(ctx context.Context, format string, args ...any) {
	log.CtxInfof(ctx, "%s: "+format, append([]any{gormutils.FileWithLineNum()}, args...)...)
}
func (l *Logger) Warn(ctx context.Context, format string, args ...any) {
	log.CtxWarningf(ctx, "%s: "+format, append([]any{gormutils.FileWithLineNum()}, args...)...)
}
func (l *Logger) Error(ctx context.Context, format string, args ...any) {
	log.CtxErrorf(ctx, "%s: "+format, append([]any{gormutils.FileWithLineNum()}, args...)...)
}

// Trace is called after every SQL query. If `database.log_queries` is true then
// it will always log the query. Otherwise it will only log slow or failed
// queries. NotFound errors are ignored.
func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}
	duration := time.Since(begin)
	getInfo := func() string {
		sql, rows := fc()
		rowsVal := any(rows)
		if rows <= 0 {
			// rows < 0 means the query does not have an associated row count.
			rowsVal = "-"
		}
		return fmt.Sprintf("(duration: %s) (rows: %v) %s", duration, rowsVal, sql)
	}
	switch {
	case err != nil && l.LogLevel >= logger.Error && !errors.Is(err, gorm.ErrRecordNotFound):
		log.CtxErrorf(ctx, "SQL: error (%s): %s %s", gormutils.FileWithLineNum(), err, getInfo())
	case duration > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= logger.Warn:
		log.CtxWarningf(ctx, "SQL: slow query (over %s) (%s): %s", l.SlowThreshold, gormutils.FileWithLineNum(), getInfo())
	case l.LogLevel == logger.Info:
		log.CtxInfof(ctx, "SQL: OK (%s) %s", gormutils.FileWithLineNum(), getInfo())
	}
}

func (l *Logger) LogMode(level logger.LogLevel) logger.Interface {
	clone := *l
	clone.LogLevel = level
	return &clone
}

// ParamsFilter implements gorm's ParamsFilter interface, ensuring that queries
// are logged without parameter values showing up in the logs.
func (l *Logger) ParamsFilter(ctx context.Context, sql string, params ...interface{}) (string, []interface{}) {
	return sql, nil
}
