package gormutil

import "gorm.io/gorm"

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
