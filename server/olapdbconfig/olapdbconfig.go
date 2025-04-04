package olapdbconfig

import (
	"flag"
)

var (
	writeExecutionsToOLAPDBEnabled = flag.Bool("app.enable_write_executions_to_olap_db", true, "If enabled, complete Executions will be flushed to OLAP DB")
)

func WriteExecutionsToOLAPDBEnabled() bool {
	return *writeExecutionsToOLAPDBEnabled
}
