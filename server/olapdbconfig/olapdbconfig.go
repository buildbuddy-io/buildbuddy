package olapdbconfig

import (
	"flag"
)

var (
	writeExecutionsToOLAPDBEnabled = flag.Bool("app.enable_write_executions_to_olap_db", true, "If enabled, complete Executions will be flushed to OLAP DB")

	writeAllInvocationsToOLAPDBEnabled = flag.Bool("app.enable_write_all_invocations_to_olap_db", false, "If enabled, invocations are dual-written to the ClickHouse AllInvocations table for groups enrolled via the olap.write_all_invocations experiment.")
)

func WriteExecutionsToOLAPDBEnabled() bool {
	return *writeExecutionsToOLAPDBEnabled
}

func WriteAllInvocationsToOLAPDBEnabled() bool {
	return *writeAllInvocationsToOLAPDBEnabled
}
