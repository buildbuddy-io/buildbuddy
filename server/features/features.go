package features

import "github.com/buildbuddy-io/buildbuddy/server/util/flag"

var (
	CodeEditorEnabled   = flag.Bool("app.code_editor_enabled", false, "If set, code editor functionality will be enabled.")
	CodeEditorV2Enabled = flag.Bool("app.code_editor_v2_enabled", false, "If set, show v2 of code editor that stores state on server instead of local storage.")
)
