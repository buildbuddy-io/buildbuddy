package config

import "flag"

var tagsEnabled = flag.Bool("app.tags_enabled", false, "Enable setting tags on invocations via build_metadata")

func TagsEnabled() bool {
	return *tagsEnabled
}
