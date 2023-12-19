package common

import (
	_ "embed"
)

//go:embed version_flag.txt
var cliVersionFlag string

func Version() string {
	return cliVersionFlag
}
