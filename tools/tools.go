//go:build tools

// This file is used to mark Go modules as used that are only (deps of) tools run during the build.
package tools

import (
	// Used by cli/explain/compactgraph/testdata/generate to generate test data.
	_ "github.com/otiai10/copy"
)
