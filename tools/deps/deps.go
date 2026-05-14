//go:build tools

// This tools/deps package keep track of Go tooling dependency that we use
// but do not depend directly in code.
// See https://github.com/bazelbuild/rules_go/blob/master/docs/go/core/bzlmod.md#depending-on-tools
//
// Importing them here helps nudge "go mod tidy" to include them inside
// "go.mod" and "go.sum" files, which are used as source of truth for rules_go
// and gazelle under bzlmod.
//
// The package is shielded with a "go:build tools" directive.
// Without a specific flag, gazelle should NOT generate go targets for this
// package.
//
// Deprecated: use "go get tool <tool_name>" to add the tool directly into go.mod file instead.
package deps

import (
	// Protobuf and grpc deps
	// We still keep these here so that they remain in the direct require portion of the go.mod file.
	// This prevents "go mod tidy -e" inconsistent formatting for "pbsync" users.
	_ "github.com/planetscale/vtprotobuf/vtproto"
	_ "google.golang.org/genproto/googleapis/api/annotations"
)
