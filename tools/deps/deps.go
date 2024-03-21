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
package deps

import (
	// Device manager
	_ "gitlab.com/arm-research/smarter/smarter-device-manager"

	// Gazelle deps for 'bb fix'
	_ "github.com/pmezard/go-difflib/difflib"

	// rules_webtesting deps
	_ "github.com/gorilla/mux"

	// static analyzers
	_ "github.com/nishanths/exhaustive"
	_ "golang.org/x/tools/go/analysis"
	_ "honnef.co/go/tools/staticcheck"

	// Protobuf and grpc deps
	_ "cloud.google.com/go/longrunning"
	_ "github.com/planetscale/vtprotobuf/vtproto"
	_ "google.golang.org/genproto/googleapis/api/annotations"
)
