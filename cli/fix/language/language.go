// Package language defines the interface for language-specific handling in the
// `bb fix` command. Each language implementation can identify its source files,
// dependency files (like package.json or go.mod), specify required Bazel
// dependencies, consolidate dependency files, and register dependencies in
// MODULE.bazel or WORKSPACE.
package language

type Language interface {
	// Returns any WORKSPACE or MODULE dependencies needed for this language.
	Deps() []string
	// Returns true if the file is a source file written in this language.
	IsSourceFile(path string) bool
	// Returns true if the file is a dependency file used by this langugae.
	IsDepFile(path string) bool
	// Gives the language an opportunity to consolidate multiple dep files before update-repos is called.
	ConsolidateDepFiles(deps map[string][]string) map[string][]string
	// Allows the language to register any dependencies in the module file.
	RegisterDeps(path string, modulePath string)
}
