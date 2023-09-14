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
}
