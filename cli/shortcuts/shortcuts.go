// Package shortcuts defines command aliases for the BuildBuddy CLI, mapping
// short command names (like 'b', 't', 'q') to their full command names (build,
// test, query) for faster typing.
package shortcuts

var (
	Shortcuts = map[string]string{
		"b": "build",
		"t": "test",
		"q": "query",
		"r": "run",
		"f": "fix",
	}
)
