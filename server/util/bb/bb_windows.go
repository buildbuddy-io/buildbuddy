package bb

// CLIBytes is nil on Windows because the CLI's tree-sitter dependency requires
// CGo. Callers that require the embedded CLI must report it as unavailable.
var CLIBytes []byte
