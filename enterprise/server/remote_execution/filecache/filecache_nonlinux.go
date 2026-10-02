//go:build !linux

package filecache

// getStatFunc returns a metadata reader using ordinary stat and ctime.
func getStatFunc(rootDir string) func(string) (fileMetadata, error) {
	return statFile
}
