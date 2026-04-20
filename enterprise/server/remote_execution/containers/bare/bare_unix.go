//go:build (darwin || linux) && !android && !ios

package bare

const (
	// TMPDIR environment variable used to specify a directory made available
	// for programs that need a place to create temporary files. This name is
	// standardized by POSIX and is widely supported:
	// https://pubs.opengroup.org/onlinepubs/9799919799/basedefs/V1_chap08.html#tag_08_01
	tmpdirEnvironmentVariableName = "TMPDIR"
)
