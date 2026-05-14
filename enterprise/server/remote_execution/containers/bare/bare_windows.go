//go:build windows

package bare

const (
	// Use TMP as documented by GetTmpPathA api, which is used in googletest.
	// https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-gettemppatha#remarks
	tmpdirEnvironmentVariableName = "TMP"
)
