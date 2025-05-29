//go:build windows

package workspace

import (
	"crypto/rand"
	"encoding/base64"
)

func newRandomBuildDirCandidate() string {
	// On Windows, some tools (mostly MSVC) have a 260-character limit on all
	// paths. Moreover, due to https://github.com/bazelbuild/bazel/issues/19733,
	// C++ header validation can fail if the absolute build dir path doesn't
	// end with `execroot\\_main` (with Bzlmod enabled; there is no guarantee
	// that any particular path is correct for WORKSPACE builds). We thus
	// use a short base64-encoded string and append `execroot` to it.
	// https://github.com/bazelbuild/bazel/blob/819aa9688229e244dc90dda1278d7444d910b48a/src/main/java/com/google/devtools/build/lib/rules/cpp/ShowIncludesFilter.java#L101
	b := make([]byte, 8)
	rand.Read(b)
	shortId := base64.RawURLEncoding.EncodeToString(b)
	return shortId + "execroot"
}
