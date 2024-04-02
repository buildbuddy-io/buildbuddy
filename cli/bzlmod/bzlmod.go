package bzlmod

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
)

var Enabled = sync.OnceValues(func() (bool, error) {
	infos, err := bazelisk.BazelInfo([]string{"release", "starlark-semantics"})
	if err != nil {
		return false, fmt.Errorf("could not run bazel info: %w", err)
	}
	starlarkSemantics, ok := infos["starlark-semantics"]
	if !ok {
		return false, fmt.Errorf("could not find `starlark-semantics` in `bazel info` result: %v", infos)
	}

	// It's possible that bzlmod is enabled/disabled explicitly inside a bazelrc file.
	// If that's the case, starlark-semantics should show the status if the value is not the default value
	if strings.Contains(starlarkSemantics, "enable_bzlmod=true") {
		return true, nil
	}
	if strings.Contains(starlarkSemantics, "enable_bzlmod=false") {
		return false, nil
	}

	release, ok := infos["release"]
	if !ok {
		return false, fmt.Errorf("could not find `release` in `bazel info` result: %v", infos)
	}
	version := strings.TrimLeft(release, "release ")
	versionParts := strings.Split(version, ".")
	majorVersion, err := strconv.Atoi(versionParts[0])
	if err != nil {
		return false, fmt.Errorf("could not parse Bazel release version: %v", err)
	}

	// From Bazel 7 on-ward, bzlmod is enabled by default and will not be shown
	// in starlark-semantics setting.
	if majorVersion <= 6 {
		return false, nil
	}

	return true, nil
})
