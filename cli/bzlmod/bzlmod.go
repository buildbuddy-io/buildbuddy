package bzlmod

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
)

var (
	once       sync.Once
	useModules bool
	infoErr    error
)

func UseModules() (bool, error) {
	once.Do(func() {
		infos, err := bazelisk.BazelInfo([]string{"release", "starlark-semantics"})
		if err != nil {
			useModules = false
			infoErr = fmt.Errorf("could not run bazel info: %w", err)
			return
		}
		starlarkSemantics, ok := infos["starlark-semantics"]
		if !ok {
			useModules = false
			infoErr = fmt.Errorf("could not find `starlark-semantics` in `bazel info` result: %v", infos)
			return
		}

		// It's possible that bzlmod is enabled/disabled explicitly inside a bazelrc file.
		// If that's the case, starlark-semantics should show the status if the value is not the default value
		if strings.Contains(starlarkSemantics, "enable_bzlmod=true") {
			useModules = true
			return
		}
		if strings.Contains(starlarkSemantics, "enable_bzlmod=false") {
			useModules = false
			return
		}

		release, ok := infos["release"]
		if !ok {
			useModules = false
			infoErr = fmt.Errorf("could not find `release` in `bazel info` result: %v", infos)
			return
		}
		version := strings.TrimLeft(release, "release ")
		versionParts := strings.Split(version, ".")
		majorVersion, err := strconv.Atoi(versionParts[0])
		if err != nil {
			useModules = false
			infoErr = fmt.Errorf("could not parse Bazel release version: %v", err)
			return
		}

		// From Bazel 7 on-ward, bzlmod is enabled by default and will not be shown
		// in starlark-semantics setting.
		if majorVersion <= 6 {
			// If current version is before Bazel 7, then bzlmod is disabled by default.
			useModules = false
			return
		}
		useModules = true
	})

	return useModules, infoErr
}
