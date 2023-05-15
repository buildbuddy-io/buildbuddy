package fix

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"

	gazelle "github.com/bazelbuild/bazel-gazelle/cmd/gazelle"
	buildifier "github.com/bazelbuild/buildtools/buildifier"
)

func HandleFix(args []string) (exitCode int, err error) {
	command, idx := arg.GetCommandAndIndex(args)
	if command != "fix" {
		return -1, nil
	}

	if idx != 0 {
		log.Debugf("Unexpected flag: %s", args[0])
		return 1, nil
	}

	// Run gazelle with the transformed args so far (e.g. if we ran bb with
	// `--verbose=1`, this will make sure we don't pass `--verbose=1` to
	// gazelle, which doesn't understand that flag).
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()
	os.Args = args
	gazelle.Run()

	err = walk()
	if err != nil {
		log.Printf("Error fixing: %+v", err)
	}
	return 0, nil
}

func walk() error {
	return filepath.WalkDir(".",
		func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			fileName := filepath.Base(path)
			fileExt := filepath.Ext(fileName)
			fileNameRoot := strings.TrimSuffix(fileName, fileExt)
			if (fileNameRoot != "BUILD" && fileNameRoot != "WORKSPACE") ||
				(fileExt != "" && fileExt != ".bazel") {
				return nil
			}
			runBuildifier(path)
			return nil
		})
}

func runBuildifier(path string) {
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()
	os.Args = []string{"buildifier", path}
	buildifier.Run()
}
