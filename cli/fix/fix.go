package fix

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/fix/language"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/translate"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"

	langs "github.com/buildbuddy-io/buildbuddy/cli/fix/langs"

	gazelle "github.com/bazelbuild/bazel-gazelle/cmd/gazelle"
	buildifier "github.com/bazelbuild/buildtools/buildifier"
)

var (
	flags = flag.NewFlagSet("fix", flag.ContinueOnError)
	diff  = flags.Bool("diff", false, "Don't apply fixes, just print a diff showing the changes that would be applied.")
)

const (
	usage = `
usage: bb fix [ --diff ]

Applies fixes to WORKSPACE and BUILD files.
Use the --diff flag to print suggested fixes without applying.
`
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

func HandleFix(args []string) (exitCode int, err error) {
	command, idx := arg.GetCommandAndIndex(args)
	if command != "fix" {
		return -1, nil
	}

	if idx != 0 {
		log.Debugf("Unexpected flag: %s", args[0])
		return 1, nil
	}

	if err := arg.ParseFlagSet(flags, args[idx+1:]); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			flags.SetOutput(os.Stderr)
			flags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}

	_, _, err = workspace.CreateWorkspaceFileIfNotExists()
	if err != nil {
		return 1, err
	}

	err = walk()
	if err != nil {
		log.Printf("Error fixing: %s", err)
	}

	runGazelle()

	return 0, nil
}

func runGazelle() {
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()
	os.Args = []string{"gazelle"}
	if *diff {
		os.Args = append(os.Args, "-mode=diff")
	}
	log.Debugf("Calling gazelle with args: %+v", os.Args)
	gazelle.Run()
}

func walk() error {
	languages := getLanguages()
	foundLanguages := map[language.Language]bool{}
	depFiles := map[string][]string{}
	err := filepath.WalkDir(".",
		func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			fileName := filepath.Base(path)
			// Collect any languages and their dep files we found used in the repo.
			for _, l := range languages {
				if l.IsSourceFile(path) {
					foundLanguages[l] = true
				}
				if !l.IsDepFile(path) {
					continue
				}
				foundLanguages[l] = true
				if _, ok := depFiles[fileName]; !ok {
					depFiles[fileName] = []string{}
				}
				depFiles[fileName] = append(depFiles[fileName], path)
			}
			fileNameRoot := strings.TrimSuffix(fileName, filepath.Ext(fileName))
			if fileNameRoot != "BUILD" && fileNameRoot != "WORKSPACE" {
				return nil
			}
			fileToFormat, err := translate.Translate(path)
			if err != nil {
				return err
			}
			if fileToFormat == "" {
				return nil
			}
			runBuildifier(fileToFormat)
			return nil
		})
	if err != nil {
		return err
	}

	if *diff {
		// TODO: support diff mode for other fixes
		return nil
	}

	// Add any necessary dependencies for languages that are used in the repo.
	for l := range foundLanguages {
		for _, d := range l.Deps() {
			log.Debugf("Adding %s", d)
			_, err := add.HandleAdd([]string{"add", d})
			if err != nil {
				log.Debugf("Failed adding %s: %s", d, err)
			}
			depFiles = l.ConsolidateDepFiles(depFiles)
		}
	}

	// Run update-repos on any dependency files we found.
	for _, paths := range depFiles {
		for _, p := range paths {
			runUpdateRepos(p)
		}
	}

	return nil
}

// Collect the languages that support auto-generating WORKSPACE files.
func getLanguages() []language.Language {
	languages := make([]language.Language, 0)
	for _, l := range langs.Languages {
		l, ok := l.(language.Language)
		if ok {
			languages = append(languages, l)
		}
	}
	return languages
}

func runBuildifier(path string) {
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()
	os.Args = []string{"buildifier"}
	if *diff {
		os.Args = append(os.Args, "-mode=diff")
	}
	os.Args = append(os.Args, path)
	buildifier.Run()
}

func runUpdateRepos(path string) {
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()
	funcName := fmt.Sprintf("install_%s_dependencies", nonAlphanumericRegex.ReplaceAllString(path, "_"))
	os.Args = []string{"gazelle", "update-repos", "--from_file=" + path, "--to_macro=deps.bzl%" + funcName}
	log.Debugf("Calling gazelle with args: %+v", os.Args)
	gazelle.Run()
}
