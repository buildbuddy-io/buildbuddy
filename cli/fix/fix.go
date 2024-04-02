package fix

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/bzlmod"
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
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			flags.SetOutput(os.Stderr)
			flags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}

	bzlmodEnabled, err := bzlmod.Enabled()
	if err != nil {
		return 1, err
	}
	path, baseFile, err := workspace.CreateWorkspaceIfNotExists(bzlmodEnabled)
	if err != nil {
		return 1, err
	}

	// don't run update-repos in bzlmod
	updateRepos := baseFile != workspace.ModuleFileName
	if err := walk(updateRepos); err != nil {
		log.Printf("Error fixing: %s", err)
	}

	if err := runGazelle(path, baseFile); err != nil {
		return 1, err
	}

	return 0, nil
}

const goRepositoryConfigLocation = "@@gazelle~override~go_deps~bazel_gazelle_go_repository_config//:WORKSPACE"

func gazelleConfig() (string, error) {
	// we intentionally skip stderr here for both
	// bazelisk and bazel to avoid failing checkstyle.sh
	bazelArgs := []string{
		"query",
		"--ui_event_filters=-info,-debug,-warning,-stderr",
		"--noshow_progress",
		"--logging=0",
		"--output=location",
		goRepositoryConfigLocation,
	}
	stdout := &bytes.Buffer{}
	opts := &bazelisk.RunOpts{
		Stdout: stdout,
	}
	_, err := bazelisk.Run(bazelArgs, opts)
	if err != nil {
		return "", err
	}

	// output will be in the form of
	//   /some/path/config/WORKSPACE:1:1 source file <target>
	// extract `/some/path/config/WORKSPACE` from that.
	out := stdout.String()
	fragments := strings.Split(out, " ")
	locations := strings.Split(fragments[0], ":")
	return locations[0], nil
}

func runGazelle(repoRoot, baseFile string) error {
	originalArgs := os.Args
	defer func() {
		os.Args = originalArgs
	}()

	os.Args = []string{"gazelle", "update"}
	if baseFile == workspace.ModuleFileName {
		configPath, err := gazelleConfig()
		if err != nil {
			return err
		}
		os.Args = append(os.Args, "-bzlmod", "-repo_config="+configPath)
	} else {
		os.Args = append(os.Args, "-repo_root="+repoRoot, "-go_prefix=")
	}

	if *diff {
		os.Args = append(os.Args, "-mode=diff")
	}
	log.Debugf("Calling gazelle with args: %+v", os.Args)
	gazelle.Run()
	return nil
}

func walk(updateRepos bool) error {
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
			// .bzl files are formatted directly by buildifier.
			if strings.HasSuffix(path, ".bzl") {
				runBuildifier(path)
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
			if fileNameRoot != "BUILD" && fileNameRoot != "WORKSPACE" && fileNameRoot != "MODULE" {
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
			_, err := add.HandleAdd([]string{d})
			if err != nil {
				log.Debugf("Failed adding %s: %s", d, err)
			}
			depFiles = l.ConsolidateDepFiles(depFiles)
		}
	}

	if updateRepos {
		// Run update-repos on any dependency files we found.
		for _, paths := range depFiles {
			for _, p := range paths {
				runUpdateRepos(p)
			}
		}
	}

	return nil
}

// Collect the languages that support auto-generating WORKSPACE files.
func getLanguages() []language.Language {
	var languages []language.Language
	for _, l := range langs.Languages {
		if l, ok := l.(language.Language); ok {
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
		os.Args = append(
			os.Args,
			"-mode=diff",
			// Pass -u arg to diff for slightly nicer output.
			"-diff_command=diff -u",
		)
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
