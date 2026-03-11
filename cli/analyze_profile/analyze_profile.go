package analyze_profile

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/timing_profile"
)

var (
	flags = flag.NewFlagSet("analyze-profile", flag.ContinueOnError)

	profilePath = flags.String("profile", "", "Path to a Bazel timing profile file (.profile, .profile.gz, etc.)")
	topN        = flags.Int("top_n", timing_profile.DefaultTopN, "How many top spans/categories/threads to include")

	usage = `
usage: bb analyze-profile --profile=<path> [--top_n=10]

Analyzes a Bazel timing profile file and prints a JSON summary to stdout.

Examples:
  bb analyze-profile --profile=command.profile.gz
  bb analyze-profile /tmp/command.profile.gz --top_n=20
`
)

// Help returns the full embedded help text for bb analyze-profile.
func Help() string {
	return usage
}

func HandleAnalyzeProfile(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	path := *profilePath
	if path == "" {
		if flags.NArg() != 1 {
			log.Print(usage)
			return 1, nil
		}
		path = flags.Arg(0)
	} else if flags.NArg() != 0 {
		log.Print(usage)
		return 1, nil
	}
	if path == "" {
		return 1, fmt.Errorf("--profile must be non-empty")
	}
	if *topN < 1 {
		return 1, fmt.Errorf("--top_n must be >= 1")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return -1, fmt.Errorf("read profile %q: %w", path, err)
	}

	summary, profileIsGzip, err := timing_profile.Summarize(raw, filepath.Base(path), *topN)
	if err != nil {
		return -1, err
	}

	out := map[string]any{
		"profile_path":    path,
		"profile_is_gzip": profileIsGzip,
		"summary":         summary,
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return -1, fmt.Errorf("marshal JSON output: %w", err)
	}
	if _, err := fmt.Fprintf(os.Stdout, "%s\n", b); err != nil {
		return -1, fmt.Errorf("write JSON output: %w", err)
	}
	return 0, nil
}
