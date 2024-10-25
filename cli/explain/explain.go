package explain

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	gocmp "github.com/google/go-cmp/cmp"
)

const (
	explainCmdUsage = `
usage: bb explain --old <old compact execution log> --new <new compact execution log> [--verbose]

Displays a human-readable, structural diff of two compact execution logs.

Use the --experimental_execution_log_compact_file flag to have Bazel produce a
compact execution log.
`
)

type MapFlag map[string]string

func (m MapFlag) String() string {
	keys := maps.Keys(m)
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func (m MapFlag) Set(s string) error {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("expected key=value pair, got %q", s)
	}
	if parts[0] != "cpu" && pprof.Lookup(parts[0]) == nil {
		return fmt.Errorf("unknown profile type %q", parts[0])
	}
	m[parts[0]] = parts[1]
	return nil
}

var (
	explainCmd = flag.NewFlagSet("explain", flag.ContinueOnError)
	oldPath    = explainCmd.String("old", "", "Path to a compact execution log to consider as the baseline for the diff.")
	newPath    = explainCmd.String("new", "", "Path to a compact execution log to compare against the baseline.")
	verbose    = explainCmd.Bool("verbose", false, "Print more detailed execution information.")

	profilePaths = make(MapFlag)
)

func HandleExplain(args []string) (int, error) {
	explainCmd.Var(profilePaths, "profile", "Path that a CPU profile should be written to.")
	if err := arg.ParseFlagSet(explainCmd, args); err != nil {
		if err != flag.ErrHelp {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(explainCmdUsage)
		return 1, nil
	}
	if profilePaths["cpu"] != "" {
		f, err := os.Create(profilePaths["cpu"])
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *oldPath == "" || *newPath == "" {
		log.Print(explainCmdUsage)
		return 1, nil
	}

	spawnDiffs, err := diff(*oldPath, *newPath)
	if err != nil {
		return -1, err
	}
	writeSpawnDiffs(os.Stdout, spawnDiffs)

	for profile, p := range profilePaths {
		if profile == "cpu" {
			continue
		}
		f, err := os.Create(p)
		if err != nil {
			log.Fatalf("could not create %s profile: %s", profile, err)
		}
		defer f.Close()
		if profile == "heap" || profile == "alloc" {
			// Get up-to-date allocation statistics.
			runtime.GC()
		}
		if err := pprof.Lookup(profile).WriteTo(f, 0); err != nil {
			log.Fatalf("could not write %s profile: %s", profile, err)
		}
		f.Close()
	}
	return 0, nil
}

func diff(oldPath, newPath string) ([]*spawn_diff.SpawnDiff, error) {
	readsEG := errgroup.Group{}
	var oldGraph *compactgraph.CompactGraph
	readsEG.Go(func() (err error) {
		oldGraph, err = readGraph(oldPath)
		return err
	})
	var newGraph *compactgraph.CompactGraph
	readsEG.Go(func() (err error) {
		newGraph, err = readGraph(newPath)
		return err
	})
	if err := readsEG.Wait(); err != nil {
		return nil, err
	}
	return compactgraph.Diff(oldGraph, newGraph)
}

func readGraph(path string) (*compactgraph.CompactGraph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return compactgraph.ReadCompactLog(f)
}

func writeSpawnDiffs(w io.Writer, diffs []*spawn_diff.SpawnDiff) {
	// Diffs come in the order "old only", "new only", then "modified".
	var oldOnly, newOnly map[string]uint32
	for _, d := range diffs {
		switch td := d.Diff.(type) {
		case *spawn_diff.SpawnDiff_OldOnly:
			if oldOnly == nil {
				oldOnly = make(map[string]uint32)
				if *verbose {
					_, _ = fmt.Fprintln(w, "old only (top-level executions only):")
				}
			}
			if *verbose && td.OldOnly.TopLevel {
				_, _ = fmt.Fprintf(w, "  %s\n", spawnHeader(d))
			} else {
				oldOnly[d.Mnemonic]++
			}

		case *spawn_diff.SpawnDiff_NewOnly:
			if len(oldOnly) > 0 {
				if *verbose {
					_, _ = fmt.Fprintln(w, "\nold only (transitive executions):")
				} else {
					_, _ = fmt.Fprintln(w, "old only (pass --verbose to see details):")
				}
				writeMnemonicCounts(w, oldOnly, "  ")
				_, _ = fmt.Fprintln(w)
				oldOnly = nil
			}

			if newOnly == nil {
				newOnly = make(map[string]uint32)
				if *verbose {
					_, _ = fmt.Fprintln(w, "new only (top-level executions):")
				}
			}
			if *verbose && td.NewOnly.TopLevel {
				_, _ = fmt.Fprintf(w, "  %s\n", spawnHeader(d))
			} else {
				newOnly[d.Mnemonic]++
			}

		case *spawn_diff.SpawnDiff_Modified:
			if len(newOnly) > 0 {
				if *verbose {
					_, _ = fmt.Fprintln(w, "\nnew only (transitive executions):")
				} else {
					_, _ = fmt.Fprintln(w, "new only (pass --verbose to see details):")
				}
				writeMnemonicCounts(w, newOnly, "  ")
				_, _ = fmt.Fprintln(w)
				newOnly = nil
			}

			if td.Modified.Expected && !*verbose {
				continue
			}

			_, _ = fmt.Fprintf(w, "%s\n", spawnHeader(d))

			for _, sd := range td.Modified.Diffs {
				writeSingleDiff(w, sd)
			}

			if len(td.Modified.TransitivelyInvalidated) > 0 {
				_, _ = fmt.Fprintf(w, "  transitively invalidated:\n")
				writeMnemonicCounts(w, td.Modified.TransitivelyInvalidated, "    ")
			}
			_, _ = fmt.Fprintln(w)
		}
	}
}

func writeMnemonicCounts(w io.Writer, mnemonicsAndCounts map[string]uint32, indent string) {
	type kv struct {
		Mnemonic string
		Count    uint32
	}
	var sorted []kv
	for k, v := range mnemonicsAndCounts {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		this, that := sorted[i], sorted[j]
		if this.Count != that.Count {
			return this.Count > that.Count
		}
		return this.Mnemonic < that.Mnemonic
	})

	for _, mc := range sorted {
		_, _ = fmt.Fprintf(w, "%s%6d %s\n", indent, mc.Count, mc.Mnemonic)
	}
}

func spawnHeader(d *spawn_diff.SpawnDiff) string {
	label := d.TargetLabel
	if label == "" {
		label = "<unknown target>"
	}
	return fmt.Sprintf("%s %s (%s)", d.Mnemonic, label, d.PrimaryOutput)
}

func writeSingleDiff(w io.Writer, diff *spawn_diff.Diff) {
	switch d := diff.Diff.(type) {
	case *spawn_diff.Diff_ToolPaths:
		_, _ = fmt.Fprintln(w, "  tool paths changed:")
		writeStringSetDiff(w, d.ToolPaths)
	case *spawn_diff.Diff_InputPaths:
		_, _ = fmt.Fprintln(w, "  input paths changed:")
		writeStringSetDiff(w, d.InputPaths)
	case *spawn_diff.Diff_InputContents:
		_, _ = fmt.Fprintln(w, "  inputs changed:")
		writeFileSetDiff(w, d.InputContents)
	case *spawn_diff.Diff_Env:
		_, _ = fmt.Fprintln(w, "  env changed:")
		writeDictDiff(w, d.Env)
	case *spawn_diff.Diff_Args:
		_, _ = fmt.Fprintln(w, "  args changed:")
		writeListDiff(w, d.Args)
	case *spawn_diff.Diff_ParamFilePaths:
		_, _ = fmt.Fprintln(w, "  param file paths changed:")
		writeStringSetDiff(w, d.ParamFilePaths)
	case *spawn_diff.Diff_ParamFileContents:
		_, _ = fmt.Fprintln(w, "  param files changed:")
		writeFileSetDiff(w, d.ParamFileContents)
	case *spawn_diff.Diff_OutputPaths:
		_, _ = fmt.Fprintln(w, "  output paths changed:")
		writeStringSetDiff(w, d.OutputPaths)
	case *spawn_diff.Diff_OutputContents:
		_, _ = fmt.Fprintln(w, "  outputs changed (action is non-hermetic):")
		writeFileSetDiff(w, d.OutputContents)
	case *spawn_diff.Diff_ExitCode:
		_, _ = fmt.Fprintf(w, "  exit code changed (action is flaky): %d -> %d\n", d.ExitCode.Old, d.ExitCode.New)
	default:
		panic(fmt.Sprintf("unknown diff type: %T", diff.Diff))
	}
}

func writeStringSetDiff(w io.Writer, d *spawn_diff.StringSetDiff) {
	for _, s := range d.OldOnly {
		_, _ = fmt.Fprintf(w, "    - %s\n", s)
	}
	for _, s := range d.NewOnly {
		_, _ = fmt.Fprintf(w, "    + %s\n", s)
	}
}

func writeListDiff(w io.Writer, d *spawn_diff.ListDiff) {
	lines := strings.Split(gocmp.Diff(d.Old, d.New), "\n")
	for i, l := range lines {
		_, _ = fmt.Fprintf(w, "    %s", l)
		if i < len(lines)-1 {
			_, _ = fmt.Fprintln(w)
		}
	}
}

func writeDictDiff(w io.Writer, d *spawn_diff.DictDiff) {
	allKeys := append(maps.Keys(d.OldChanged), maps.Keys(d.NewChanged)...)
	slices.Sort(allKeys)
	allKeys = slices.Compact(allKeys)

	for _, k := range allKeys {
		oldV, oldOk := d.OldChanged[k]
		newV, newOk := d.NewChanged[k]
		if oldOk && newOk {
			if oldV != newV {
				_, _ = fmt.Fprintf(w, "    %q: %q -> %q\n", k, oldV, newV)
			}
		} else if oldOk {
			_, _ = fmt.Fprintf(w, "    %q: %q ->\n", k, oldV)
		} else {
			_, _ = fmt.Fprintf(w, "    %q: -> %q\n", k, newV)
		}
	}
}

const typeFile = "regular file"
const typeSymlink = "symlink"
const typeDirectory = "directory"
const typeInvalidOutput = "invalid output"

func writeFileSetDiff(w io.Writer, d *spawn_diff.FileSetDiff) {
	for _, f := range d.FileDiffs {
		var oldResolvedPath, newResolvedPath string
		var oldType, newType string
		switch of := f.Old.(type) {
		case *spawn_diff.FileDiff_OldFile:
			oldResolvedPath = of.OldFile.Path
			oldType = typeFile
		case *spawn_diff.FileDiff_OldSymlink:
			oldResolvedPath = of.OldSymlink.Path
			oldType = typeSymlink
		case *spawn_diff.FileDiff_OldDirectory:
			oldResolvedPath = of.OldDirectory.Path
			oldType = typeDirectory
		case *spawn_diff.FileDiff_OldInvalidOutput:
			oldResolvedPath = of.OldInvalidOutput
			oldType = typeInvalidOutput
		}
		switch nf := f.New.(type) {
		case *spawn_diff.FileDiff_NewFile:
			newResolvedPath = nf.NewFile.Path
			newType = typeFile
		case *spawn_diff.FileDiff_NewSymlink:
			newResolvedPath = nf.NewSymlink.Path
			newType = typeSymlink
		case *spawn_diff.FileDiff_NewDirectory:
			newResolvedPath = nf.NewDirectory.Path
			newType = typeDirectory
		case *spawn_diff.FileDiff_NewInvalidOutput:
			newResolvedPath = nf.NewInvalidOutput
			newType = typeInvalidOutput
		}
		var prefix string
		if oldResolvedPath != newResolvedPath {
			prefix = fmt.Sprintf("    %s (%s -> %s)", f.LogicalPath, oldResolvedPath, newResolvedPath)
		} else if oldResolvedPath != f.LogicalPath {
			prefix = fmt.Sprintf("    %s (%s)", f.LogicalPath, oldResolvedPath)
		} else {
			prefix = fmt.Sprintf("    %s", f.LogicalPath)
		}
		if oldType != newType {
			_, _ = fmt.Fprintf(w, "%s: %s -> %s\n", prefix, oldType, newType)
			continue
		}
		switch of := f.Old.(type) {
		case *spawn_diff.FileDiff_OldFile:
			_, _ = fmt.Fprintf(w, "%s: content changed\n", prefix)
		case *spawn_diff.FileDiff_OldSymlink:
			nf := f.New.(*spawn_diff.FileDiff_NewSymlink)
			_, _ = fmt.Fprintf(
				w,
				"%s: symlink target changed:\n      %q -> %q\n",
				prefix,
				of.OldSymlink.TargetPath,
				nf.NewSymlink.TargetPath,
			)
		case *spawn_diff.FileDiff_OldDirectory:
			nf := f.New.(*spawn_diff.FileDiff_NewDirectory)
			if len(of.OldDirectory.Files) == 0 && len(nf.NewDirectory.Files) == 0 {
				// The only diffs with no files are runfiles directories, which have their contents diffed on a separate
				// spawn.
				_, _ = fmt.Fprintf(w, "%s: runfiles tree changed (details in the corresponding \"Runfiles directory\")\n", prefix)
				continue
			}
			_, _ = fmt.Fprintf(w, "%s: directory contents changed:\n", prefix)
			var allPaths []string
			oldFiles := map[string]*spawn.ExecLogEntry_File{}
			newFiles := map[string]*spawn.ExecLogEntry_File{}
			for _, file := range of.OldDirectory.Files {
				allPaths = append(allPaths, file.Path)
				oldFiles[file.Path] = file
			}
			for _, file := range nf.NewDirectory.Files {
				allPaths = append(allPaths, file.Path)
				newFiles[file.Path] = file
			}
			slices.Sort(allPaths)
			allPaths = slices.Compact(allPaths)
			for _, p := range allPaths {
				oldFile, oldOk := oldFiles[p]
				newFile, newOk := newFiles[p]
				if oldOk && newOk {
					if oldFile.GetDigest().GetHash() != newFile.GetDigest().GetHash() {
						_, _ = fmt.Fprintf(w, "      %s: content changed\n", p)
					}
				} else if oldOk {
					_, _ = fmt.Fprintf(w, "      %s: removed\n", p)
				} else {
					_, _ = fmt.Fprintf(w, "      %s: added\n", p)
				}
			}
		case *spawn_diff.FileDiff_OldInvalidOutput:
			panic(fmt.Sprintf("invalid outputs %s always have the same content", f.LogicalPath))
		}
	}
}
