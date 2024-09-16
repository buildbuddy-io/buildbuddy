package explain

import (
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"

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
usage: bb explain --old <old compact execution log> --new <new compact execution log>

Displays a human-readable, structural diff of two compact execution logs.

Use the --experimental_execution_log_compact_file flag to have Bazel produce a
compact execution log.
`
)

var (
	explainCmd = flag.NewFlagSet("explain", flag.ContinueOnError)
	oldPath    = explainCmd.String("old", "", "Path to a compact execution log to consider as the baseline for the diff.")
	newPath    = explainCmd.String("new", "", "Path to a compact execution log to compare against the baseline.")
)

func HandleExplain(args []string) (int, error) {
	if err := arg.ParseFlagSet(explainCmd, args); err != nil {
		if err != flag.ErrHelp {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(explainCmdUsage)
		return 1, nil
	}
	if *oldPath == "" || *newPath == "" {
		log.Print(explainCmdUsage)
		return 1, nil
	}

	spawnDiffs, err := diff(*oldPath, *newPath)
	if err != nil {
		return -1, err
	}
	for _, spawnDiff := range spawnDiffs {
		writeSpawnDiff(os.Stdout, spawnDiff)
	}
	return 0, nil
}

func diff(oldPath, newPath string) ([]*spawn_diff.SpawnDiff, error) {
	readsEG := errgroup.Group{}
	var oldGraph compactgraph.CompactGraph
	var oldHashFunction string
	readsEG.Go(func() (err error) {
		oldGraph, oldHashFunction, err = readGraph(oldPath)
		return err
	})
	var newGraph compactgraph.CompactGraph
	var newHashFunction string
	readsEG.Go(func() (err error) {
		newGraph, newHashFunction, err = readGraph(newPath)
		return err
	})
	if err := readsEG.Wait(); err != nil {
		return nil, err
	}
	if oldHashFunction != newHashFunction {
		return nil, fmt.Errorf("hash functions differ: %q vs %q", oldHashFunction, newHashFunction)
	}
	return compactgraph.Diff(oldGraph, newGraph), nil
}

func readGraph(path string) (compactgraph.CompactGraph, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	return compactgraph.ReadCompactLog(f)
}

func writeSpawnDiff(w io.Writer, diff *spawn_diff.SpawnDiff) {
	if diff.DiffType == spawn_diff.SpawnDiff_OLD_ONLY {
		// We assume that the second execution log is the newer one and thus don't print spawns that are only in the old
		// one - they may very well just be up-to-date.
		return
	}
	_, _ = fmt.Fprintf(w, "\n%s %s (%s)\n", diff.Mnemonic, diff.TargetLabel, diff.PrimaryOutput)
	if diff.DiffType == spawn_diff.SpawnDiff_NEW_ONLY {
		_, _ = fmt.Fprintf(w, "  newly executed\n")
		return
	}

	for _, d := range diff.Diffs {
		writeSingleDiff(w, d)
	}

	if len(diff.TransitivelyInvalidated) == 0 {
		return
	}

	_, _ = fmt.Fprintf(w, "  transitively invalidated:\n")

	type kv struct {
		Mnemonic string
		Count    uint32
	}
	var sorted []kv
	for k, v := range diff.TransitivelyInvalidated {
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
		_, _ = fmt.Fprintf(w, "    %d %s\n", mc.Count, mc.Mnemonic)
	}
}

func writeSingleDiff(w io.Writer, diff *spawn_diff.Diff) {
	switch d := diff.Diff.(type) {
	case *spawn_diff.Diff_ToolPaths:
		_, _ = fmt.Fprintln(w, "  tool paths changed:")
		writeStringSetDiff(w, d.ToolPaths)
	case *spawn_diff.Diff_ToolContents:
		_, _ = fmt.Fprintln(w, "  tools changed:")
		writeFileSetDiff(w, d.ToolContents)
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
		_, _ = fmt.Fprintln(w, "  outputs changed non-hermetically:")
		writeFileSetDiff(w, d.OutputContents)
	default:
		panic(fmt.Sprintf("unknown diff type: %T", diff.Diff))
	}
}

func writeStringSetDiff(w io.Writer, d *spawn_diff.StringSetDiff) {
	for _, s := range d.OldOnly {
		_, _ = fmt.Fprintf(w, "    -%s\n", s)
	}
	for _, s := range d.NewOnly {
		_, _ = fmt.Fprintf(w, "    +%s\n", s)
	}
}

func writeListDiff(w io.Writer, d *spawn_diff.ListDiff) {
	_, _ = fmt.Fprintf(w, "    %s\n", gocmp.Diff(d.Old, d.New))
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

func writeFileSetDiff(w io.Writer, d *spawn_diff.FileSetDiff) {
	for _, f := range d.FileDiffs {
		var path string
		var oldType string
		var newType string
		switch of := f.Old.(type) {
		case *spawn_diff.FileDiff_OldFile:
			path = of.OldFile.Path
			oldType = typeFile
		case *spawn_diff.FileDiff_OldSymlink:
			path = of.OldSymlink.Path
			oldType = typeSymlink
		case *spawn_diff.FileDiff_OldDirectory:
			path = of.OldDirectory.Path
			oldType = typeDirectory
		}
		switch f.New.(type) {
		case *spawn_diff.FileDiff_NewFile:
			newType = typeFile
		case *spawn_diff.FileDiff_NewSymlink:
			newType = typeSymlink
		case *spawn_diff.FileDiff_NewDirectory:
			newType = typeDirectory
		}
		if oldType != newType {
			_, _ = fmt.Fprintf(w, "    %s: %s -> %s\n", path, oldType, newType)
			return
		}
		switch of := f.Old.(type) {
		case *spawn_diff.FileDiff_OldFile:
			_, _ = fmt.Fprintf(w, "    %s: content changed\n", path)
		case *spawn_diff.FileDiff_OldSymlink:
			nf := f.New.(*spawn_diff.FileDiff_NewSymlink)
			_, _ = fmt.Fprintf(
				w,
				"    %s: symlink target changed:\n      %q -> %q\n",
				path,
				of.OldSymlink.TargetPath,
				nf.NewSymlink.TargetPath,
			)
		case *spawn_diff.FileDiff_OldDirectory:
			_, _ = fmt.Fprintf(w, "    %s: directory contents changed:\n", path)
			nf := f.New.(*spawn_diff.FileDiff_NewDirectory)
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
		}
	}
}
