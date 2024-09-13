package explain

import (
	"fmt"
	"io"
	"os"
	"slices"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	gocmp "github.com/google/go-cmp/cmp"
)

const (
	usage = `
usage: bb explain <old compact execution log> <new compact execution log>
`
)

func HandleExplain(args []string) (int, error) {
	if len(args) != 2 {
		log.Print(usage)
		return 1, nil
	}
	spawnDiffs, err := diff(args[0], args[1])
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
	return compactgraph.Compare(oldGraph, newGraph), nil
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
	_, _ = fmt.Fprintf(w, "\n%s %s (%s)\n", diff.Mnemonic, diff.Target, diff.PrimaryOutput)
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

func writeFileSetDiff(w io.Writer, d *spawn_diff.FileSetDiff) {
	for _, f := range d.FileDiffs {
		oldIsSymlink := f.GetOldTargetPath() != ""
		newIsSymlink := f.GetNewTargetPath() != ""
		if oldIsSymlink && newIsSymlink {
			_, _ = fmt.Fprintf(w, "    %s: %q -> %q\n", f.Path, f.GetOldTargetPath(), f.GetNewTargetPath())
		} else if oldIsSymlink {
			_, _ = fmt.Fprintf(w, "    %s: symlink -> regular file\n", f.Path)
		} else if newIsSymlink {
			_, _ = fmt.Fprintf(w, "    %s: regular file -> symlink\n", f.Path)
		} else {
			_, _ = fmt.Fprintf(w, "    %s: content changed\n", f.Path)
		}
	}
}
