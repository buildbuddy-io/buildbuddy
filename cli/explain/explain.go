package explain

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/explain/compactgraph"
	"github.com/buildbuddy-io/buildbuddy/cli/flaghistory"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbpb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	gocmp "github.com/google/go-cmp/cmp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	explainCmdUsage = `
usage: bb explain [--old {FILE | INVOCATION_ID}] [--new {FILE | INVOCATION_ID}] [--output_format {text|json|proto}] [--nondeterministic_only]
       bb explain profile INVOCATION_ID

Displays a human-readable, structural diff of two compact execution logs, either
obtained from the given invocations or located at the given file paths.

If --new isn't specified, the most recent build performed with the bb CLI is
used as the "new" log. If --old also isn't specified, the second most recent
build is used as the "old" log.

Use the --execution_log_compact_file flag to have Bazel produce a compact
execution log and upload it to the BuildBuddy BES backend.

Pass --nondeterministic_only to restrict the output to non-deterministic spawns,
i.e. spawns whose outputs or exit code changed even though their inputs didn't.

Output formats:
  text   Unstructured output (default)
  json   Structured output as JSON
  proto  Structured output as binary proto

Subcommands:
  profile   Analyzes the timing profile for an invocation.
`
)

type MapFlag map[string]string

func (m MapFlag) String() string {
	keys := slices.Sorted(maps.Keys(m))
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
	explainCmd       = flag.NewFlagSet("explain", flag.ContinueOnError)
	Flags            = explainCmd
	oldLog           = explainCmd.String("old", "", "Path to a compact execution log or invocation ID of a build to consider as the baseline for the diff.")
	newLog           = explainCmd.String("new", "", "Path to a compact execution log or invocation ID of a build to compare against the baseline.")
	verbose          = explainCmd.Bool("verbose", false, "Print more detailed execution information.")
	apiTarget        = explainCmd.String("target", "", "The API target to use for fetching logs instead of the last --bes_backend.")
	outputFormat     = explainCmd.String("output_format", "text", "Output format: text, json, or proto.")
	nondeterministic = explainCmd.Bool("nondeterministic_only", false, "Only show non-deterministic spawns, i.e. spawns whose outputs or exit code changed even though their inputs didn't.")

	profilePaths = make(MapFlag)
)

func HandleExplain(args []string) (int, error) {
	if len(args) > 0 && args[0] == "profile" {
		return handleProfile(args[1:])
	}
	explainCmd.Var(profilePaths, "profile", "Path that a CPU profile should be written to.")
	if err := arg.ParseFlagSet(explainCmd, args); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(explainCmdUsage)
		return 1, nil
	}
	if len(explainCmd.Args()) > 0 {
		log.Print(explainCmdUsage)
		return 1, nil
	}
	if profilePaths["cpu"] != "" {
		f, err := os.Create(profilePaths["cpu"])
		if err != nil {
			return -1, fmt.Errorf("could not create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return -1, fmt.Errorf("could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *newLog == "" {
		newId, err := flaghistory.GetPreviousFlag(flaghistory.InvocationIDFlagName)
		if err != nil {
			return -1, fmt.Errorf("could not get invocation ID of the last build, please specify --new: %v", err)
		}
		if newId == "" {
			return -1, fmt.Errorf("no previous build to compare against, please specify --new")
		}
		*newLog = newId
		if *oldLog == "" {
			oldId, err := flaghistory.GetNthPreviousFlag(flaghistory.InvocationIDFlagName, 2)
			if err != nil {
				return -1, fmt.Errorf("could not get invocation ID of the build before the last, please specify --old: %v", err)
			}
			if oldId == "" {
				return -1, fmt.Errorf("no previous build to compare against, please specify --old")
			}
			*oldLog = oldId
		}
	}
	if *oldLog == "" || *newLog == "" {
		log.Print(explainCmdUsage)
		return 1, nil
	}

	diffResult, err := Diff(*oldLog, *newLog, *nondeterministic)
	if err != nil {
		return -1, err
	}
	switch *outputFormat {
	case "text":
		WriteText(os.Stdout, diffResult, *verbose)
	case "json":
		b, err := protojson.MarshalOptions{Multiline: true, UseProtoNames: true}.Marshal(diffResult)
		if err != nil {
			return -1, fmt.Errorf("failed to marshal diff result as JSON: %v", err)
		}
		_, _ = os.Stdout.Write(b)
		_, _ = fmt.Fprintln(os.Stdout)
	case "proto":
		b, err := proto.Marshal(diffResult)
		if err != nil {
			return -1, fmt.Errorf("failed to marshal diff result as proto: %v", err)
		}
		_, _ = os.Stdout.Write(b)
	default:
		return 1, fmt.Errorf("unknown --output_format %q: must be text, json, or proto", *outputFormat)
	}

	for profile, p := range profilePaths {
		if profile == "cpu" {
			continue
		}
		f, err := os.Create(p)
		if err != nil {
			return -1, fmt.Errorf("could not create %s profile: %v", profile, err)
		}
		defer f.Close()
		if profile == "heap" || profile == "alloc" {
			// Get up-to-date allocation statistics.
			runtime.GC()
		}
		if err := pprof.Lookup(profile).WriteTo(f, 0); err != nil {
			return -1, fmt.Errorf("could not write %s profile: %v", profile, err)
		}
	}
	return 0, nil
}

// Diff returns a structural diff of two compact execution logs. If
// nondeterministicOnly is set, the diff is reduced to the spawns that represent
// genuine non-determinism (see filterNondeterministicSpawns).
func Diff(oldPath, newPath string, nondeterministicOnly bool) (*spawn_diff.DiffResult, error) {
	oldSource, err := openLog(oldPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open old log: %v", err)
	}
	defer oldSource.Close()
	newSource, err := openLog(newPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open new log: %v", err)
	}
	defer newSource.Close()
	readsEG := errgroup.Group{}
	var oldGraph *compactgraph.CompactGraph
	readsEG.Go(func() (err error) {
		oldGraph, err = compactgraph.ReadCompactLog(oldSource)
		return err
	})
	var newGraph *compactgraph.CompactGraph
	readsEG.Go(func() (err error) {
		newGraph, err = compactgraph.ReadCompactLog(newSource)
		return err
	})
	if err := readsEG.Wait(); err != nil {
		return nil, err
	}
	result, err := compactgraph.Diff(oldGraph, newGraph)
	if err != nil {
		return nil, err
	}
	if nondeterministicOnly {
		result.SpawnDiffs = filterNondeterministicSpawns(result.SpawnDiffs)
	}
	return result, nil
}

// filterNondeterministicSpawns reduces the diff to the modified spawns that
// represent genuine non-determinism, i.e. whose outputs or exit code changed
// even though their inputs didn't (see isNondeterministic). Spawns whose
// non-determinism is expected (e.g. timestamps in test outputs) are dropped.
func filterNondeterministicSpawns(diffs []*spawn_diff.SpawnDiff) []*spawn_diff.SpawnDiff {
	var filtered []*spawn_diff.SpawnDiff
	for _, d := range diffs {
		if d.GetModified().GetExpected() {
			continue
		}
		if isNondeterministic(d) {
			filtered = append(filtered, d)
		}
	}
	return filtered
}

// isNondeterministic reports whether a modified spawn diff records a change in
// output contents (non-hermetic outputs) or exit code (flaky action) without a
// corresponding change in inputs.
func isNondeterministic(d *spawn_diff.SpawnDiff) bool {
	for _, sd := range d.GetModified().GetDiffs() {
		switch sd.Diff.(type) {
		case *spawn_diff.Diff_OutputContents, *spawn_diff.Diff_ExitCode:
			return true
		}
	}
	return false
}

var uuidPattern = regexp.MustCompile("^(?:.*/invocation/)?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")

func openLog(pathOrId string) (io.ReadCloser, error) {
	f, err := os.Open(pathOrId)
	if err == nil {
		return f, nil
	} else if !os.IsNotExist(err) || !uuidPattern.MatchString(pathOrId) {
		return nil, err
	}
	matches := uuidPattern.FindStringSubmatch(pathOrId)
	invocationId := matches[1]

	ctx := context.Background()
	target, err := download.ResolveTarget(*apiTarget)
	if err != nil {
		return nil, err
	}
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return nil, err
	}
	bsClient := bspb.NewByteStreamClient(conn)
	bbClient := bbpb.NewBuildBuddyServiceClient(conn)

	// Avoid reading the entire log into memory at once.
	in, out := io.Pipe()
	go func() {
		err := download.GetInvocationFile(ctx, bsClient, bbClient, out, invocationId, "execution log", findExecutionLog)
		conn.Close()
		out.CloseWithError(err)
	}()
	return in, nil
}

func findExecutionLog(inv *inpb.Invocation) *bespb.File {
	for _, event := range inv.GetEvent() {
		for _, file := range event.GetBuildEvent().GetBuildToolLogs().GetLog() {
			if file.Name == "execution_log.binpb.zst" {
				return file
			}
		}
	}
	return nil
}

func writeHeader(w io.Writer, oldInvocationId, newInvocationId string) {
	besResultsUrl, err := flaghistory.GetPreviousFlag(flaghistory.BesResultsUrlFlagName)
	if err != nil {
		besResultsUrl = ""
	}
	if oldInvocationId != "" {
		_, _ = fmt.Fprintf(w, "old invocation: %s%s\n", besResultsUrl, oldInvocationId)
	}
	if newInvocationId != "" {
		_, _ = fmt.Fprintf(w, "new invocation: %s%s\n", besResultsUrl, newInvocationId)
	}
	if oldInvocationId != "" || newInvocationId != "" {
		_, _ = fmt.Fprintln(w)
	}
}

const (
	initialState = iota
	oldOnlyState
	newOnlyState
	modifiedState
	finalState
)

// WriteText writes a human-readable diff result.
func WriteText(w io.Writer, diffResult *spawn_diff.DiffResult, verbose bool) {
	writeHeader(w, diffResult.OldInvocationId, diffResult.NewInvocationId)
	writeSpawnDiffs(w, diffResult.SpawnDiffs, verbose)
}

func writeSpawnDiffs(w io.Writer, diffs []*spawn_diff.SpawnDiff, verbose bool) {
	// Diffs come in the order "old only", "new only", then "modified".
	var oldOnly, newOnly map[string]uint32
	previousState := initialState
	// Append a nil diff as a sentinel to ensure the final state is processed.
	for _, d := range append(diffs, nil) {
		var currentState int
		switch d.GetDiff().(type) {
		case *spawn_diff.SpawnDiff_OldOnly:
			currentState = oldOnlyState
		case *spawn_diff.SpawnDiff_NewOnly:
			currentState = newOnlyState
		case *spawn_diff.SpawnDiff_Modified:
			currentState = modifiedState
		case nil:
			currentState = finalState
		}
		if currentState != previousState {
			switch previousState {
			case oldOnlyState:
				if len(oldOnly) > 0 {
					if verbose {
						_, _ = fmt.Fprintln(w, "\nold only (transitive executions):")
					} else {
						_, _ = fmt.Fprintln(w, "old only (pass --verbose to see details):")
					}
					writeMnemonicCounts(w, oldOnly, "  ")
					_, _ = fmt.Fprintln(w)
				}
			case newOnlyState:
				if len(newOnly) > 0 {
					if verbose {
						_, _ = fmt.Fprintln(w, "\nnew only (transitive executions):")
					} else {
						_, _ = fmt.Fprintln(w, "new only (pass --verbose to see details):")
					}
					writeMnemonicCounts(w, newOnly, "  ")
					_, _ = fmt.Fprintln(w)
				}
			default:
			}
			switch currentState {
			case oldOnlyState:
				oldOnly = make(map[string]uint32)
				if verbose {
					_, _ = fmt.Fprintln(w, "old only (top-level executions only):")
				}
			case newOnlyState:
				newOnly = make(map[string]uint32)
				if verbose {
					_, _ = fmt.Fprintln(w, "new only (top-level executions only):")
				}
			default:
			}
			previousState = currentState
		}

		switch td := d.GetDiff().(type) {
		case *spawn_diff.SpawnDiff_OldOnly:
			if verbose && td.OldOnly.TopLevel {
				_, _ = fmt.Fprintf(w, "  %s\n", spawnHeader(d))
			} else {
				oldOnly[d.Mnemonic]++
			}
		case *spawn_diff.SpawnDiff_NewOnly:
			if verbose && td.NewOnly.TopLevel {
				_, _ = fmt.Fprintf(w, "  %s\n", spawnHeader(d))
			} else {
				newOnly[d.Mnemonic]++
			}
		case *spawn_diff.SpawnDiff_Modified:
			if td.Modified.Expected && !verbose {
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
	case *spawn_diff.Diff_ExecProperties:
		_, _ = fmt.Fprintln(w, "  exec properties changed:")
		writeDictDiff(w, d.ExecProperties)
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
	allKeys := make([]string, 0, len(d.OldChanged)+len(d.NewChanged))
	allKeys = slices.AppendSeq(allKeys, maps.Keys(d.OldChanged))
	allKeys = slices.AppendSeq(allKeys, maps.Keys(d.NewChanged))
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
