package bbmake

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/bbmake/makedb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
)

var (
	makeCmd = flag.NewFlagSet("make", flag.ExitOnError)

	besBackend    = makeCmd.String("bes_backend", "grpc://localhost:1985", "Endpoint for streaming build events.")
	besResultsUrl = makeCmd.String("bes_results_url", "http://localhost:8080/invocation/", "URL where build events can be found. The invocation ID is appended to this URL.")
	remoteCache   = makeCmd.String("remote_cache", "grpc://localhost:1985", "Endpoint for storing and retrieving remote build artifacts.")
	remoteHeaders = []string{}

	bbFlagNames = map[string]struct{}{}
)

var (
	// Matches make variable expansions in parentheses like $(SRCS)
	makeVariableRE = regexp.MustCompile(`\$\(\s*?([^\s]+?)\s*?\)`)
)

func init() {
	makeCmd.Func("remote_header", "Remote headers to set on all outgoing gRPC requests.", func(value string) error {
		remoteHeaders = append(remoteHeaders, value)
		return nil
	})
	makeCmd.VisitAll(func(f *flag.Flag) {
		bbFlagNames[f.Name] = struct{}{}
	})
}

func Run(ctx context.Context, args ...string) error {
	var bbArgs, makeArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		isBBFlag := false
		for bbFlag := range bbFlagNames {
			// If one of our flags flag without =, consume the next arg.
			// TODO: Bool flags should be handled differently.
			if arg == "-"+bbFlag || arg == "--"+bbFlag {
				isBBFlag = true
				if i+1 >= len(args)-1 {
					return status.InvalidArgumentErrorf("missing flag argument for %q", arg)
				}
				nextArg := args[i+1]
				bbArgs = append(bbArgs, arg)
				bbArgs = append(bbArgs, nextArg)
				i++
				break
			}
			if strings.HasPrefix(arg, "-"+bbFlag+"=") || strings.HasPrefix(arg, "--"+bbFlag+"=") {
				isBBFlag = true
				bbArgs = append(bbArgs, arg)
				break
			}
		}
		if !isBBFlag {
			makeArgs = append(makeArgs, arg)
		}
	}
	makeCmd.Parse(bbArgs)
	fmt.Println("makeArgs:", makeArgs)
	fmt.Println("bbArgs:", bbArgs)
	return (&Invocation{
		BESBackend:         *besBackend,
		BESResultsURL:      *besResultsUrl,
		RemoteHeaders:      remoteHeaders,
		PrintCommands:      true,
		PrintActionOutputs: true,
		MakeArgs:           makeArgs,
	}).Run(ctx)
}

type Invocation struct {
	// Dir is the directory in which to run. This directory is expected to
	// contain a Makefile.
	Dir string

	BESBackend         string
	BESResultsURL      string
	RemoteHeaders      []string
	InvocationID       string
	PrintCommands      bool
	PrintActionOutputs bool

	// MakeArgs are the arguments for `make`.
	MakeArgs []string

	bep             *build_event_publisher.Publisher
	mdb             *makedb.DB
	progressCounter int32
}

func (inv *Invocation) infof(format string, args ...interface{}) {
	// TODO: Toggleable colors
	msg := fmt.Sprintf("\x1b[32mINFO:\x1b[m "+format+"\n", args...)
	os.Stderr.Write([]byte(msg))
	inv.publishProgressEvent(msg)
}

func (inv *Invocation) errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf("\x1b[31mERROR:\x1b[m "+format+"\n", args...)
	os.Stderr.Write([]byte(msg))
	inv.publishProgressEvent(msg)
}

func (inv *Invocation) Run(ctx context.Context) (err error) {
	for _, h := range inv.RemoteHeaders {
		parts := strings.Split(h, "=")
		name := parts[0]
		value := strings.Join(parts[1:], "=")
		ctx = metadata.AppendToOutgoingContext(ctx, name, value)
	}

	if inv.InvocationID == "" {
		iid, err := uuid.NewRandom()
		if err != nil {
			return status.UnknownErrorf("failed to generate invocation ID: %s", err)
		}
		inv.InvocationID = iid.String()
	}
	if inv.BESResultsURL != "" {
		bep, err := build_event_publisher.New(inv.BESBackend, "" /*=apiKey*/, inv.InvocationID)
		if err != nil {
			return err
		}
		inv.bep = bep
		if err := inv.startPublisher(ctx); err != nil {
			return err
		}
	}
	if inv.bep != nil {
		inv.infof("Streaming results to %s%s", inv.BESResultsURL, inv.InvocationID)
	}
	startTime := time.Now()
	if err := inv.publishStartedEvent(startTime); err != nil {
		return err
	}
	if err := inv.publishWorkspaceStatusEvent(); err != nil {
		return err
	}
	if err := inv.publishBuildMetadataEvent(); err != nil {
		return err
	}
	if err := inv.publishPatternEvent(); err != nil {
		return err
	}
	exitCode := -1
	defer func() {
		if err != nil {
			inv.errorf("%s", err)
		}
		finishTime := time.Now()
		pubErr := inv.publishFinishedEvent(exitCode, finishTime)
		if err == nil {
			err = pubErr
		}
		pubErr = inv.publishBuildToolLogsEvent(finishTime.Sub(startTime))
		if err == nil {
			err = pubErr
		}
		stopErr := inv.stopPublisher()
		if err == nil {
			err = stopErr
		}
	}()

	// TODO: Currently we'll just treat all make args as targets. Ideally we
	// could be mostly API-compatible with the `make` CLI and respect arguments
	// like "--dry-run" etc.
	targets := inv.MakeArgs
	for _, target := range targets {
		if err := inv.publishTargetConfiguredEvent(target); err != nil {
			return err
		}
	}

	mdb, err := getMakeDB(ctx, inv.Dir, targets)
	if err != nil {
		return err
	}
	inv.mdb = mdb

	g, err := makeActionGraph(mdb, inv.Dir, targets)
	if err != nil {
		return status.WrapError(err, "failed to initialize build graph")
	}

	actions := make(chan *node)
	allActionsDone := make(chan struct{})
	const numWorkers = 8 // TODO: make configurable
	actionDone := make(chan *node, numWorkers)
	actionsRun := map[string]struct{}{}

	eg, ctx := errgroup.WithContext(ctx)
	// Action scheduler: while there are nodes that aren't up to date, or
	// there are nodes currently in progress, schedule more actions if possible,
	// then wait for actions to complete.
	eg.Go(func() error {
		defer close(allActionsDone)
		executing := map[*node]struct{}{}
		for g.Peek() != nil || len(executing) > 0 {
			// Schedule any and all schedulable actions.
			for {
				n := g.Next()
				if n == nil {
					break
				}
				// Note: This send will block until a worker is available, but
				// that's OK.
				actions <- n
				executing[n] = struct{}{}
			}
			select {
			case n := <-actionDone:
				actionsRun[n.File.Name] = struct{}{}
				delete(executing, n)
				n.Mtime = time.Now()
				g.MarkComplete(n)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	// Workers: repeatedly dequeue actions and run them.
	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for {
				select {
				case action := <-actions:
					if err := inv.runAction(ctx, action); err != nil {
						return err
					}
					actionDone <- action
				case <-allActionsDone:
					return nil
				}
			}
		})
	}

	if err := eg.Wait(); err != nil {
		if err, ok := err.(*exitError); ok {
			exitCode = err.ExitCode
		}

		// For any actions not run, publish a TargetComplete event with
		// success=false
		for _, target := range targets {
			if _, ok := actionsRun[target]; ok {
				continue
			}
			inv.publishTargetCompletedEvent(target, false)
		}

		return err
	}

	inv.infof("Build completed successfully.")
	return nil
}

func (inv *Invocation) startPublisher(ctx context.Context) error {
	if inv.bep == nil {
		return nil
	}
	inv.bep.Start(ctx)
	return nil
}

func (inv *Invocation) stopPublisher() error {
	if inv.bep == nil {
		return nil
	}
	return inv.bep.Finish()
}

func (inv *Invocation) publishStartedEvent(startTime time.Time) error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
			{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 0}}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
			{Id: &bespb.BuildEventId_Pattern{Pattern: &bespb.BuildEventId_PatternExpandedId{Pattern: inv.MakeArgs}}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
			Uuid:      inv.InvocationID,
			StartTime: timestamppb.New(startTime),
			// TODO: Include non-make args too?
			OptionsDescription: strings.Join(inv.MakeArgs, " "),
			Command:            "make",
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishTargetConfiguredEvent(target string) error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_TargetConfigured{TargetConfigured: &bespb.BuildEventId_TargetConfiguredId{Label: target}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_TargetCompleted{TargetCompleted: &bespb.BuildEventId_TargetCompletedId{Label: target}}},
		},
		Payload: &bespb.BuildEvent_Configured{Configured: &bespb.TargetConfigured{
			TargetKind: "make rule",
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishTargetCompletedEvent(target string, success bool) error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_TargetCompleted{TargetCompleted: &bespb.BuildEventId_TargetCompletedId{Label: target}}},
		Payload: &bespb.BuildEvent_Completed{Completed: &bespb.TargetComplete{
			Success: success,
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishPatternEvent() error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id:      &bespb.BuildEventId{Id: &bespb.BuildEventId_Pattern{Pattern: &bespb.BuildEventId_PatternExpandedId{Pattern: inv.MakeArgs}}},
		Payload: &bespb.BuildEvent_Expanded{Expanded: &bespb.PatternExpanded{}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishBuildMetadataEvent() error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: &bespb.BuildMetadata{
			Metadata: map[string]string{
				"ROLE": "MAKE",
			},
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishWorkspaceStatusEvent() error {
	if inv.bep == nil {
		return nil
	}

	userName := ""
	if u, err := user.Current(); err == nil {
		userName = u.Name
	}
	host := ""
	if h, err := os.Hostname(); err == nil {
		host = h
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bespb.WorkspaceStatus{
			Item: []*bespb.WorkspaceStatus_Item{
				{Key: "BUILD_USER", Value: userName},
				{Key: "BUILD_HOST", Value: host},
				// {Key: "GIT_BRANCH", Value: ""}, // TODO
				// {Key: "GIT_TREE_STATUS", Value: ""}, // TODO
				// {Key: "COMMIT_SHA", Value: ""}, // TODO
				// REPO_URL is used to report statuses, so always set it to the
				// target repo URL (which should be the same URL on which the workflow
				// is configured).
				// {Key: "REPO_URL", Value: ""}, // TODO
			},
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishProgressEvent(msg string) error {
	if inv.bep == nil {
		return nil
	}
	count := atomic.AddInt32(&inv.progressCounter, 1) - 1
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: count}}},
		Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{
			Stderr: msg,
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishFinishedEvent(exitCode int, finishTime time.Time) error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		},
		Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
			ExitCode: &bespb.BuildFinished_ExitCode{
				Name: "", // TODO
				Code: int32(exitCode),
			},
			FinishTime: timestamppb.New(finishTime),
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) publishBuildToolLogsEvent(duration time.Duration) error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		Payload: &bespb.BuildEvent_BuildToolLogs{BuildToolLogs: &bespb.BuildToolLogs{
			Log: []*bespb.File{
				{Name: "elapsed time", File: &bespb.File_Contents{Contents: []byte(fmt.Sprintf("%.6f", duration.Seconds()))}},
			},
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) runAction(ctx context.Context, action *node) error {
	// TODO: BES publishing, profiling
	// TODO: Remote cache lookup
	// TODO: Sandboxed execution
	// TODO: Short-circuit if recipe is empty.
	recipe := strings.TrimSpace(expandRecipe(action.File, inv.mdb))
	if inv.PrintCommands && recipe != "" {
		inv.infof("Target %q: running %q", action.File.Name, strings.TrimSpace(recipe))
	}
	if inv.PrintCommands && recipe == "" {
		inv.infof("Target %q: nothing to do", action.File)
	}
	cmd := exec.CommandContext(ctx, "sh", "-c", recipe)
	cmd.Dir = inv.Dir
	b, err := cmd.CombinedOutput()
	if inv.PrintActionOutputs && len(b) > 0 {
		logFn := inv.infof
		if err != nil {
			logFn = inv.errorf
		}
		logFn("Output from building target %q:\n%s", action.File.Name, string(b))
	}
	if contains(inv.MakeArgs, action.File.Name) {
		success := err == nil
		if pubErr := inv.publishTargetCompletedEvent(action.File.Name, success); pubErr != nil {
			if err == nil {
				err = pubErr
			}
		}

	}
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			return &exitError{Target: action.File.Name, Err: err, ExitCode: err.ProcessState.ExitCode()}
		}
		return err
	}
	return nil
}

type exitError struct {
	Target   string
	Err      error
	ExitCode int
}

func (e *exitError) Error() string {
	return fmt.Sprintf("Target %q failed (%s)", e.Target, e.Err)
}

func mtime(path string) (time.Time, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	return stat.ModTime(), nil
}

func contains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

type node struct {
	File      *makedb.File
	Parents   map[*node]struct{}
	Children  map[*node]struct{}
	Mtime     time.Time
	Scheduled bool
}

func newNode() *node {
	return &node{
		Parents:  make(map[*node]struct{}, 0),
		Children: make(map[*node]struct{}, 0),
	}
}

func (n *node) UpToDate() bool {
	if n.Mtime.IsZero() {
		return false
	}
	for c := range n.Children {
		if c.Mtime.After(n.Mtime) {
			return false
		}
	}
	return true
}

func (n *node) ChildrenUpToDate() bool {
	for c := range n.Children {
		if !c.UpToDate() {
			return false
		}
	}
	return true
}

type graph struct {
	nodes map[string]*node
	// next contains nodes to be processed next in the action graph. The
	// invariant to be maintained is that this is the complete set of nodes
	// that are not up to date, but whose children are all up to date.
	next map[*node]struct{}
}

func makeActionGraph(mdb *makedb.DB, dir string, targets []string) (*graph, error) {
	// Resolve desired target names to Files
	files := make([]*makedb.File, 0, len(targets))
	for _, t := range targets {
		f := mdb.FilesByName[t]
		if f == nil {
			return nil, status.InvalidArgumentErrorf("no such target %q", t)
		}
		files = append(files, f)
	}
	// Build up the graph
	g := &graph{
		nodes: map[string]*node{},
		next:  map[*node]struct{}{},
	}
	for _, f := range files {
		if err := explore(g, f, nil /*=parent*/, mdb, nil /*=path*/); err != nil {
			return nil, err
		}
	}
	// Initialize mtimes from the FS.
	// TODO: Parallelize
	for _, n := range g.nodes {
		t, err := mtime(filepath.Join(dir, n.File.Name))
		if err != nil {
			return nil, err
		}
		n.Mtime = t
	}
	// Initialize the "next" set.
	// TODO: Make this more efficient.
	for _, n := range g.nodes {
		if !n.UpToDate() && n.ChildrenUpToDate() {
			g.next[n] = struct{}{}
		}
	}
	return g, nil
}

func (g *graph) Peek() *node {
	return anyKey(g.next)
}

func (g *graph) Next() *node {
	n := g.Peek()
	delete(g.next, n)
	return n
}

func (g *graph) MarkComplete(n *node) {
	delete(g.next, n)
	for p := range n.Parents {
		if !p.UpToDate() && p.ChildrenUpToDate() {
			g.next[p] = struct{}{}
		}
	}
}

func explore(g *graph, f *makedb.File, parent *makedb.File, mdb *makedb.DB, path []string) error {
	// Detect cycles
	for _, p := range path {
		if p == f.Name {
			return status.InvalidArgumentErrorf("cycle detected in build graph: %s", append(path, f.Name))
		}
	}
	path = append(path, f.Name)

	// Make a node for this file if one doesn't already exist
	n := g.nodes[f.Name]
	if n == nil {
		n = &node{
			File:     f,
			Parents:  map[*node]struct{}{},
			Children: map[*node]struct{}{},
		}
		g.nodes[f.Name] = n
	}

	// Update parent / child relationships
	if parent != nil {
		pn := g.nodes[parent.Name]
		pn.Children[n] = struct{}{}
		n.Parents[pn] = struct{}{}
	}

	for _, d := range f.Dependencies {
		dep := mdb.FilesByName[d]
		if dep == nil {
			return status.UnknownErrorf("missing file metadata for dependency %q", d)
		}
		if err := explore(g, dep, f, mdb, path); err != nil {
			return err
		}
	}
	return nil
}

func expandRecipe(file *makedb.File, db *makedb.DB) string {
	// TODO: Handle escaping
	recipe := file.Recipe
	recipe = makeVariableRE.ReplaceAllStringFunc(recipe, func(match string) string {
		varName := strings.TrimSpace(match[2 : len(match)-1])
		v := db.VariablesByName[varName]
		if v == nil {
			// TODO: Should this return an "undefined variable" error?
			return ""
		}
		return v.Value
	})
	recipe = strings.ReplaceAll(recipe, "$@", file.Name)
	recipe = strings.ReplaceAll(recipe, "$^", strings.Join(file.Dependencies, " "))
	// TODO: Handle len(file.Dependencies) == 0 like make does.
	if len(file.Dependencies) > 0 {
		recipe = strings.ReplaceAll(recipe, "$<", file.Dependencies[0])
	}
	return recipe
}

func getMakeDB(ctx context.Context, dir string, targets []string) (*makedb.DB, error) {
	// First, print Make's internal database to get detailed info about
	// all the targets that can be produced, including commands and variable
	// expansions.
	cmd := exec.CommandContext(ctx, "make", "--print-data-base", "--question")
	cmd.Dir = dir
	b, _ := cmd.CombinedOutput()
	db, err := makedb.Parse(b)
	if err != nil {
		return nil, status.WrapError(err, "failed to parse make database")
	}
	if err := db.ExpandImplicitRules(dir, targets); err != nil {
		return nil, err
	}
	return db, nil
}

func anyKey[K comparable, V any](m map[K]V) K {
	for k := range m {
		return k
	}
	var zero K
	return zero
}
