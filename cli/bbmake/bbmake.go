package bbmake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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
	return (&Invocation{
		BESBackend:         *besBackend,
		BESResultsURL:      *besResultsUrl,
		RemoteCache:        *remoteCache,
		RemoteHeaders:      remoteHeaders,
		PrintCommands:      false,
		PrintActionOutputs: true,
		MakeArgs:           makeArgs,
	}).Run(ctx)
}

type ActionGraph struct {
	notify chan struct{}

	mu       sync.Mutex
	closed   bool
	queue    []*Action
	complete map[string]bool
}

func NewActionGraph() *ActionGraph {
	return &ActionGraph{
		complete: make(map[string]bool),
		notify:   make(chan struct{}, 1),
	}
}

func (g *ActionGraph) Add(a *Action) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		panic("attempted add to closed graph")
	}

	g.queue = append(g.queue, a)
	g.complete[a.Name] = false
}

func (g *ActionGraph) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	close(g.notify)
}

func (g *ActionGraph) MarkComplete(target string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.complete[target] = true

	if g.closed {
		return
	}
	select {
	case g.notify <- struct{}{}:
	default:
	}
}

func (g *ActionGraph) isRunnable(a *Action) bool {
	for _, dep := range a.ChangedDeps {
		complete, ok := g.complete[dep]
		if !ok {
			// We don't know about this dep, so it's probably already up to
			// date.
			continue
		}
		if !complete {
			// A changed dep has yet to be built, so this target is not yet
			// runnable.
			return false
		}
	}
	return true
}

func (g *ActionGraph) pop() (*Action, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.queue) == 0 && g.closed {
		return nil, io.EOF
	}
	for i, a := range g.queue {
		if g.isRunnable(a) {
			g.queue = append(g.queue[:i], g.queue[i+1:]...)
			return a, nil
		}
	}
	return nil, nil
}

func (g *ActionGraph) IsComplete(action string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.complete[action]
}

func (g *ActionGraph) CompletedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := 0
	for _, c := range g.complete {
		if c {
			out++
		}
	}
	return out
}

func (g *ActionGraph) Next(ctx context.Context) (*Action, error) {
	for {
		a, err := g.pop()
		if a != nil || err != nil {
			return a, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-g.notify:
		}
	}
}

type Profile struct {
	startTime time.Time

	mu     sync.Mutex
	events []*ProfileEvent
}

type ProfileEvent struct {
	Name       string
	ThreadName string
	Start      time.Time
	Duration   time.Duration
}

type TraceEvent struct {
	Ph   string                 `json:"ph"`
	Pid  int64                  `json:"pid"`
	Tid  int64                  `json:"tid"`
	Ts   float64                `json:"ts"`
	Dur  float64                `json:"dur"`
	Name string                 `json:"name"`
	Cat  string                 `json:"cat"`
	Args map[string]interface{} `json:"args"`
}

func NewProfile(startTime time.Time) *Profile {
	return &Profile{
		startTime: startTime,
	}
}

func (p *Profile) Add(event *ProfileEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
}

func (p *Profile) GzipBytes() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var traceEvents []*TraceEvent
	tid := int64(0)
	tids := map[string]int64{}
	for _, e := range p.events {
		_, ok := tids[e.ThreadName]
		if !ok {
			tids[e.ThreadName] = tid
			traceEvents = append(traceEvents, &TraceEvent{
				Ph:   "M", // metadata
				Tid:  tid,
				Name: "thread_name",
				Args: map[string]interface{}{
					"name": e.ThreadName,
				},
			})
			tid++
		}
		traceEvents = append(traceEvents, &TraceEvent{
			Ph:   "X", // duration
			Tid:  tids[e.ThreadName],
			Ts:   float64(e.Start.UnixNano()-p.startTime.UnixNano()) / 1e3,
			Dur:  float64(e.Duration.Nanoseconds()) / 1e3,
			Name: e.Name,
			Cat:  "execute recipe",
		})
	}

	b, err := json.Marshal(map[string]interface{}{"traceEvents": traceEvents})
	if err != nil {
		return nil, err
	}
	gzBuf := &bytes.Buffer{}
	w := gzip.NewWriter(gzBuf)
	if _, err := io.Copy(w, bytes.NewReader(b)); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return gzBuf.Bytes(), nil
}

type Invocation struct {
	// Dir is the directory in which to run. This directory is expected to
	// contain a Makefile.
	Dir string

	BESBackend         string
	BESResultsURL      string
	RemoteCache        string
	RemoteHeaders      []string
	InvocationID       string
	PrintCommands      bool
	PrintActionOutputs bool

	// MakeArgs are the arguments for `make`.
	MakeArgs []string

	bep             *build_event_publisher.Publisher
	bsClient        bspb.ByteStreamClient
	progressCounter int32
}

func (inv *Invocation) infof(format string, args ...interface{}) {
	// TODO: Toggleable colors
	msg := fmt.Sprintf("\x1b[32mINFO:\x1b[m "+format, args...)
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	os.Stderr.Write([]byte(msg))
	inv.publishProgressEvent(msg)
}

func (inv *Invocation) warnf(format string, args ...interface{}) {
	msg := fmt.Sprintf("\x1b[33mWARNING:\x1b[m "+format, args...)
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	os.Stderr.Write([]byte(msg))
	inv.publishProgressEvent(msg)
}

func (inv *Invocation) errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf("\x1b[31mERROR:\x1b[m "+format+"\n", args...)
	os.Stderr.Write([]byte(msg))
	inv.publishProgressEvent(msg)
}

func (inv *Invocation) printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	os.Stderr.Write([]byte(msg))
	inv.publishProgressEvent(msg)
}

func (inv *Invocation) Run(ctx context.Context) (err error) {
	startTime := time.Now()
	// TODO: Currently we'll just treat all make args as targets. Ideally we
	// could be mostly API-compatible with the `make` CLI and respect arguments
	// like "--dry-run" etc.
	targets := inv.MakeArgs

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
	md := &repb.RequestMetadata{
		ToolInvocationId: inv.InvocationID,
		// NOTE: The only cache transfers currently are BES uploads, so this is
		// fine. If there are other uploads/downloads added, this will need to
		// be set per cache request.
		ActionId: "bes-upload",
	}
	mdBytes, err := proto.Marshal(md)
	if err != nil {
		inv.warnf("Failed to marshal request metadata: %s", err)
	} else {
		ctx = metadata.AppendToOutgoingContext(ctx, "build.bazel.remote.execution.v2.requestmetadata-bin", string(mdBytes))
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
	profile := NewProfile(startTime)
	actionsCreated := 0
	exitCode := 0
	if inv.RemoteCache != "" {
		conn, err := grpc_client.DialTarget(inv.RemoteCache)
		if err != nil {
			inv.errorf("Failed to dial remote cache: %s")
		}
		defer conn.Close()
		inv.bsClient = bspb.NewByteStreamClient(conn)
	}
	finalize := func() error {
		finishTime := time.Now()
		var lastErr error
		if err := inv.publishFinishedEvent(exitCode, finishTime); err != nil {
			lastErr = err
		}
		if err := inv.publishBuildMetricsEvent(actionsCreated, len(targets)); err != nil {
			lastErr = err
		}
		profileFile, uploadErr := inv.uploadProfile(ctx, profile)
		if uploadErr != nil {
			lastErr = uploadErr
		}
		if err := inv.publishBuildToolLogsEvent(finishTime.Sub(startTime), profileFile); err != nil {
			lastErr = err
		}
		inv.infof("Elapsed time: %s", finishTime.Sub(startTime))
		actionsLabel := "actions"
		if actionsCreated == 1 {
			actionsLabel = "action"
		}
		inv.infof("Streaming build results to: %s%s", inv.BESResultsURL, inv.InvocationID)
		if err != nil {
			inv.errorf("Build failed (%s), %d total %s", err, actionsCreated, actionsLabel)
		} else {
			inv.infof("Build completed successfully, %d total %s", actionsCreated, actionsLabel)
		}
		if err := inv.stopPublisher(); err != nil {
			lastErr = err
		}
		return lastErr
	}
	defer func() {
		finalizeErr := finalize()
		if finalizeErr != nil && err == nil {
			err = finalizeErr
		}
	}()

	for _, target := range targets {
		if err := inv.publishTargetConfiguredEvent(target); err != nil {
			return err
		}
	}

	g := NewActionGraph()
	inv.infof("Tracing Makefile")
	tracedActionsCh := TraceDryRun(ctx, inv.Dir, targets)

	const numWorkers = 8 // TODO: make configurable

	eg, egCtx := errgroup.WithContext(ctx)
	// `make --trace --dry-run` output consumer: read the output from the make
	// trace as it becomes available, and add it to the action graph.
	eg.Go(func() error {
		defer g.Close()
		for ae := range tracedActionsCh {
			if actionsCreated == 0 {
				profile.Add(&ProfileEvent{
					Name:       "analyze Makefile",
					ThreadName: "main thread",
					Start:      startTime,
					Duration:   time.Since(startTime),
				})
			}
			if ae.Error != nil {
				return ae.Error
			}
			g.Add(ae.Action)
			actionsCreated++
		}
		return nil
	})
	// Workers: repeatedly dequeue actions from the graph and run them.
	for i := 0; i < numWorkers; i++ {
		workerID := i + 1
		eg.Go(func() error {
			ctx := egCtx
			for {
				a, err := g.Next(ctx)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				start := time.Now()
				err = inv.runAction(ctx, a)
				profile.Add(&ProfileEvent{
					ThreadName: fmt.Sprintf("worker %d", workerID),
					Name:       a.Name,
					Start:      start,
					Duration:   time.Since(start),
				})
				g.MarkComplete(a.Name)
				if contains(targets, a.Name) {
					pubErr := inv.publishTargetCompletedEvent(a.Name, err == nil)
					if err == nil {
						err = pubErr
					}
				}
				if err != nil {
					return err
				}
			}
		})
	}

	if err := eg.Wait(); err != nil {
		if err, ok := err.(*exitError); ok {
			exitCode = err.ExitCode
		}

		// For any actions not run, publish a TargetComplete event with
		// success=false.
		for _, target := range targets {
			if g.IsComplete(target) {
				continue
			}
			inv.publishTargetCompletedEvent(target, false)
		}

		return err
	}

	for _, target := range targets {
		if g.IsComplete(target) {
			continue
		}
		inv.publishTargetCompletedEvent(target, true)
	}

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

func (inv *Invocation) publishBuildToolLogsEvent(duration time.Duration, profileFile *bespb.File) error {
	if inv.bep == nil {
		return nil
	}
	files := []*bespb.File{
		{Name: "elapsed time", File: &bespb.File_Contents{Contents: []byte(fmt.Sprintf("%.6f", duration.Seconds()))}},
	}
	if profileFile != nil {
		files = append(files, profileFile)
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildToolLogs{BuildToolLogs: &bespb.BuildEventId_BuildToolLogsId{}}},
		Payload: &bespb.BuildEvent_BuildToolLogs{BuildToolLogs: &bespb.BuildToolLogs{
			Log: files,
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) uploadProfile(ctx context.Context, profile *Profile) (*bespb.File, error) {
	if inv.bsClient == nil {
		return nil, nil
	}
	u, err := url.Parse(inv.RemoteCache)
	if err != nil {
		return nil, status.InternalErrorf("Failed to parse remote_cache endpoint %q: %s", inv.RemoteCache, err)
	}
	b, err := profile.GzipBytes()
	if err != nil {
		return nil, status.InternalErrorf("Failed to assemble compressed profile: %s", err)
	}
	d, err := cachetools.UploadBlob(ctx, inv.bsClient, "", cachetools.NewBytesReadSeekCloser(b))
	if err != nil {
		return nil, err
	}
	return &bespb.File{
		Name: "command.profile.gz",
		File: &bespb.File_Uri{
			Uri: fmt.Sprintf("bytestream://%s/blobs/%s/%d", u.Host, d.GetHash(), d.GetSizeBytes()),
		},
	}, nil
}

func (inv *Invocation) publishBuildMetricsEvent(actionsCreated, targetsConfigured int) error {
	if inv.bep == nil {
		return nil
	}
	event := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetrics{BuildMetrics: &bespb.BuildEventId_BuildMetricsId{}}},
		Payload: &bespb.BuildEvent_BuildMetrics{BuildMetrics: &bespb.BuildMetrics{
			ActionSummary: &bespb.BuildMetrics_ActionSummary{
				ActionsCreated: int64(actionsCreated),
			},
			TargetMetrics: &bespb.BuildMetrics_TargetMetrics{
				TargetsConfigured: int64(targetsConfigured),
			},
		}},
	}
	return inv.bep.Publish(event)
}

func (inv *Invocation) runAction(ctx context.Context, action *Action) error {
	// TODO: BES publishing, profiling
	// TODO: Remote cache lookup
	// TODO: Sandboxed execution
	// TODO: Short-circuit if recipe is empty.
	recipe := action.Recipe
	if inv.PrintCommands && recipe != "" {
		inv.infof("Target %q: running %q", action.Name, strings.TrimSpace(recipe))
	}
	if inv.PrintCommands && recipe == "" {
		inv.infof("Target %q: nothing to do", action.Name)
	}
	cmd := exec.CommandContext(ctx, "sh", "-c", recipe)
	cmd.Dir = action.Dir
	b, err := cmd.CombinedOutput()
	if len(b) > 0 {
		inv.printf("%s", string(b))
	}
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			return &exitError{Target: action.Name, Err: err, ExitCode: err.ProcessState.ExitCode()}
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

type Action struct {
	Dir         string
	Name        string
	ChangedDeps []string
	Recipe      string
}

type ActionOrError struct {
	Action *Action
	Error  error
}

// TODO: Support filenames other than "Makefile"
var (
	upToDateTargetRE     = regexp.MustCompile(`^make: '(.*?)' is up to date.$`)
	updateTargetRE       = regexp.MustCompile(`^(Makefile):(\d+): update target '(.*?)' due to: (.*)$`)
	targetDoesNotExistRE = regexp.MustCompile(`^(Makefile):(\d+): target '(.*?)' does not exist$`)
	dirChangeRE          = regexp.MustCompile(`^make\[(\d+)\]: (Entering|Leaving) directory '(.*?)'$`)
)

func TraceDryRun(ctx context.Context, dir string, targets []string) chan *ActionOrError {
	args := append([]string{"--trace", "--dry-run"}, targets...)
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, "make", args...)
	cmd.Dir = dir
	var stderrBuf bytes.Buffer
	outr, outw := io.Pipe()
	cmd.Stderr = &stderrBuf
	cmd.Stdout = outw
	ch := make(chan *ActionOrError, 128)
	doneScanning := make(chan struct{})
	go func() {
		defer cancel()
		defer close(doneScanning)
		scanner := bufio.NewScanner(outr)

		var action *Action
		flushAction := func() {
			if action != nil {
				ch <- &ActionOrError{Action: action}
				action = nil
			}
		}

		dirStack := []string{dir}
		for scanner.Scan() {
			line := scanner.Text()
			m := dirChangeRE.FindStringSubmatch(line)
			if len(m) > 0 {
				dir := m[3]
				if m[2] == "Leaving" {
					// Validate that we're currently in the directory that make
					// says we're leaving
					if dirStack[len(dirStack)-1] != dir {
						ch <- &ActionOrError{
							Error: status.UnknownErrorf("unexpected `make --trace` output %q (not currently in directory %s)", line, dir),
						}
						fmt.Println("!!!")
						return
					}
					dirStack = dirStack[:len(dirStack)-1]
					continue
				}
				dirStack = append(dirStack, dir)
				continue
			}
			// Don't handle recursive make for now -- just let those be invoked
			// as regular make commands.
			// TODO: Replace recursive make commands w/ `bb make`
			if len(dirStack) > 1 {
				continue
			}

			m = upToDateTargetRE.FindStringSubmatch(line)
			if len(m) > 0 {
				flushAction()
				continue
			}

			m = updateTargetRE.FindStringSubmatch(line)
			if len(m) > 0 {
				flushAction()
				action = &Action{
					Dir:         dirStack[len(dirStack)-1],
					Name:        m[3],
					ChangedDeps: strings.Split(m[4], " "),
				}
				continue
			}

			m = targetDoesNotExistRE.FindStringSubmatch(line)
			if len(m) > 0 {
				flushAction()
				action = &Action{
					Dir:  dirStack[len(dirStack)-1],
					Name: m[3],
				}
				continue
			}

			if action == nil {
				ch <- &ActionOrError{
					Error: status.UnknownErrorf("unexpected output from `make --trace`: %q", line),
				}
				fmt.Println("!!!")
				return
			}
			action.Recipe += line + "\n"
		}
		if err := scanner.Err(); err != nil {
			ch <- &ActionOrError{
				Error: status.UnknownErrorf("failed to scan output from `make --trace`: %q", err),
			}
			return
		}
		flushAction()
	}()
	if err := cmd.Start(); err != nil {
		ch <- &ActionOrError{Error: err}
		close(ch)
		return ch
	}
	go func() {
		err := cmd.Wait()
		outw.Close()
		<-doneScanning
		if err != nil {
			ch <- &ActionOrError{Error: err}
		}
		close(ch)
	}()
	return ch
}

func contains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func anyKey[K comparable, V any](m map[K]V) K {
	for k := range m {
		return k
	}
	var zero K
	return zero
}
