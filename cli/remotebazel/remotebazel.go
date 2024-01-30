package remotebazel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/setup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	buildBuddyArtifactDir = "bb-out"

	escapeSeq                  = "\u001B["
	gitConfigSection           = "buildbuddy"
	gitConfigRemoteBazelRemote = "remote-bazel-remote-name"
	defaultRemoteExecutionURL  = "remote.buildbuddy.io"
)

var (
	remoteFlagset = flag.NewFlagSet("remote", flag.ContinueOnError)

	execOs         = remoteFlagset.String("os", "linux", "If set, requests execution on a specific OS.")
	execArch       = remoteFlagset.String("arch", "amd64", "If set, requests execution on a specific CPU architecture.")
	containerImage = remoteFlagset.String("container_image", "", "If set, requests execution on a specific runner image. Otherwise uses the default hosted runner version. A `docker://` prefix is required.")
	remoteRunner   = remoteFlagset.String("remote_runner", defaultRemoteExecutionURL, "The Buildbuddy grpc target the remote runner should run on.")

	defaultBranchRefs = []string{"refs/heads/main", "refs/heads/master"}
)

func consoleCursorMoveUp(y int) {
	fmt.Print(escapeSeq + strconv.Itoa(y) + "A")
}

func consoleCursorMoveBeginningLine() {
	fmt.Print(escapeSeq + "1G")
}

func consoleDeleteLines(n int) {
	fmt.Print(escapeSeq + strconv.Itoa(n) + "M")
}

type RunOpts struct {
	Server            string
	APIKey            string
	Args              []string
	WorkspaceFilePath string
}

type RepoConfig struct {
	Root      string
	URL       string
	Ref       string
	CommitSHA string
	Patches   [][]byte
}

func determineRemote(repo *git.Repository) (*git.Remote, error) {
	remotes, err := repo.Remotes()
	if err != nil {
		return nil, err
	}

	if len(remotes) == 0 {
		return nil, status.FailedPreconditionError("the git repository must have a remote configured to use remote Bazel")
	}

	if len(remotes) == 1 {
		return remotes[0], nil
	}

	conf, err := repo.Config()
	if err != nil {
		return nil, err
	}
	confRemote := conf.Raw.Section(gitConfigSection).Option(gitConfigRemoteBazelRemote)
	if confRemote != "" {
		r, err := repo.Remote(confRemote)
		if err == nil {
			return r, nil
		}
		log.Debugf("Could not find remote %q saved in config, ignoring", confRemote)
	}

	var remoteNames []string
	for _, r := range remotes {
		remoteNames = append(remoteNames, fmt.Sprintf("%s (%s)", r.Config().Name, r.Config().URLs[0]))
	}

	selectedRemoteAndURL := ""
	prompt := &survey.Select{
		Message: "Select the git remote that will be used by the remote Bazel instance to fetch your repo:",
		Options: remoteNames,
	}
	if err := survey.AskOne(prompt, &selectedRemoteAndURL); err != nil {
		return nil, err
	}

	selectedRemote := strings.Split(selectedRemoteAndURL, " (")[0]
	remote, err := repo.Remote(selectedRemote)
	if err != nil {
		return nil, err
	}

	conf.Raw.Section(gitConfigSection).SetOption(gitConfigRemoteBazelRemote, selectedRemote)
	if err := repo.SetConfig(conf); err != nil {
		return nil, err
	}

	return remote, nil
}

func determineDefaultBranch(repo *git.Repository) (string, error) {
	branches, err := repo.Branches()
	if err != nil {
		return "", status.UnknownErrorf("could not list branches: %s", err)
	}

	allBranches := make(map[string]struct{})
	err = branches.ForEach(func(branch *plumbing.Reference) error {
		allBranches[string(branch.Name())] = struct{}{}
		return nil
	})
	if err != nil {
		return "", status.UnknownErrorf("could not iterate over branches: %s", err)
	}

	for _, defaultBranch := range defaultBranchRefs {
		if _, ok := allBranches[defaultBranch]; ok {
			return defaultBranch, nil
		}
	}

	return "", status.NotFoundErrorf("could not determine default branch")
}

func runGit(args ...string) (string, error) {
	stdout := bytes.Buffer{}
	stderr := bytes.Buffer{}
	cmd := exec.Command("git", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return stdout.String(), err
		}
		return stdout.String(), status.UnknownErrorf("error running git %s: %s\n%s", args, err, stderr.String())
	}
	return stdout.String(), nil
}

func diffUntrackedFile(path string) (string, error) {
	patch, err := runGit("diff", "--no-index", "/dev/null", path)
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return patch, nil
		}
		return "", err
	}

	return patch, nil
}

func Config(path string) (*RepoConfig, error) {
	repo, err := git.PlainOpenWithOptions(path, &git.PlainOpenOptions{DetectDotGit: true})
	if err != nil {
		return nil, err
	}

	remote, err := determineRemote(repo)
	if err != nil {
		return nil, err
	}
	if len(remote.Config().URLs) == 0 {
		return nil, status.FailedPreconditionErrorf("remote %q does not have a fetch URL", remote.Config().Name)
	}
	fetchURL := remote.Config().URLs[0]

	log.Debugf("Using fetch URL: %s", fetchURL)

	defaultBranchRef, err := determineDefaultBranch(repo)
	if err != nil {
		return nil, err
	}

	log.Debugf("Using base branch: %s", defaultBranchRef)

	defaultBranchCommitHash, err := repo.ResolveRevision(plumbing.Revision(defaultBranchRef))
	if err != nil {
		return nil, status.UnknownErrorf("could not find commit hash for branch ref %q", defaultBranchRef)
	}

	log.Debugf("Using base branch commit hash: %s", defaultBranchCommitHash)

	wt, err := repo.Worktree()
	if err != nil {
		return nil, status.UnknownErrorf("could not determine git repo root")
	}

	repoConfig := &RepoConfig{
		Root:      wt.Filesystem.Root(),
		URL:       fetchURL,
		CommitSHA: defaultBranchCommitHash.String(),
		Ref:       defaultBranchRef,
	}

	patch, err := runGit("diff", defaultBranchCommitHash.String())
	if err != nil {
		return nil, err
	}
	if patch != "" {
		repoConfig.Patches = append(repoConfig.Patches, []byte(patch))
	}

	untrackedFiles, err := runGit("ls-files", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	untrackedFiles = strings.Trim(untrackedFiles, "\n")
	if untrackedFiles != "" {
		for _, uf := range strings.Split(untrackedFiles, "\n") {
			if strings.HasPrefix(uf, buildBuddyArtifactDir+"/") {
				continue
			}
			patch, err := diffUntrackedFile(uf)
			if err != nil {
				return nil, err
			}
			repoConfig.Patches = append(repoConfig.Patches, []byte(patch))
		}
	}

	return repoConfig, nil
}

func getTermWidth() int {
	size, err := unix.IoctlGetWinsize(int(os.Stdout.Fd()), unix.TIOCGWINSZ)
	if err != nil {
		return 80
	}
	return int(size.Col)
}

func splitLogBuffer(buf []byte) []string {
	var lines []string

	termWidth := getTermWidth()
	for _, line := range strings.Split(string(buf), "\n") {
		for len(line) > termWidth {
			lines = append(lines, line[0:termWidth])
			line = line[termWidth:]
		}
		lines = append(lines, line)
	}
	return lines
}

func streamLogs(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) error {
	chunkID := ""
	moveBack := 0

	drawChunk := func(chunk *elpb.GetEventLogChunkResponse) {
		// Are we redrawing the current chunk?
		if moveBack > 0 {
			consoleCursorMoveUp(moveBack)
			consoleCursorMoveBeginningLine()
			consoleDeleteLines(moveBack)
		}

		logLines := splitLogBuffer(chunk.GetBuffer())
		if !chunk.GetLive() {
			moveBack = 0
		} else {
			moveBack = len(logLines)
		}

		for _, l := range logLines {
			_, _ = os.Stdout.Write([]byte(l))
			_, _ = os.Stdout.Write([]byte("\n"))
		}
	}

	var chunks []*elpb.GetEventLogChunkResponse
	wasLive := false
	for {
		l, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
			InvocationId: invocationID,
			ChunkId:      chunkID,
			MinLines:     100,
		})
		if err != nil {
			return status.UnknownErrorf("error streaming logs: %s", err)
		}

		chunks = append(chunks, l)
		// If the current chunk was live but is no longer then delay redraw
		// until the next chunk is retrieved. The "volatile" part of the
		// chunk moves to the next chunk when a chunk is finalized. Without
		// the delay, we would print the chunk without the volatile portion
		// which will look like a "flicker" once the volatile portion is
		// printed again.
		if !wasLive || l.GetLive() {
			for _, chunk := range chunks {
				drawChunk(chunk)
			}
			chunks = nil
		}
		wasLive = l.GetLive()

		if l.GetNextChunkId() == "" {
			break
		}

		if l.GetNextChunkId() == chunkID {
			time.Sleep(1 * time.Second)
		}
		chunkID = l.GetNextChunkId()
	}
	return nil
}

func downloadFile(ctx context.Context, bsClient bspb.ByteStreamClient, resourceName *digest.ResourceName, outFile string) error {
	if err := os.MkdirAll(filepath.Dir(outFile), 0755); err != nil {
		return err
	}
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer out.Close()
	if err := cachetools.GetBlob(ctx, bsClient, resourceName, out); err != nil {
		return err
	}
	return nil
}

func lookupBazelInvocationOutputs(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) ([]*bespb.File, error) {
	childInRsp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{Lookup: &inpb.InvocationLookup{InvocationId: invocationID}})
	if err != nil {
		return nil, fmt.Errorf("could not retrieve invocation %q: %s", invocationID, err)
	}

	fileSets := make(map[string][]*bespb.File)
	outputFileSetNames := make(map[string]struct{})
	for _, e := range childInRsp.GetInvocation()[0].GetEvent() {
		switch t := e.GetBuildEvent().GetPayload().(type) {
		case *bespb.BuildEvent_NamedSetOfFiles:
			fileSets[e.GetBuildEvent().GetId().GetNamedSet().GetId()] = t.NamedSetOfFiles.GetFiles()
		case *bespb.BuildEvent_Completed:
			for _, og := range t.Completed.GetOutputGroup() {
				for _, fs := range og.GetFileSets() {
					outputFileSetNames[fs.GetId()] = struct{}{}
				}
			}
		}
	}

	var outputs []*bespb.File
	for fsID := range outputFileSetNames {
		fs, ok := fileSets[fsID]
		if !ok {
			return nil, fmt.Errorf("could not find file set with ID %q while fetching outputs", fsID)
		}
		outputs = append(outputs, fs...)
	}

	return outputs, nil
}

func bytestreamURIToResourceName(uri string) (*digest.ResourceName, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	r := strings.TrimPrefix(u.RequestURI(), "/")
	rn, err := digest.ParseDownloadResourceName(r)
	if err != nil {
		return nil, err
	}
	return rn, nil
}

// TODO(vadim): add interactive progress bar for downloads
// TODO(vadim): parallelize downloads
func downloadOutputs(ctx context.Context, env environment.Env, mainOutputs []*bespb.File, supportingOutputs []*bespb.File, supportingDirs []*bespb.Tree, outputBaseDir string) ([]string, error) {
	bsClient := env.GetByteStreamClient()

	var mainLocalArtifacts []string
	download := func(f *bespb.File) (string, error) {
		r, err := bytestreamURIToResourceName(f.GetUri())
		if err != nil {
			return "", nil
		}
		outFile := filepath.Join(outputBaseDir, buildBuddyArtifactDir)
		for _, p := range f.GetPathPrefix() {
			outFile = filepath.Join(outFile, p)
		}
		outFile = filepath.Join(outFile, f.GetName())
		if err := downloadFile(ctx, bsClient, r, outFile); err != nil {
			return "", err
		}
		return outFile, nil
	}
	for _, f := range mainOutputs {
		outFile, err := download(f)
		if err != nil {
			return nil, err
		}
		mainLocalArtifacts = append(mainLocalArtifacts, outFile)
	}
	// Supporting outputs (i.e. runtime files) are downloaded but not displayed to the user.
	for _, f := range supportingOutputs {
		if _, err := download(f); err != nil {
			return nil, err
		}
	}
	for _, d := range supportingDirs {
		rn, err := bytestreamURIToResourceName(d.GetUri())
		if err != nil {
			return nil, err
		}
		tree := &repb.Tree{}
		if err := cachetools.GetBlobAsProto(ctx, bsClient, rn, tree); err != nil {
			return nil, err
		}
		outDir := filepath.Join(outputBaseDir, buildBuddyArtifactDir, d.GetName())
		if err := os.MkdirAll(outDir, 0755); err != nil {
			return nil, err
		}
		if _, err := dirtools.DownloadTree(ctx, env, rn.GetInstanceName(), rn.GetDigestFunction(), tree, outDir, &dirtools.DownloadTreeOpts{}); err != nil {
			return nil, err
		}
	}

	// Format as relative paths with indentation for human consumption.
	var relArtifacts []string
	for _, a := range mainLocalArtifacts {
		rp, err := filepath.Rel(outputBaseDir, a)
		if err != nil {
			return nil, err
		}
		relArtifacts = append(relArtifacts, "  "+rp)
	}
	fmt.Printf("Downloaded artifacts:\n%s\n", strings.Join(relArtifacts, "\n"))
	return mainLocalArtifacts, nil
}

func Run(ctx context.Context, opts RunOpts, repoConfig *RepoConfig) (int, error) {
	healthChecker := healthcheck.NewHealthChecker("ci-runner")
	env := real_environment.NewRealEnv(healthChecker)

	conn, err := grpc_client.DialSimple(opts.Server)
	if err != nil {
		return 0, status.UnavailableErrorf("could not connect to BuildBuddy remote bazel service %q: %s", opts.Server, err)
	}
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", opts.APIKey)

	log.Debugf("Requesting command execution on remote Bazel instance.")

	instanceHash := sha256.New()
	instanceHash.Write(uuid.NodeID())
	instanceHash.Write([]byte(repoConfig.Root))

	reqOS := runtime.GOOS
	if *execOs != "" {
		reqOS = *execOs
	}
	reqArch := runtime.GOARCH
	if *execArch != "" {
		reqArch = *execArch
	}

	fetchOutputs := false
	runOutput := false
	bazelArgs := arg.GetBazelArgs(opts.Args)
	if len(bazelArgs) > 0 && (bazelArgs[0] == "build" || bazelArgs[0] == "run") {
		fetchOutputs = true
		if bazelArgs[0] == "run" {
			runOutput = true
		}
	}

	req := &rnpb.RunRequest{
		GitRepo: &rnpb.RunRequest_GitRepo{
			RepoUrl: repoConfig.URL,
		},
		RepoState: &rnpb.RunRequest_RepoState{
			CommitSha: repoConfig.CommitSHA,
			Branch:    repoConfig.Ref,
		},
		SessionAffinityKey: fmt.Sprintf("%x", instanceHash.Sum(nil)),
		BazelCommand:       strings.Join(bazelArgs, " "),
		Os:                 reqOS,
		Arch:               reqArch,
		ContainerImage:     *containerImage,
	}

	req.GetRepoState().Patch = append(req.GetRepoState().Patch, repoConfig.Patches...)

	rsp, err := bbClient.Run(ctx, req)
	if err != nil {
		return 0, status.UnknownErrorf("error running bazel: %s", err)
	}

	iid := rsp.GetInvocationId()

	log.Debugf("Invocation ID: %s", iid)

	if err := streamLogs(ctx, bbClient, iid); err != nil {
		return 0, err
	}

	inRsp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{Lookup: &inpb.InvocationLookup{InvocationId: iid}})
	if err != nil {
		return 0, fmt.Errorf("could not retrieve invocation: %s", err)
	}
	if len(inRsp.GetInvocation()) == 0 {
		return 0, fmt.Errorf("invocation not found")
	}

	childIID := ""
	exitCode := -1
	runfilesRoot := ""
	var runfiles []*bespb.File
	var runfileDirectories []*bespb.Tree
	var defaultRunArgs []string
	for _, e := range inRsp.GetInvocation()[0].GetEvent() {
		if cic, ok := e.GetBuildEvent().GetPayload().(*bespb.BuildEvent_ChildInvocationCompleted); ok {
			childIID = e.GetBuildEvent().GetId().GetChildInvocationCompleted().GetInvocationId()
			exitCode = int(cic.ChildInvocationCompleted.ExitCode)
		}
		if runOutput {
			if rta, ok := e.GetBuildEvent().GetPayload().(*bespb.BuildEvent_RunTargetAnalyzed); ok {
				runfilesRoot = rta.RunTargetAnalyzed.GetRunfilesRoot()
				runfiles = rta.RunTargetAnalyzed.GetRunfiles()
				runfileDirectories = rta.RunTargetAnalyzed.GetRunfileDirectories()
				defaultRunArgs = rta.RunTargetAnalyzed.GetArguments()
			}
		}
	}

	if exitCode == -1 {
		return 0, fmt.Errorf("could not determine remote Bazel exit code")
	}

	if fetchOutputs && exitCode == 0 {
		conn, err := grpc_client.DialSimple(opts.Server)
		if err != nil {
			return 0, fmt.Errorf("could not communicate with sidecar: %s", err)
		}
		env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
		env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", opts.APIKey)

		mainOutputs, err := lookupBazelInvocationOutputs(ctx, bbClient, childIID)
		if err != nil {
			return 0, err
		}
		outputsBaseDir := filepath.Dir(opts.WorkspaceFilePath)
		outputs, err := downloadOutputs(ctx, env, mainOutputs, runfiles, runfileDirectories, outputsBaseDir)
		if err != nil {
			return 0, err
		}
		if runOutput {
			if len(outputs) > 1 {
				return 0, fmt.Errorf("run requested but target produced more than one artifact")
			}
			binPath := outputs[0]
			if err := os.Chmod(binPath, 0755); err != nil {
				return 0, fmt.Errorf("could not prepare binary %q for execution: %s", binPath, err)
			}
			execArgs := defaultRunArgs
			// Pass through extra arguments (-- --foo=bar) from the command line.
			execArgs = append(execArgs, arg.GetExecutableArgs(opts.Args)...)
			log.Debugf("Executing %q with arguments %s", binPath, execArgs)
			cmd := exec.CommandContext(ctx, binPath, execArgs...)
			cmd.Dir = filepath.Join(outputsBaseDir, buildBuddyArtifactDir, runfilesRoot)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if e, ok := err.(*exec.ExitError); ok {
				return e.ExitCode(), nil
			}
			return 0, err
		}
	}

	return exitCode, nil
}

func HandleRemoteBazel(args []string) (int, error) {
	tempDir, err := os.MkdirTemp("", "buildbuddy-cli-*")
	if err != nil {
		return 1, err
	}
	defer func() {
		os.RemoveAll(tempDir)
	}()

	_, bazelArgs, execArgs, err := setup.Setup(args, tempDir)
	if err != nil {
		return 1, status.WrapError(err, "bazel setup")
	}

	bazelArgs, err = parseRemoteCliFlags(bazelArgs)
	if err != nil {
		return 1, status.WrapError(err, "parse remote bazel cli flags")
	}

	bazelArgs = arg.Remove(bazelArgs, "bes_backend")
	bazelArgs = arg.Remove(bazelArgs, "remote_cache")

	// Ensure all bazel remote runs use the remote cache.
	// The goal is to keep remote workloads close to our servers, so use the same
	// app backend as the remote runner.
	bazelArgs = append(bazelArgs, "--bes_backend="+*remoteRunner)
	bazelArgs = append(bazelArgs, "--remote_cache="+*remoteRunner)

	ctx := context.Background()
	repoConfig, err := Config(".")
	if err != nil {
		return 1, status.WrapError(err, "remote config")
	}

	wsFilePath, err := bazel.FindWorkspaceFile(".")
	if err != nil {
		return 1, status.WrapError(err, "finding workspace")
	}

	runner := *remoteRunner
	if !strings.HasPrefix(runner, "grpc") {
		runner = "grpcs://" + runner
	}

	return Run(ctx, RunOpts{
		Server:            runner,
		APIKey:            arg.Get(bazelArgs, "remote_header=x-buildbuddy-api-key"),
		Args:              arg.JoinExecutableArgs(bazelArgs, execArgs),
		WorkspaceFilePath: wsFilePath,
	}, repoConfig)
}

// parseRemoteCliFlags parses flags that affect configuration of remote bazel.
// These flags are defined in `remoteFlagset`.
//
// These flags are expected to be set between the `remote` command and the bazel
// command. Ex. bb remote <--remote_cli_flag> build //...
//
// If there are bazel startup flags set (also set before the bazel command),
// the remote cli flags and startup flags can be mixed in any order and will still
// be parsed correctly.
// Ex. bb remote <--remote_cli_flag> <--startup_flag> <--remote_cli_flag> build //...
//
// Return the list of original args with all remote cli flags removed.
func parseRemoteCliFlags(args []string) ([]string, error) {
	// Discard flag parse error logging because it's very verbose if you parse
	// a flag not in remoteFlagset, but we might expect that if bazel startup flags
	// are set
	remoteFlagset.SetOutput(io.Discard)

	// Stop parsing flags when we reach the bazel command
	_, bazelCmdIdx := parser.GetBazelCommandAndIndex(args)
	if bazelCmdIdx == -1 {
		return nil, status.InvalidArgumentErrorf("no bazel command passed to run remotely")
	}
	unparsedArgs := args[:bazelCmdIdx]

	for len(unparsedArgs) > 0 {
		err := remoteFlagset.Parse(unparsedArgs)
		if err == nil {
			// flagset.Args() contains the list of any unparsed arguments
			// Keep parsing them in a loop until we process all the args
			unparsedArgs = remoteFlagset.Args()
		} else {
			// Parsing undefined flags could happen if there are bazel startup flags set
			// Remove them from the list of unparsed arguments and keep parsing
			// remaining args.
			if strings.Contains(err.Error(), "flag provided but not defined") {
				// Remove the unrecognized flag and keep trying to parse
				unparsedArgs = unparsedArgs[1:]

				// Startup flags could be set in the format `--flag value`.
				// flagset.Parse() won't process arg lists not starting with a flag,
				// so remove any floating value arguments.
				if len(unparsedArgs) > 0 && !strings.HasPrefix(unparsedArgs[0], "-") {
					unparsedArgs = unparsedArgs[1:]
				}
			} else {
				return nil, err
			}
		}
	}

	// Remove all cli flags from the arg list
	argsRemoteFlagsRemoved := args[:bazelCmdIdx]
	remoteFlagset.VisitAll(func(f *flag.Flag) {
		_, argsRemoteFlagsRemoved = arg.Pop(argsRemoteFlagsRemoved, f.Name)
	})

	// Add back in the bazel command and any subsequent flags
	argsRemoteFlagsRemoved = append(argsRemoteFlagsRemoved, args[bazelCmdIdx:]...)
	return argsRemoteFlagsRemoved, nil
}
