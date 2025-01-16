package remotebazel

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/metadata"

	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	bbflag "github.com/buildbuddy-io/buildbuddy/server/util/flag"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	BuildBuddyArtifactDir = "bb-out"

	escapeSeq                  = "\u001B["
	gitConfigSection           = "buildbuddy"
	gitConfigRemoteBazelRemote = "remote-bazel-remote-name"
	defaultRemoteExecutionURL  = "remote.buildbuddy.io"

	// Name of the dir where the remote runner should write bazel run scripts
	// (used to facilitate building a target remotely and running it locally).
	runScriptDirName = "bazel-run-scripts"

	// `git remote` output is expected to look like:
	// `origin	git@github.com:buildbuddy-io/buildbuddy.git (fetch)`
	gitRemoteRegex = `(.+)\s+(.+)\s+\((push|fetch)\)`
)

var (
	RemoteFlagset = flag.NewFlagSet("remote", flag.ContinueOnError)

	execOs                  = RemoteFlagset.String("os", "", "If set, requests execution on a specific OS.")
	execArch                = RemoteFlagset.String("arch", "", "If set, requests execution on a specific CPU architecture.")
	containerImage          = RemoteFlagset.String("container_image", "", "If set, requests execution on a specific runner image. Otherwise uses the default hosted runner version. A `docker://` prefix is required.")
	envInput                = bbflag.New(RemoteFlagset, "env", []string{}, "Environment variables to set in the runner environment. Key-value pairs can either be separated by '=' (Ex. --env=k1=val1), or if only a key is specified, the value will be taken from the invocation environment (Ex. --env=k2). To apply multiple env vars, pass the env flag multiple times (Ex. --env=k1=v1 --env=k2). If the same key is given twice, the latest will apply.")
	remoteRunner            = RemoteFlagset.String("remote_runner", defaultRemoteExecutionURL, "The Buildbuddy grpc target the remote runner should run on.")
	timeout                 = RemoteFlagset.Duration("timeout", 0, "If set, requests that have exceeded this timeout will be canceled automatically. (Ex. --timeout=15m; --timeout=2h)")
	execPropsFlag           = bbflag.New(RemoteFlagset, "runner_exec_properties", []string{}, "Exec properties that will apply to the *ci runner execution*. Key-value pairs should be separated by '=' (Ex. --runner_exec_properties=NAME=VALUE). Can be specified more than once. NOTE: If you want to apply an exec property to the bazel command that's run on the runner, just pass at the end of the command (Ex. bb remote build //... --remote_default_exec_properties=OSFamily=linux).")
	remoteHeaders           = bbflag.New(RemoteFlagset, "remote_run_header", []string{}, "Remote headers to be applied to the execution request for the remote run. Can be used to set platform properties containing secrets (Ex. --remote_run_header=x-buildbuddy-platform.SECRET_NAME=SECRET_VALUE). Can be specified more than once.")
	runRemotely             = RemoteFlagset.Bool("run_remotely", true, "For `run` commands, whether the target should be run remotely. If false, the target will be built remotely, and then fetched and run locally.")
	useSystemGitCredentials = RemoteFlagset.Bool("use_system_git_credentials", false, "Whether to use github auth pre-configured on the remote runner. If false, require https and an access token for git access.")
	runFromBranch           = RemoteFlagset.String("run_from_branch", "", "A GitHub branch to base the remote run off. If unset, the remote workspace will mirror your local workspace.")
	runFromCommit           = RemoteFlagset.String("run_from_commit", "", "A GitHub commit SHA to base the remote run off. If unset, the remote workspace will mirror your local workspace.")
	script                  = RemoteFlagset.String("script", "", "Shell code to run remotely instead of a Bazel command.")
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
	Server string
	APIKey string
	// Name of the remote run.
	Name string
	// Command to run remotely.
	Command string
	// Whether the remotely built outputs should be fetched locally.
	FetchOutputs bool
	// Whether the remotely built target should be fetched and run locally.
	RunOutputLocally bool
	// If RunOutputLocally=true, execution arguments for running the target locally.
	ExecArgs          []string
	WorkspaceFilePath string
}

type gitRemote struct {
	name string
	url  string
	// What the remote is used for, like 'fetch' or 'pull'
	urlType string
}

type RepoConfig struct {
	URL           string
	Ref           string
	CommitSHA     string
	Patches       [][]byte
	DefaultBranch string
}

// determineRemote returns the git remote that will be used by the remote runner
// to fetch the git repo.
// Uses the `git remote -v` command to fetch remote info.
func determineRemote() (*gitRemote, error) {
	remotesStr, err := runGit("remote", "-v")
	if err != nil {
		return nil, status.WrapError(err, "git remote -v")
	} else if remotesStr == "" {
		return nil, status.FailedPreconditionError("the git repository must have a remote configured to use remote Bazel")
	}

	remotes := make([]*gitRemote, 0)
	for _, s := range strings.Split(remotesStr, "\n") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		remote, err := parseRemote(s)
		if err != nil {
			return nil, status.WrapError(err, "parse remote")
		}
		if remote.name == "" || remote.url == "" {
			log.Warnf("malformed `git remote -v` output: %s", s)
			continue
		}
		if remote.urlType == "fetch" {
			remotes = append(remotes, remote)
		}
	}

	if len(remotes) == 0 {
		return nil, status.InvalidArgumentErrorf("invalid .git/config - no remote URLs")
	}
	if len(remotes) == 1 {
		return remotes[0], nil
	}

	// If multiple remotes are configured, check if a remote was previously
	// used and cached in the buildbuddy config.
	cachedRemote, _ := storage.ReadRepoConfig(gitConfigRemoteBazelRemote)
	if cachedRemote != "" {
		for _, r := range remotes {
			if r.name == cachedRemote {
				return r, nil
			}
		}
		log.Debugf("Could not find remote %q saved in config, ignoring", cachedRemote)
	}

	// Prompt user to select a remote if there are multiple.
	var remoteNames []string
	for _, r := range remotes {
		remoteNames = append(remoteNames, fmt.Sprintf("%s (%s)", r.name, r.url))
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
	for _, r := range remotes {
		if r.name == selectedRemote {
			err = storage.WriteRepoConfig(gitConfigRemoteBazelRemote, r.name)
			if err != nil {
				log.Warnf("failed to cache selected remote in .git/config: %s", err)
			}
			return r, nil
		}
	}

	return nil, status.InternalError("selected remote is not configured")
}

// parseRemote parses the string output of a `git remote` command into a `gitRemote` struct.
func parseRemote(s string) (*gitRemote, error) {
	match := regexp.MustCompile(gitRemoteRegex).FindStringSubmatch(s)
	if match == nil {
		return nil, status.InvalidArgumentErrorf("invalid remote %s", s)
	}
	name := strings.TrimSpace(match[1])
	urlStr := strings.TrimSpace(match[2])
	urlType := strings.TrimSpace(match[3])

	r := &gitRemote{
		name:    name,
		url:     urlStr,
		urlType: urlType,
	}
	return r, nil

}

// determineDefaultBranch parses `remoteData` (the output from `git ls-remote --symref origin`)
// and returns the HEAD branch for the repo (often `main` or `master).
//
// We expect `remoteData` to contain a string looking like
// `ref: refs/heads/main	HEAD`
// and this function would return `main`.
func determineDefaultBranch(remoteData string) (string, error) {
	re := regexp.MustCompile(`ref: refs/heads/(\S+)\s+HEAD`)
	match := re.FindStringSubmatch(remoteData)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", status.NotFoundErrorf("Failed to parse default branch from:\n%s", remoteData)
}

func runGit(args ...string) (string, error) {
	startTime := time.Now()
	defer func() {
		log.Debugf("git %s took %v", strings.Join(args, " "), time.Since(startTime).String())
	}()
	return runCommand("git", args...)
}

func runCommand(name string, args ...string) (string, error) {
	stdout := bytes.Buffer{}
	stderr := bytes.Buffer{}
	cmd := exec.Command(name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), status.UnknownErrorf("error running %s %s: %s\n%s", name, strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String(), nil
}

func isBinaryFile(path string) (bool, error) {
	fileDetails, err := runCommand("file", "--mime", path)
	if err != nil {
		return false, err
	}
	isBinary := strings.Contains(fileDetails, "charset=binary")
	return isBinary, nil
}

func diffUntrackedFile(path string) (string, error) {
	isBinary, err := isBinaryFile(path)
	if err != nil {
		return "", err
	}

	args := []string{"diff", "--no-index", "/dev/null", path}
	if isBinary {
		args = append(args, "--binary")
	}
	patch, err := runGit(args...)
	if err != nil {
		// `git diff` returns exit code 1 if there is (valid) diff. Explicitly
		// check for this case.
		if !strings.Contains(patch, "diff --git") {
			return "", err
		}
	}

	return patch, nil
}

func Config() (*RepoConfig, error) {
	remote, err := determineRemote()
	if err != nil {
		return nil, status.WrapError(err, "determine remote")
	}
	fetchURL := remote.url
	log.Debugf("Using fetch URL: %s", fetchURL)

	remoteData, err := runGit("ls-remote", "--symref", remote.name)
	if err != nil {
		return nil, status.WrapErrorf(err, "git remote show %s", remote.name)
	}

	branch, commit, err := getBaseBranchAndCommit(remoteData)
	if err != nil {
		return nil, status.WrapError(err, "get base branch and commit")
	}

	defaultBranch, err := determineDefaultBranch(remoteData)
	if err != nil {
		log.Warnf("Failed to fetch default branch: %s", err)
	}

	repoConfig := &RepoConfig{
		URL:           fetchURL,
		CommitSHA:     commit,
		Ref:           branch,
		DefaultBranch: defaultBranch,
	}

	if *runFromBranch == "" && *runFromCommit == "" {
		patches, err := generatePatches(commit)
		if err != nil {
			return nil, status.WrapError(err, "generate patches")
		}
		repoConfig.Patches = patches
	}

	return repoConfig, nil
}

// getBaseBranchAndCommit returns the git branch and commit that the remote run
// should be based off
//
// remoteData is the output from `git remote show origin`
func getBaseBranchAndCommit(remoteData string) (branch string, commit string, err error) {
	branch = *runFromBranch
	commit = *runFromCommit
	if branch != "" || commit != "" {
		return branch, commit, nil
	}

	currentBranch, err := getCurrentRef()
	if err != nil {
		return "", "", err
	}

	currentBranchExistsRemotely := branchExistsRemotely(remoteData, currentBranch)
	if currentBranchExistsRemotely {
		branch = currentBranch

		currentCommitHash, err := getHeadCommitForLocalBranch("HEAD")
		if err != nil {
			return "", "", status.WrapError(err, "get current commit hash")
		}
		currentCommitHash = strings.TrimSuffix(currentCommitHash, "\n")

		remoteCommitOutput, err := runGit("branch", "-r", "--contains", currentCommitHash)
		if err != nil {
			return "", "", status.WrapError(err, fmt.Sprintf("check if commit %s exists remotely", currentCommitHash))
		}
		currentCommitExistsRemotely := strings.Contains(remoteCommitOutput, fmt.Sprintf("origin/%s", branch))
		if currentCommitExistsRemotely {
			commit = currentCommitHash
		} else {
			remoteHeadCommit, err := getHeadCommitForRemoteBranch(remoteData, branch)
			if err != nil {
				return "", "", err
			}
			commit = remoteHeadCommit
		}
	} else {
		// If the current branch does not exist remotely, the remote runner will
		// not be able to fetch it. In this case, use the default branch for the repo
		defaultBranch, err := determineDefaultBranch(remoteData)
		if err != nil {
			return "", "", status.WrapError(err, "get default branch")
		}
		branch = defaultBranch

		defaultBranchCommitHash, err := getHeadCommitForLocalBranch(defaultBranch)
		if err != nil {
			return "", "", err
		}
		commit = defaultBranchCommitHash
	}

	log.Debugf("Using base branch: %s", branch)
	log.Debugf("Using base commit hash: %s", commit)

	return branch, commit, nil
}

// getCurrentRef returns the current branch, or the current commit if in a detached
// HEAD state
func getCurrentRef() (string, error) {
	currentBranch, err := runGit("symbolic-ref", "--short", "HEAD")
	if err == nil {
		return strings.TrimSpace(currentBranch), nil
	} else if !strings.Contains(err.Error(), "ref HEAD is not a symbolic ref") {
		return "", status.WrapError(err, "get current branch")
	}

	// Handle detached head state
	detachedHeadOutput, _ := runGit("branch")
	regex := regexp.MustCompile(".*detached at ([^)]+).*")
	matches := regex.FindStringSubmatch(detachedHeadOutput)
	if len(matches) != 2 {
		return "", status.UnknownErrorf("unexpected branch state %s", detachedHeadOutput)
	}
	return strings.TrimSpace(matches[1]), nil
}

// branchExistsRemotely parses `remoteData` (the output from “git ls-remote --symref origin)
// and returns whether `branch` is tracked remotely.
//
// If the branch is tracked remotely, we expect `remoteData` to contain a string looking like
// `abc123	refs/heads/my_branch`
func branchExistsRemotely(remoteData string, branch string) bool {
	regex := fmt.Sprintf("\\brefs/heads/%s\\b", branch)
	re := regexp.MustCompile(regex)
	return re.MatchString(remoteData)
}

// getHeadCommitForRemoteBranch parses `remoteData` (the output from “git ls-remote --symref origin)
// and returns the commit at HEAD for the remote branch.
//
//	We expect `remoteData` to contain a string looking like
//
// `abc123	refs/heads/my_branch`
// and this function would return `abc123`.
func getHeadCommitForRemoteBranch(remoteData string, branch string) (string, error) {
	regex := `\n(\S+)\s+refs/heads/` + branch + `\n`
	re := regexp.MustCompile(regex)
	match := re.FindStringSubmatch(remoteData)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", status.NotFoundErrorf("failed to get HEAD commit for remote branch %s from:\n%s", branch, remoteData)
}

// getHeadCommitForLocalBranch returns the commit at HEAD for the local branch.
func getHeadCommitForLocalBranch(branch string) (string, error) {
	headCommit, err := runGit("rev-parse", branch)
	if err != nil {
		return "", status.WrapErrorf(err, "get head commit for local branch %s", branch)
	}
	headCommit = strings.Trim(headCommit, "\n")
	return headCommit, nil
}

// generates diffs between the current state of the repo and `baseCommit`
func generatePatches(baseCommit string) ([][]byte, error) {
	modifiedFiles, err := runGit("diff", baseCommit, "--name-only")
	if err != nil {
		return nil, status.WrapError(err, "get modified files")
	}
	modifiedFiles = strings.Trim(modifiedFiles, "\n")

	binaryFilesToExclude := make([]string, 0)
	binaryFiles := make([]string, 0)
	if modifiedFiles != "" {
		for _, mf := range strings.Split(modifiedFiles, "\n") {
			isBinary, err := isBinaryFile(mf)
			if err != nil {
				return nil, status.WrapError(err, "check binary file")
			}
			if isBinary {
				binaryFilesToExclude = append(binaryFilesToExclude, fmt.Sprintf(":!%s", mf))
				binaryFiles = append(binaryFiles, mf)
			}
		}
	}

	patches := make([][]byte, 0)

	// Generate patches for non-binary files
	args := []string{"diff", baseCommit}
	if len(binaryFilesToExclude) > 0 {
		args = append(args, binaryFilesToExclude...)
	}
	patch, err := runGit(args...)
	if err != nil {
		return nil, status.WrapError(err, "git diff")
	}
	if patch != "" {
		patches = append(patches, []byte(patch))
	}

	// Generate patches for binary files
	if len(binaryFiles) > 0 {
		binaryArgs := append([]string{"diff", baseCommit, "--binary", "--"}, binaryFiles...)
		binaryPatch, err := runGit(binaryArgs...)
		if err != nil {
			return nil, status.WrapError(err, "git diff --binary")
		}
		if binaryPatch != "" {
			patches = append(patches, []byte(binaryPatch))
		}
	}

	// Generate patches for non-tracked files
	untrackedFiles, err := runGit("ls-files", "--others", "--exclude-standard")
	if err != nil {
		return nil, status.WrapError(err, "get untracked files")
	}
	untrackedFiles = strings.Trim(untrackedFiles, "\n")
	if untrackedFiles != "" {
		for _, uf := range strings.Split(untrackedFiles, "\n") {
			if strings.HasPrefix(uf, BuildBuddyArtifactDir+"/") {
				continue
			}
			patch, err := diffUntrackedFile(uf)
			if err != nil {
				return nil, status.WrapError(err, "diff untracked file")
			}
			patches = append(patches, []byte(patch))
		}
	}

	return patches, nil
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

// streamLogs streams the logs with real-time progress updates. It uses ANSI
// escape sequences to delete and rewrite outdated progress messages
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
			return err
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

// printLogs prints the logs with real-time streaming updates disabled
func printLogs(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) error {
	chunkID := ""

	for {
		l, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
			InvocationId: invocationID,
			ChunkId:      chunkID,
			MinLines:     100,
		})
		if err != nil {
			return err
		}

		if l.GetLive() {
			time.Sleep(1 * time.Second)
			continue
		}
		os.Stdout.Write(l.GetBuffer())

		if l.GetNextChunkId() == "" {
			break
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

	if len(childInRsp.GetInvocation()) < 1 {
		return nil, fmt.Errorf("invocation %s not found", invocationID)
	}
	inv := childInRsp.GetInvocation()[0]

	var outputs []*bespb.File
	for _, g := range inv.TargetGroups {
		// The `GetTarget` API only fetches file data for the general
		// STATUS_UNSPECIFIED status. For other statuses, it only returns metadata.
		if g.Status != cmnpb.Status_STATUS_UNSPECIFIED {
			continue
		}
		for _, t := range g.Targets {
			outputs = append(outputs, t.Files...)
		}
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
		outFile := filepath.Join(outputBaseDir, BuildBuddyArtifactDir)
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
		outDir := filepath.Join(outputBaseDir, BuildBuddyArtifactDir, d.GetName())
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
	env := real_environment.NewBatchEnv()

	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", opts.APIKey)

	// Handle interrupts to cancel the remote run.
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc_client.DialSimple(opts.Server)
	if err != nil {
		return 1, status.UnavailableErrorf("could not connect to BuildBuddy remote bazel service %q: %s", opts.Server, err)
	}
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)
	execClient := repb.NewExecutionClient(conn)

	reqOS := runtime.GOOS
	if *execOs != "" {
		reqOS = *execOs
	}
	reqArch := runtime.GOARCH
	if *execArch != "" {
		reqArch = *execArch
	}

	envVars := make(map[string]string, 0)
	for _, envVar := range *envInput {
		// If a value was explicitly passed in, use that
		keyValArr := strings.SplitN(envVar, "=", 2)
		if len(keyValArr) == 2 {
			key := keyValArr[0]
			val := keyValArr[1]
			envVars[key] = val
			continue
		}

		// Otherwise pull the value from the local environment
		val := os.Getenv(envVar)
		envVars[envVar] = val
	}

	// If not explicitly set, set the build user (the user that initiated the build).
	if !contains(envVars, "BUILD_USER") {
		val := os.Getenv("BUILD_USER")
		if val == "" {
			val = os.Getenv("USER")
		}
		envVars["BUILD_USER"] = val
	}

	// If not explicitly set, try to set the default branch env var,
	// because it will allow us to fallback to snapshots for the default branch
	// if there is no snapshot for the current branch
	if !(contains(envVars, "GIT_REPO_DEFAULT_BRANCH") || contains(envVars, "GIT_BASE_BRANCH")) {
		defaultBranch := strings.TrimPrefix(repoConfig.DefaultBranch, "refs/heads/")
		envVars["GIT_REPO_DEFAULT_BRANCH"] = defaultBranch
	}

	if *useSystemGitCredentials {
		envVars["USE_SYSTEM_GIT_CREDENTIALS"] = "1"
	}

	platform, err := rexec.MakePlatform(*execPropsFlag...)
	if err != nil {
		return 1, status.InvalidArgumentErrorf("invalid exec properties - key value pairs must be separated by '=': %s", err)
	}

	req := &rnpb.RunRequest{
		Name: opts.Name,
		GitRepo: &gitpb.GitRepo{
			RepoUrl:                 repoConfig.URL,
			UseSystemGitCredentials: *useSystemGitCredentials,
		},
		RepoState: &gitpb.RepoState{
			CommitSha: repoConfig.CommitSHA,
			Branch:    repoConfig.Ref,
		},
		Os:             reqOS,
		Arch:           reqArch,
		ContainerImage: *containerImage,
		Env:            envVars,
		ExecProperties: platform.Properties,
		RemoteHeaders:  *remoteHeaders,
		RunRemotely:    *runRemotely,
		Steps: []*rnpb.Step{
			{
				Run: opts.Command,
			},
		},
	}
	req.GetRepoState().Patch = append(req.GetRepoState().Patch, repoConfig.Patches...)

	if *timeout != 0 {
		req.Timeout = timeout.String()
	}

	encodedReq, err := json.Marshal(req)
	if err != nil {
		log.Debugf("Failed to marshall req: %s", err)
	}
	if len(encodedReq) > 0 {
		log.Debugf("Run request: %s", string(encodedReq))
	}

	log.Printf("\nWaiting for available remote runner...\n")
	rsp, err := bbClient.Run(ctx, req)
	if err != nil {
		return 1, status.UnknownErrorf("error running bazel: %s", err)
	}

	iid := rsp.GetInvocationId()
	log.Debugf("Invocation ID: %s", iid)

	// If the remote bazel process is canceled or killed, cancel the remote run
	isInvocationRunning := true
	go func() {
		<-ctx.Done()

		if !isInvocationRunning {
			return
		}

		// Use a non-cancelled context to ensure the remote executions are
		// canceled
		_, err = bbClient.CancelExecutions(context.WithoutCancel(ctx), &inpb.CancelExecutionsRequest{
			InvocationId: iid,
		})
		if err != nil {
			log.Warnf("Failed to cancel remote run: %s", err)
		}
	}()

	interactive := terminal.IsTTY(os.Stdin) && terminal.IsTTY(os.Stderr)
	if interactive {
		if err := streamLogs(ctx, bbClient, iid); err != nil {
			return 1, status.WrapError(err, "streaming logs")
		}
	} else {
		if err := printLogs(ctx, bbClient, iid); err != nil {
			return 1, status.WrapError(err, "streaming logs")
		}
	}
	isInvocationRunning = false

	eg := errgroup.Group{}
	var inRsp *inpb.GetInvocationResponse
	var executeResponse *repb.ExecuteResponse
	eg.Go(func() error {
		var err error
		inRsp, err = bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{Lookup: &inpb.InvocationLookup{InvocationId: iid}})
		if err != nil {
			return fmt.Errorf("could not retrieve invocation: %s", err)
		}
		if len(inRsp.GetInvocation()) == 0 {
			return fmt.Errorf("invocation not found")
		}
		return nil
	})
	eg.Go(func() error {
		execution, err := bbClient.GetExecution(ctx, &espb.GetExecutionRequest{ExecutionLookup: &espb.ExecutionLookup{
			InvocationId: iid,
		}})
		if err != nil {
			return fmt.Errorf("could not retrieve ci_runner execution: %s", err)
		}
		if len(execution.GetExecution()) == 0 {
			return fmt.Errorf("ci_runner execution not found")
		}
		executionID := execution.GetExecution()[0].GetExecutionId()
		waitExecutionStream, err := execClient.WaitExecution(ctx, &repb.WaitExecutionRequest{
			Name: executionID,
		})
		if err != nil {
			return fmt.Errorf("wait execution: %w", err)
		}
		rsp, err := rexec.Wait(rexec.NewRetryingStream(ctx, execClient, waitExecutionStream, executionID))
		if err != nil {
			return fmt.Errorf("wait execution: %v", err)
		} else if rsp.Err != nil {
			return fmt.Errorf("wait execution: %v", rsp.Err)
		} else if rsp.ExecuteResponse.GetResult() == nil {
			return fmt.Errorf("empty execute response from WaitExecution: %v", rsp.ExecuteResponse.GetStatus())
		}
		executeResponse = rsp.ExecuteResponse
		return nil
	})

	err = eg.Wait()
	if err != nil {
		return 1, err
	}

	childIID := ""
	runfilesRoot := ""
	var runfiles []*bespb.File
	var runfileDirectories []*bespb.Tree
	var defaultRunArgs []string
	for _, e := range inRsp.GetInvocation()[0].GetEvent() {
		if _, ok := e.GetBuildEvent().GetPayload().(*bespb.BuildEvent_ChildInvocationCompleted); ok {
			childIID = e.GetBuildEvent().GetId().GetChildInvocationCompleted().GetInvocationId()
		}
		if opts.RunOutputLocally {
			if rta, ok := e.GetBuildEvent().GetPayload().(*bespb.BuildEvent_RunTargetAnalyzed); ok {
				runfilesRoot = rta.RunTargetAnalyzed.GetRunfilesRoot()
				runfiles = rta.RunTargetAnalyzed.GetRunfiles()
				runfileDirectories = rta.RunTargetAnalyzed.GetRunfileDirectories()
				defaultRunArgs = rta.RunTargetAnalyzed.GetArguments()
			}
		}
	}

	exitCode := int(executeResponse.GetResult().GetExitCode())
	if opts.FetchOutputs && exitCode == 0 {
		if childIID != "" {
			conn, err := grpc_client.DialSimple(opts.Server)
			if err != nil {
				return 1, fmt.Errorf("could not communicate with sidecar: %s", err)
			}
			env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
			env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
			ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", opts.APIKey)

			mainOutputs, err := lookupBazelInvocationOutputs(ctx, bbClient, childIID)
			if err != nil {
				return 1, err
			}
			outputsBaseDir := filepath.Dir(opts.WorkspaceFilePath)
			outputs, err := downloadOutputs(ctx, env, mainOutputs, runfiles, runfileDirectories, outputsBaseDir)
			if err != nil {
				return 1, err
			}
			if opts.RunOutputLocally {
				if len(outputs) > 1 {
					return 1, fmt.Errorf("run requested but target produced more than one artifact")
				}
				binPath := outputs[0]
				if err := os.Chmod(binPath, 0755); err != nil {
					return 1, fmt.Errorf("could not prepare binary %q for execution: %s", binPath, err)
				}
				execArgs := defaultRunArgs
				// Pass through extra arguments (-- --foo=bar) from the command line.
				execArgs = append(execArgs, opts.ExecArgs...)
				log.Debugf("Executing %q with arguments %s", binPath, execArgs)
				cmd := exec.CommandContext(ctx, binPath, execArgs...)
				cmd.Dir = filepath.Join(outputsBaseDir, BuildBuddyArtifactDir, runfilesRoot)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err = cmd.Run()
				if e, ok := err.(*exec.ExitError); ok {
					return e.ExitCode(), nil
				} else if err != nil {
					return 1, err
				}
				return 0, nil
			}
		} else {
			log.Warnf("Cannot download outputs - no child invocations found")
		}
	}

	return exitCode, nil
}

func HandleRemoteBazel(commandLineArgs []string) (int, error) {
	commandLineArgs, err := parseRemoteCliFlags(commandLineArgs)
	if err != nil {
		return 1, status.WrapError(err, "parse cli flags")
	}

	tempDir, err := os.MkdirTemp("", "buildbuddy-cli-*")
	if err != nil {
		return 1, err
	}
	defer func() {
		os.RemoveAll(tempDir)
	}()

	ctx := context.Background()
	repoConfig, err := Config()
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

	cmd := ""
	remoteRunName := "remote run"
	apiKey := ""
	fetchOutputs := false
	runOutputLocally := false
	var localExecArgs []string
	if *script != "" {
		cmd = *script

		// Read API key from command line if it is set.
		apiKey = arg.Get(commandLineArgs, "remote_header=x-buildbuddy-api-key")
	} else {
		// If no script passed in, parse the bazel command to run from the command line.
		bazelArgs, execArgs, err := parseArgs(commandLineArgs)
		if err != nil {
			return 1, status.WrapError(err, "parse args")
		}

		// Read API key from command line if it is set.
		apiKey = arg.Get(bazelArgs, "remote_header=x-buildbuddy-api-key")

		bazelCmd, _ := parser.GetBazelCommandAndIndex(bazelArgs)
		if bazelCmd == "build" || (bazelCmd == "run" && !*runRemotely) {
			fetchOutputs = true
			if bazelCmd == "run" {
				runOutputLocally = true
			}
		}

		remoteRunName = fmt.Sprintf("remote %s %s", bazelCmd, parser.GetFirstTargetPattern(bazelArgs))

		// If we are running the target locally, remove the exec arguments for now,
		// and append them when we actually run it
		if runOutputLocally {
			// Use shlex.Quote so that the command will be correctly parsed by the shell
			// command line.
			quotedArgs := shlex.Quote(bazelArgs...)

			// To support building the target on the remote runner and running it locally,
			// have Bazel write out a run script using the --script_path flag so we can
			// extract run options (i.e. args, runfile information) from the generated run script.
			//
			// We do not pass this to shlex.Quote, or the env var won't be expanded
			// correctly.
			extraFlags := fmt.Sprintf("--script_path=$BUILDBUDDY_CI_RUNNER_ROOT_DIR/%s/run.sh", runScriptDirName)

			cmd = fmt.Sprintf("bazel %s %s", quotedArgs, extraFlags)
			localExecArgs = execArgs
		} else {
			cmd = fmt.Sprintf("bazel %s", shlex.Quote(arg.JoinExecutableArgs(bazelArgs, execArgs)...))
		}
	}

	// If an API key was not set in the command line, attempt to read from config.
	if apiKey == "" {
		apiKey, err = getAPIKeyFromConfig()
		if err != nil {
			return 1, err
		}
	}

	exitCode, err := Run(ctx, RunOpts{
		Server:            runner,
		APIKey:            apiKey,
		Name:              remoteRunName,
		Command:           cmd,
		RunOutputLocally:  runOutputLocally,
		ExecArgs:          localExecArgs,
		FetchOutputs:      fetchOutputs,
		WorkspaceFilePath: wsFilePath,
	}, repoConfig)
	if err != nil && strings.Contains(err.Error(), "context canceled") {
		return exitCode, nil
	}
	return exitCode, err
}

func parseArgs(commandLineArgs []string) (bazelArgs []string, execArgs []string, err error) {
	bazelArgs, execArgs = arg.SplitExecutableArgs(commandLineArgs)

	bazelArgs, err = login.ConfigureAPIKey(bazelArgs)
	if err != nil {
		return nil, nil, err
	}
	bazelArgs, err = parser.CanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, nil, err
	}

	// Ensure all bazel remote runs use the remote cache.
	// The goal is to keep remote workloads close to our servers, so use the same
	// app backend as the remote runner.
	bazelArgs = arg.Remove(bazelArgs, "bes_backend")
	bazelArgs = arg.Remove(bazelArgs, "remote_cache")
	bazelArgs = append(bazelArgs, "--config=buildbuddy_bes_backend")
	bazelArgs = append(bazelArgs, "--config=buildbuddy_bes_results_url")
	bazelArgs = append(bazelArgs, "--config=buildbuddy_remote_cache")

	// If the CLI needs to fetch build outputs, make sure the remote runner uploads them.
	bazelCmd, _ := parser.GetBazelCommandAndIndex(bazelArgs)
	if (!*runRemotely && bazelCmd == "run") || bazelCmd == "build" {
		bazelArgs = append(bazelArgs, "--remote_upload_local_results")
	}

	return bazelArgs, execArgs, nil
}

// parseRemoteCliFlags parses flags that affect configuration of remote bazel.
// These flags are defined in `RemoteFlagset`.
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
	// a flag not in RemoteFlagset, but we might expect that if bazel startup flags
	// are set
	RemoteFlagset.SetOutput(io.Discard)

	runBashScript := false
	for _, a := range args {
		if strings.HasPrefix(a, "--script") {
			runBashScript = true
			break
		}
	}

	endParsingIndex := len(args)
	if !runBashScript {
		// Stop parsing flags when we reach the bazel command
		_, bazelCmdIdx := parser.GetBazelCommandAndIndex(args)
		if bazelCmdIdx == -1 {
			return nil, status.InvalidArgumentErrorf("no bazel command passed to run remotely")
		}
		endParsingIndex = bazelCmdIdx
	}

	unparsedArgs := args[:endParsingIndex]
	for len(unparsedArgs) > 0 {
		err := RemoteFlagset.Parse(unparsedArgs)
		if err == nil {
			// flagset.Args() contains the list of any unparsed arguments
			// Keep parsing them in a loop until we process all the args
			unparsedArgs = RemoteFlagset.Args()
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
	argsRemoteFlagsRemoved := args[:endParsingIndex]
	RemoteFlagset.VisitAll(func(f *flag.Flag) {
		// Certain flags with slice values can be passed multiple times.
		// Remove all instances.
		flagVal := "start"
		for flagVal != "" {
			flagVal, argsRemoteFlagsRemoved = arg.Pop(argsRemoteFlagsRemoved, f.Name)
		}
	})

	// Add back in the bazel command and any subsequent flags
	argsRemoteFlagsRemoved = append(argsRemoteFlagsRemoved, args[endParsingIndex:]...)
	return argsRemoteFlagsRemoved, nil
}

func contains(m map[string]string, elem string) bool {
	_, ok := m[elem]
	return ok
}

// getAPIKeyFromConfig attempts to read an API key from the buildbuddy config
// set at the key `buildbuddy.api-key` in .git/config. If it isn't set, will
// prompt the user to set it.
func getAPIKeyFromConfig() (string, error) {
	apiKey, err := storage.ReadRepoConfig("api-key")
	if err != nil {
		log.Debugf("Could not read api key from bb config: %s", err)
	} else {
		log.Debugf("API key read from `buildbuddy.api-key` in .git/config.")
	}
	if apiKey != "" {
		return apiKey, nil
	}

	// If an API key is not set, prompt the user to set it in their cli config.
	if _, err := login.HandleLogin([]string{}); err == nil {
		log.Warnf("Failed to enter login flow. Manually trigger with " +
			"`bb login` or add an API key to your remote bazel run with `--remote_header=x-buildbuddy-api-key=XXX`.")
		return "", status.WrapError(err, "handle login")
	}
	apiKey, err = storage.ReadRepoConfig("api-key")
	if err != nil {
		return "", status.WrapError(err, "read api key from bb config")
	}
	return apiKey, nil
}
