package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "embed"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/overlayfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_util"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/fspath"
	"github.com/buildbuddy-io/buildbuddy/server/util/gds"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/gobwas/glob"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	ci_runner_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cli_bundle "github.com/buildbuddy-io/buildbuddy/server/util/bb"
)

var (
	// WorkspaceMarkedForRemovalError is returned from workspace operations
	// whenever Remove was previously called on the workspace.
	WorkspaceMarkedForRemovalError = status.UnavailableError("workspace is marked for removal")

	deleteParallelism = flag.Int("executor.delete_parallelism", 0, "Number of goroutines to use when deleting files.")
	deleteWaitGroup   = errgroup.Group{}
	deleteLimitSet    = sync.Once{}

	vfsVerbose             = flag.Bool("executor.vfs.verbose", false, "Enables verbose logs for VFS operations.")
	vfsVerboseFUSEOps      = flag.Bool("executor.vfs.verbose_fuse", false, "Enables low-level verbose logs in the go-fuse library.")
	vfsLogFUSELatencyStats = flag.Bool("executor.vfs.log_fuse_latency_stats", false, "Enables logging of per-operation latency stats when VFS is unmounted. Implicitly enabled by --executor.vfs.verbose.")
	vfsLogFUSEPerFileStats = flag.Bool("executor.vfs.log_fuse_per_file_stats", false, "Enables tracking and logging of per-file per-operation stats. Logged when VFS is unmounted.")

	poolSlots       = flag.Int("executor.workspace_pool.slots", 0, "Number of finished task workspaces that the pool can hold for reuse by later tasks with similar input trees, when the executor.workspace_pool.enabled experiment is on. 0 means the number of CPU cores allocated to tasks, i.e. how many 1-core tasks can run at once. A negative value disables reuse.")
	poolMaxFileSize = flag.Int64("executor.workspace_pool.max_file_size_bytes", 1<<20, "Input files larger than this are removed from a workspace before it enters the reuse pool, so that pooled workspaces don't keep large files on disk after the file cache evicts them. This is mostly a hedge against rare edge cases, since pooled workspaces only hold hard links to file cache entries, and the file cache rarely evicts files that pooled workspaces still use. 0 means no size limit.")
)

func setDeleteLimit() {
	deleteLimitSet.Do(func() {
		limit := *deleteParallelism
		if limit == 0 {
			limit = runtime.GOMAXPROCS(0)
		}
		deleteWaitGroup.SetLimit(limit)
	})
}

// Workspace holds the working tree for an action and keeps track of
// inputs and outputs.
type Workspace struct {
	env     environment.Env
	rootDir string
	overlay *overlayfs.Overlay
	// dirPerms are the permissions set on the workspace root directory as well as
	// any input or output directories created by the executor. It does not affect
	// file permissions.
	dirPerms  fs.FileMode
	task      *repb.ExecutionTask
	dirHelper *dirtools.DirHelper
	Opts      *Opts

	// VFS fields are only set if VFS is enabled.
	// vfs is the FUSE implementation that delegates work to the VFS server.
	vfs *vfs.VFS
	// vfsServer contains most of the smarts of the VFS implementation.
	vfsServer *vfs_server.Server

	// Action input files known to exist in the workspace, as a map of
	// workspace-relative paths to file nodes.
	// TODO: Make sure these files are written read-only
	// to make sure this map accurately reflects the filesystem.
	Inputs map[fspath.Key]*repb.FileNode

	mu          sync.Mutex // protects(removing, treeFetcher)
	removing    bool
	treeFetcher *dirtools.TreeFetcher

	// State for reusing the workspace through Opts.Pool, which we set when
	// setting up inputs. It records the input tree that the workspace holds,
	// and the group whose tasks may reuse the workspace once it returns to the
	// pool. inputDirs holds the tree's directories by digest hash rather than
	// by path, so we find a path's directory by walking down from
	// inputRootDigest.
	inputRootDigest     *repb.Digest
	inputDirs           map[string]*repb.Directory
	inputDigestFunction repb.DigestFunction_Value
	groupID             string
	// Each input directory's fingerprint right after inputs were set up, by
	// workspace-relative path. ReturnToPool compares against them to find
	// the directories the task changed.
	dirFingerprints map[string]dirFingerprint
}

// dirFingerprint identifies the state of a directory. The kernel updates a
// directory's ctime whenever an entry is added, removed or renamed, or the
// directory's own attributes change, and userspace can't set ctime back. So if
// a directory's fingerprint still matches, its entries are the same inodes as
// when we recorded it. Writing to a file in place doesn't change its inode or
// its directory's ctime, so we only pool workspaces whose inputs are hard links
// to the file cache, which the file cache already relies on tasks not writing
// to. We also record the mode and owner, so that we recreate a directory whose
// mode or owner the task changed instead of passing it on to the next task.
type dirFingerprint struct {
	ino        uint64
	ctimeNanos int64
	mode       uint32
	uid        uint32
	gid        uint32
}

type VFSPrefetchMode string

const (
	VFSPrefetchModeAll  VFSPrefetchMode = platform.VFSPrefetchModeAll
	VFSPrefetchModeNone VFSPrefetchMode = platform.VFSPrefetchModeNone
)

type Opts struct {
	// Preserve specifies whether to preserve all files in the workspace except
	// for output paths.
	Preserve    bool
	CleanInputs string
	// CaseInsensitive specifies whether the root directory (under which the
	// workspace directory is created) is case-insensitive.
	CaseInsensitive bool
	// UseOverlayfs specifies whether the workspace should use overlayfs to
	// allow copy-on-write for workspace inputs.
	UseOverlayfs bool
	// UseVFS specifies whether the workspace should use a FUSE virtual file
	// system to serve CAS artifacts and scratch files.
	UseVFS bool
	// VFSPrefetchMode controls whether VFS inputs are prefetched.
	VFSPrefetchMode VFSPrefetchMode
	// Pool, if set, lets this workspace start from the directory of a
	// finished task with a similar input tree, and lets ReturnToPool keep this
	// workspace's directory for later tasks. Workspaces are only reused within
	// the group of the task's authenticated user.
	Pool *Pool
}

// New creates a new workspace directly under the given parent directory.
func New(env environment.Env, parentDir string, opts *Opts) (*Workspace, error) {
	if opts.VFSPrefetchMode != "" && opts.VFSPrefetchMode != VFSPrefetchModeAll && opts.VFSPrefetchMode != VFSPrefetchModeNone {
		return nil, status.InvalidArgumentErrorf("invalid VFS prefetch mode %q", opts.VFSPrefetchMode)
	}
	dirPerms := fs.FileMode(0777)
	var rootDir string
	maxAttempts := 10
	for i := range maxAttempts {
		rootDir = filepath.Join(parentDir, newRandomBuildDirCandidate())
		if err := os.Mkdir(rootDir, dirPerms); err == nil {
			break
		} else if !os.IsExist(err) {
			return nil, status.UnavailableErrorf("failed to create workspace at %q: %s", rootDir, err)
		}
		// Got unlucky and the random directory name already exists. This should
		// never happen on Unix and rarely on Windows, so just try again.
		if i == maxAttempts-1 {
			return nil, status.UnavailableErrorf("failed to find random dir below %q in %d attempts", rootDir, maxAttempts)
		}
	}
	rootDir, err := maybeCreatePlatformSpecificSubDir(rootDir)
	if err != nil {
		return nil, err
	}

	if opts.UseOverlayfs && opts.UseVFS {
		return nil, status.InvalidArgumentErrorf("Overlayfs and VFS cannot be enabled at the same time")
	}

	var overlay *overlayfs.Overlay
	if opts.UseOverlayfs {
		overlayOpts := overlayfs.Opts{DirPerms: dirPerms}
		var err error
		overlay, err = overlayfs.Convert(context.TODO(), rootDir, overlayOpts)
		if err != nil {
			return nil, status.UnavailableErrorf("failed to create workspace overlayfs at %q: %s", rootDir, err)
		}
	}

	var vfs *vfs.VFS
	var vfsServer *vfs_server.Server
	if opts.UseVFS {
		v, s, err := startVFS(env, rootDir)
		if err != nil {
			return nil, err
		}
		vfs = v
		vfsServer = s
	}

	setDeleteLimit()
	return &Workspace{
		env:       env,
		rootDir:   rootDir,
		overlay:   overlay,
		dirPerms:  dirPerms,
		vfs:       vfs,
		vfsServer: vfsServer,
		Opts:      opts,
		Inputs:    map[fspath.Key]*repb.FileNode{},
	}, nil
}

func startVFS(env environment.Env, path string) (*vfs.VFS, *vfs_server.Server, error) {
	var vfsServer *vfs_server.Server
	scratchDir := path + ".scratch"
	if err := os.Mkdir(scratchDir, 0755); err != nil {
		return nil, nil, status.UnavailableErrorf("could not create FUSE scratch dir: %s", err)
	}

	vfsServer, err := vfs_server.New(env, scratchDir)
	if err != nil {
		return nil, nil, err
	}
	fs := vfs.New(vfs_server.NewDirectClient(vfsServer), path, &vfs.Options{
		EnablePassthrough:   true,
		Verbose:             *vfsVerbose,
		LogFUSEOps:          *vfsVerboseFUSEOps,
		LogFUSELatencyStats: *vfsLogFUSELatencyStats,
		LogFUSEPerFileStats: *vfsLogFUSEPerFileStats,
	})
	if err := fs.Mount(); err != nil {
		return nil, nil, status.UnavailableErrorf("unable to mount VFS at %q: %s", path, err)
	}
	return fs, vfsServer, nil
}

// Path returns the absolute path to the workspace root directory, which should
// be used as the action working directory. If overlayfs is enabled, this will
// be the path where the overlayfs is mounted.
func (ws *Workspace) Path() string {
	return ws.rootDir
}

// inputRoot returns the directory where inputs should be downloaded. For
// overlayfs, this is the overlay lower directory. Otherwise, this behaves the
// same as Path().
func (ws *Workspace) inputRoot() string {
	if ws.overlay == nil {
		return ws.Path()
	}
	return ws.overlay.LowerDir
}

// SetTask sets the next task to be executed within the workspace.
func (ws *Workspace) SetTask(ctx context.Context, task *repb.ExecutionTask) {
	log.CtxDebugf(ctx, "Assigned task %s to workspace at %q", task.GetExecutionId(), ws.rootDir)
	ws.task = task
	cmd := task.GetCommand()
	ws.dirHelper = dirtools.NewDirHelper(filepath.Join(ws.inputRoot(), cmd.GetWorkingDirectory()), cmd, ws.dirPerms)
}

// CommandWorkingDirectory returns the absolute path to the working directory
// for the command to be executed. It assumes that any container executing the
// task will have the workspace mounted at a path identical to its path on the
// executor. For example: /buildbuddy/remotebuilds/abc on the executor's
// filesystem is expected to be mounted to /buildbuddy/remotebuilds/abc in the
// container.
func (ws *Workspace) CommandWorkingDirectory() string {
	dir := ws.Path()
	if wd := ws.task.GetCommand().GetWorkingDirectory(); wd != "" {
		dir = filepath.Join(dir, wd)
	}
	return dir
}

// CreateOutputDirs creates the required output directories for the current
// action.
//
// It must be called after DownloadInputs, which records the input directory
// fingerprints of a workspace with a Pool, so that ReturnToPool sees the
// directories it adds entries to as changed.
func (ws *Workspace) CreateOutputDirs() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return WorkspaceMarkedForRemovalError
	}

	return ws.dirHelper.CreateOutputDirs()
}

func (ws *Workspace) prepareVFS(ctx context.Context, layout *container.FileSystemLayout, inputFetcher container.InputFetcher) error {
	if ws.vfs == nil {
		return status.FailedPreconditionError("vfs cannot be null if vfsServer is set")
	}

	invalidatedInodes, err := ws.vfsServer.Prepare(ctx, layout, inputFetcher)
	if err != nil {
		return err
	}
	if err := ws.vfs.PrepareForTask(ctx, ws.task.GetExecutionId(), invalidatedInodes); err != nil {
		return err
	}

	return nil
}

// validateWorkingDirectoryInTree checks that the given working directory path
// exists as a directory in the input tree. This enforces the REAPI spec
// requirement that working_directory "must be a directory which exists in the
// input tree."
func validateWorkingDirectoryInTree(tree *repb.Tree, digestFunction repb.DigestFunction_Value, workingDir string) error {
	_, dirMap, err := dirtools.DirMapFromTree(tree, digestFunction)
	if err != nil {
		return status.WrapError(err, "failed to build directory map from input tree")
	}

	// Walk the tree path segment by segment starting from root.
	current := tree.GetRoot()
	segments := strings.SplitSeq(workingDir, "/")
	for seg := range segments {
		found := false
		for _, dirNode := range current.GetDirectories() {
			if dirNode.GetName() == seg {
				next, ok := dirMap[digest.NewKey(dirNode.GetDigest())]
				if !ok {
					return status.FailedPreconditionErrorf("working_directory %q not found in input tree", workingDir)
				}
				current = next
				found = true
				break
			}
		}
		if !found {
			return status.FailedPreconditionErrorf("working_directory %q does not exist in the input tree", workingDir)
		}
	}
	return nil
}

// DownloadInputs downloads any missing inputs for the current action.
func (ws *Workspace) DownloadInputs(ctx context.Context, layout *container.FileSystemLayout) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return WorkspaceMarkedForRemovalError
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	execReq := ws.task.GetExecuteRequest()

	// The REAPI spec requires working_directory to be a directory that
	// already exists in the input tree. Validate this against the tree.
	if wd := ws.task.GetCommand().GetWorkingDirectory(); wd != "" {
		if err := validateWorkingDirectoryInTree(layout.Inputs, execReq.GetDigestFunction(), wd); err != nil {
			return err
		}
	}

	vfsPrefetchMode := ws.Opts.VFSPrefetchMode
	usesVFS := ws.vfs != nil || vfsPrefetchMode != ""
	if vfsPrefetchMode == VFSPrefetchModeNone {
		layout.InputFetcher = nil
		ws.treeFetcher = nil
		if ws.vfs != nil {
			if err := ws.prepareVFS(ctx, layout, nil); err != nil {
				return err
			}
		}
		return nil
	}

	opts := &dirtools.DownloadTreeOpts{CaseInsensitive: ws.Opts.CaseInsensitive}
	if !usesVFS {
		opts.RootDir = ws.inputRoot()
	}
	opts.ChunkedInputFiles = slices.Contains(ws.task.GetExperiments(), "executor.download_inputs_chunked")
	opts.RecordInputFetchMetadata = execution_experiments.RecordInputFetchMetadata.Get(ctx)
	if ws.Opts.Preserve {
		opts.KnownInputs = ws.Inputs
		opts.TrackTransfers = true
	}
	// The runner only sets Opts.Pool for workspaces that we can pool.
	reusable := ws.Opts.Pool != nil
	if reusable {
		// The tree fetcher checks names too, but we build paths from them
		// earlier, when cleaning up a pooled workspace for reuse.
		if err := dirtools.ValidateTreeNames(layout.Inputs, ws.Opts.CaseInsensitive); err != nil {
			return err
		}
		root, dirMap, err := dirtools.DirMapFromTree(layout.Inputs, execReq.GetDigestFunction())
		if err != nil {
			return status.WrapError(err, "build directory map from input tree")
		}
		inputDirs := make(map[string]*repb.Directory, len(dirMap))
		for k, d := range dirMap {
			inputDirs[k.Hash] = d
		}
		ws.inputRootDigest, ws.inputDirs, ws.inputDigestFunction = root, inputDirs, execReq.GetDigestFunction()
		ws.groupID = filecache.GroupIDStringFromContext(ctx)
		kept, err := ws.reusePooledWorkspace(ctx)
		if err != nil {
			return status.UnavailableErrorf("clear workspace after failing to reuse a pooled one: %s", err)
		}
		if kept != nil {
			opts.KnownInputs = kept
		}
	}
	tf, err := dirtools.NewTreeFetcher(ctx, ws.env, execReq.GetInstanceName(), execReq.GetDigestFunction(), layout.Inputs, opts)
	if err != nil {
		return status.WrapErrorf(err, "could not create tree fetcher")
	}
	ws.treeFetcher = tf
	if usesVFS {
		layout.InputFetcher = tf
	}

	// Start fetching inputs.
	inputsState, err := tf.Start()
	if err != nil {
		return status.WrapErrorf(err, "could not start tree fetcher")
	}

	// Inform VFS about the layout of the input tree and give it access to the
	// running tree fetcher.
	if usesVFS {
		if ws.vfs != nil {
			if err := ws.prepareVFS(ctx, layout, tf); err != nil {
				return err
			}
		}
	} else {
		// If we're not using FUSE, wait for the input tree to be fully downloaded.
		txInfo, err := ws.treeFetcher.Wait()
		if err != nil {
			return status.WrapError(err, "could not fetch inputs")
		}
		mbps := (float64(txInfo.BytesTransferred) / float64(1e6)) / float64(txInfo.TransferDuration.Seconds())
		span.SetAttributes(attribute.Int64("file_count", txInfo.FileCount))
		span.SetAttributes(attribute.Int64("bytes_transferred", txInfo.BytesTransferred))
		log.CtxDebugf(ctx, "DownloadTree linked %d files in %s, downloaded %d bytes in %s [%2.2f MB/sec]", txInfo.LinkCount, txInfo.LinkDuration, txInfo.BytesTransferred, txInfo.TransferDuration, mbps)
		if reusable {
			// Now that the inputs are in place, and before the task runs, we
			// record a fingerprint of each input directory. When the workspace
			// returns to the pool, we compare against these, so that we only
			// need to read the directories that the task changed.
			if err := ws.recordDirFingerprints(); err != nil {
				log.CtxWarningf(ctx, "Workspace won't be pooled, because recording directory fingerprints failed: %s", err)
				ws.dirFingerprints = nil
			}
		}
	}

	// Now that the input tree is setup, remove any unwanted inputs.
	// Don't touch any inputs specified by the current action as represented
	// by inputsState.Exist
	if err := ws.CleanInputsIfNecessary(inputsState.Exist); err != nil {
		return err
	}

	// Update the input tracking map to include inputs specified by the action
	// that were not in the workspace yet.
	for relPath, node := range inputsState.NeedFetching {
		ws.Inputs[fspath.NewKey(relPath, ws.Opts.CaseInsensitive)] = node
	}

	return nil
}

func (ws *Workspace) AddRemoteRunnerBinaries(ctx context.Context) error {
	if err := ws.AddCIRunner(ctx); err != nil {
		return err
	}
	// The bb CLI is not embedded on Windows, but the CI runner can still use
	// its embedded Bazelisk binary.
	if len(cli_bundle.CLIBytes) > 0 {
		if err := ws.AddCLI(ctx); err != nil {
			return err
		}
	}
	return nil
}

// AddCLI adds the bb CLI to the workspace root if it doesn't already exist.
func (ws *Workspace) AddCLI(ctx context.Context) error {
	// The CLI binary is not embedded on Windows due to a CGo dependency that isn't supported.
	if len(cli_bundle.CLIBytes) == 0 {
		return status.UnimplementedErrorf("CLI binary not embedded")
	}
	// Don't add CLI if the workspace is backed by FUSE.
	if ws.vfs != nil {
		return status.UnimplementedErrorf("AddCLI not supported on VFS")
	}
	destPath := path.Join(ws.Path(), ci_runner_util.CLIBinaryName)
	exists, err := disk.FileExists(ctx, destPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	return os.WriteFile(destPath, cli_bundle.CLIBytes, 0o555)
}

// AddCIRunner adds the BuildBuddy CI runner to the workspace root if it doesn't
// already exist.
func (ws *Workspace) AddCIRunner(ctx context.Context) error {
	// Don't add CI runner if the workspace is backed by FUSE.
	if ws.vfs != nil {
		return status.UnimplementedErrorf("AddCIRunner not supported on VFS")
	}
	destPath := path.Join(ws.Path(), ci_runner_util.ExecutableName)
	exists, err := disk.FileExists(ctx, destPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return os.WriteFile(destPath, ci_runner_bundle.CiRunnerBytes, 0o555)
}

func (ws *Workspace) CleanInputsIfNecessary(keep map[fspath.Key]*repb.FileNode) error {
	if ws.Opts.CleanInputs == "" {
		return nil
	}
	inputFilesToCleanUp := make(map[fspath.Key]*repb.FileNode)
	// Curly braces indicate a comma separated list of patterns: https://pkg.go.dev/github.com/gobwas/glob#Compile
	glob, err := glob.Compile(fmt.Sprintf("{%s}", ws.Opts.CleanInputs), os.PathSeparator)
	if err != nil {
		return status.FailedPreconditionErrorf("Invalid glob {%s} used for input cleaning: %s", ws.Opts.CleanInputs, err.Error())
	}
	for path, node := range ws.Inputs {
		// NOTE: the glob is matching against the normalized path key here
		// (which is always lowercase on case-insensitive filesystems), not the
		// path string.
		if ws.Opts.CleanInputs == "*" || glob.Match(path.NormalizedString()) {
			inputFilesToCleanUp[path] = node
		}
	}
	for path := range keep {
		delete(inputFilesToCleanUp, path)
	}
	if len(inputFilesToCleanUp) > 0 {
		for path := range inputFilesToCleanUp {
			if err := os.RemoveAll(filepath.Join(ws.Path(), path.NormalizedString())); err != nil && !os.IsNotExist(err) {
				return status.UnavailableErrorf("Failed to clean inputs: %s", err)
			}
			delete(ws.Inputs, path)
		}
	}
	return nil
}

// UploadOutputs uploads any outputs created by the last executed command
// as well as the command's stdout and stderr.
func (ws *Workspace) UploadOutputs(ctx context.Context, cmd *repb.Command, executeResponse *repb.ExecuteResponse, cmdResult *interfaces.CommandResult) (*dirtools.TransferInfo, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return nil, WorkspaceMarkedForRemovalError
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	bsClient := ws.env.GetByteStreamClient()
	instanceName := ws.task.GetExecuteRequest().GetInstanceName()
	digestFunction := ws.task.GetExecuteRequest().GetDigestFunction()

	var txInfo *dirtools.TransferInfo
	executeResponse.ServerLogs = make(map[string]*repb.LogFile, len(cmdResult.AuxiliaryLogs))

	// Let independent uploads finish even if another upload fails, preserving
	// available outputs and diagnostics within the caller's deadline.
	var wg sync.WaitGroup
	var stdoutErr, stderrErr, treeErr error
	wg.Go(func() {
		d, err := cachetools.UploadBlob(ctx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.Stdout))
		if err != nil {
			stdoutErr = status.UnavailableErrorf("upload stdout: %w", err)
			return
		}
		executeResponse.Result.StdoutDigest = d
	})
	wg.Go(func() {
		d, err := cachetools.UploadBlob(ctx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.Stderr))
		if err != nil {
			stderrErr = status.UnavailableErrorf("upload stderr: %w", err)
			return
		}
		executeResponse.Result.StderrDigest = d
	})
	wg.Go(func() {
		if ws.overlay != nil {
			// When overlayfs is enabled, apply the changes from upperdir to
			// lowerdir, since dirHelper's root dir is configured as the
			// lowerdir and it needs to see the output files.
			//
			// Optimization: if recycling is not enabled, then at this point the
			// runner should be removed and cannot affect any files in the
			// workspace anymore, so it is safe to rename the outputs files in
			// upperdir here rather than copying.
			recyclingEnabled := platform.IsRecyclingEnabled(ws.task)
			opts := overlayfs.ApplyOpts{AllowRename: !recyclingEnabled}
			if err := ws.overlay.Apply(ctx, opts); err != nil {
				treeErr = status.WrapError(err, "apply overlay upperdir changes")
				return
			}
		}
		// Don't try to add files to the filecache if VFS is enabled. We use hard links to store data in the file
		// cache and hard links across filesystems are not possible.
		addToFileCache := ws.vfs == nil
		txInfo, treeErr = dirtools.UploadTree(
			ctx,
			ws.env,
			ws.dirHelper,
			instanceName,
			digestFunction,
			filepath.Join(ws.inputRoot(), cmd.GetWorkingDirectory()),
			cmd,
			executeResponse.Result,
			addToFileCache,
			ws.task.GetFastCdc_2020Params(),
		)
	})
	var logsMu sync.Mutex
	auxiliaryErrors := make([]error, len(cmdResult.AuxiliaryLogs))
	for i, name := range slices.Sorted(maps.Keys(cmdResult.AuxiliaryLogs)) {
		wg.Go(func() {
			d, err := cachetools.UploadBlob(ctx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.AuxiliaryLogs[name]))
			if err != nil {
				auxiliaryErrors[i] = fmt.Errorf("upload auxiliary log %q: %w", name, err)
				return
			}
			logsMu.Lock()
			defer logsMu.Unlock()
			executeResponse.ServerLogs[name] = &repb.LogFile{
				Digest:        d,
				HumanReadable: true,
			}
		})
	}
	wg.Wait()
	if err := errors.Join(stdoutErr, stderrErr, treeErr, errors.Join(auxiliaryErrors...)); err != nil {
		return nil, err
	}
	txInfo.FileCount += 2 // for stdout and stderr
	txInfo.BytesTransferred += int64(len(cmdResult.Stdout) + len(cmdResult.Stderr))
	txInfo.FileCount += int64(len(cmdResult.AuxiliaryLogs))
	for _, b := range cmdResult.AuxiliaryLogs {
		txInfo.BytesTransferred += int64(len(b))
	}
	span.SetAttributes(attribute.Int64("file_count", txInfo.FileCount))
	span.SetAttributes(attribute.Int64("bytes_transferred", txInfo.BytesTransferred))
	return txInfo, nil
}

func (ws *Workspace) stopVFS(ctx context.Context) error {
	var unmountErr error
	if ws.vfs != nil {
		unmountErr = ws.vfs.Unmount()
	}
	if ws.vfsServer != nil {
		ws.vfsServer.Stop()
		errorChan := make(chan error)
		deleteWaitGroup.Go(func() error {
			errorChan <- disk.ForceRemove(ctx, ws.vfsServer.Path())
			return nil
		})
		if removeErr := <-errorChan; removeErr != nil {
			if unmountErr == nil {
				return status.InternalErrorf("failed to clean up vfs scratch directory: %s", removeErr)
			} else {
				log.CtxWarningf(ctx, "Failed to clean up vfs scratch directory: %s", removeErr)
			}
		}
	}
	return unmountErr
}

func (ws *Workspace) Remove(ctx context.Context) error {
	ws.mu.Lock()
	ws.removing = true
	// No need to keep the lock held while removing; other operations will
	// immediately fail since we've set the removing bit.
	ws.mu.Unlock()

	if ws.overlay != nil {
		return ws.overlay.Remove(ctx)
	}

	if ws.vfs != nil {
		return ws.stopVFS(ctx)
	}

	errorChan := make(chan error)
	// Sometimes removal fails if badly-behaved actions write their
	// directories read-only. Use force-removal to handle these cases.
	deleteWaitGroup.Go(func() error {
		errorChan <- disk.ForceRemove(ctx, ws.rootDir)
		return nil
	})

	if err := <-errorChan; err != nil {
		return status.InternalErrorf("failed to force-remove workspace: %s", err)
	}
	return nil
}

// Size computes the current workspace size in bytes.
func (ws *Workspace) DiskUsageBytes() (int64, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return 0, WorkspaceMarkedForRemovalError
	}

	return disk.DirSize(ws.Path())
}

// ComputeVFSStats returns vfs-specific stats, if vfs is enabled.
//
// When VFS is enabled in the workspace, the VFS implementation keeps track
// of its own IO stats and a call is necessary to propagate these stats to the
// action results.
func (ws *Workspace) ComputeVFSStats() *repb.VfsStats {
	if ws.vfsServer == nil {
		return nil
	}
	return ws.vfsServer.ComputeStats()
}

// VFSError returns the first input download error exposed to the current task,
// or nil if VFS is disabled or no download failed after retries.
func (ws *Workspace) VFSError() error {
	if ws.vfsServer == nil {
		return nil
	}
	return ws.vfsServer.TaskError()
}

// TaskFinished informs the workspace that task execution is done.
// Returns the transfer stats.
func (ws *Workspace) TaskFinished() (*dirtools.TransferInfo, error) {
	ws.mu.Lock()
	tf := ws.treeFetcher
	ws.mu.Unlock()
	if tf == nil {
		if ws.Opts.VFSPrefetchMode == VFSPrefetchModeNone {
			return &dirtools.TransferInfo{}, nil
		}
		return nil, status.FailedPreconditionError("tree fetcher not set")
	}
	// TODO(vadim): cancel unfinished transfers instead of waiting for them
	txInfo, err := tf.Wait()
	ws.mu.Lock()
	ws.treeFetcher = nil
	ws.mu.Unlock()
	return txInfo, err
}

// Clean removes files and directories in the workspace which are not preserved
// according to the workspace options.
func (ws *Workspace) Clean() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return WorkspaceMarkedForRemovalError
	}

	// No task is currently assigned; nothing to clean.
	if ws.task == nil {
		return nil
	}

	// If preserving the workspace, only remove outputs and leave other files
	// as-is.
	if ws.Opts.Preserve {
		cmd := ws.task.GetCommand()
		outputPaths := slices.Concat(cmd.GetOutputFiles(), cmd.GetOutputDirectories(), cmd.GetOutputPaths())
		workingDir := cmd.GetWorkingDirectory()
		for _, outputPath := range outputPaths {
			// Resolve output path relative to working directory, since
			// output paths in the command are relative to working_directory
			// but ws.Inputs and the filesystem are relative to the
			// workspace root.
			fullOutputPath := filepath.Join(workingDir, outputPath)
			if err := os.RemoveAll(filepath.Join(ws.Path(), fullOutputPath)); err != nil && !os.IsNotExist(err) {
				return status.UnavailableErrorf("Failed to clean workspace: %s", err)
			}
			// If the output path was a directory, we need to delete any known
			// input files which lived under that output directory.
			for inputKey := range ws.Inputs {
				if fspath.IsParent(fullOutputPath, inputKey.NormalizedString(), ws.Opts.CaseInsensitive) {
					delete(ws.Inputs, inputKey)
				}
			}
			// In case this output path previously pointed to an input file,
			// delete it from the inputs index (this should be pretty uncommon).
			delete(ws.Inputs, fspath.NewKey(fullOutputPath, ws.Opts.CaseInsensitive))
		}
		return nil
	}

	if err := removeChildren(ws.Path()); err != nil {
		return status.UnavailableErrorf("Failed to clean workspace: %s", err)
	}
	return nil
}

func removeChildren(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dirPath, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Pool holds the workspaces of finished tasks, so that a later task with
// a similar input tree can start from one of them. For the later task, the
// executor then only creates and removes the entries that differ, instead of
// hard-linking its whole input tree into an empty directory and deleting all
// of it afterwards, which costs a journaled metadata update per file.
//
// The runner calls ReturnToPool after removing the task's container.
// ReturnToPool removes everything from the workspace that isn't one of the
// task's unchanged inputs, then moves the workspace into the pool. Directories
// whose inode, ctime, mode and owner still match what we recorded after input
// setup can't have had entries added, removed or replaced, so we skip them. In
// the other directories, we compare each entry with the input tree and remove
// entries that aren't inputs, such as outputs, and input files that aren't the
// file cache's copy. Only the executor can reach pooled directories, so when
// we reuse a pooled workspace for a later task, we don't look at its files
// again, and only remove the inputs that the later task's tree doesn't have.
type Pool struct {
	dir  string
	size int

	mu sync.Mutex // protects all of the fields below
	// Pooled workspaces, stored using a GreedyDual-Size priority queue. This
	// models the value of keeping large workspaces around for longer than small
	// workspaces, while still allowing large workspaces to eventually get
	// evicted. LRU performs significantly worse because the number of pooled
	// workspaces is small.
	workspaces *gds.Queue[*pooledWorkspace]
}

// NewPool returns a pool that keeps workspaces under buildRoot, or nil if
// workspace reuse is disabled.
func NewPool(buildRoot string) (*Pool, error) {
	// Workspaces pooled by a previous executor process aren't tracked, so we
	// start with an empty pool, and delete the old one even if reuse is now
	// disabled.
	dir := filepath.Join(buildRoot, "_WORKSPACE_POOL")
	if err := disk.ForceRemove(context.Background(), dir); err != nil {
		return nil, fmt.Errorf("remove old workspace reuse pool: %w", err)
	}
	// Recording directory fingerprints (lstatDir) is only implemented on Linux.
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	size := *poolSlots
	if size == 0 {
		size = int(resources.GetAllocatedCPUMillis() / 1000)
	}
	if size <= 0 {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create workspace reuse pool: %w", err)
	}
	// Fingerprints compare ctimes, so they can't detect changes that a task
	// makes within the same second as input setup on filesystems that only
	// record whole seconds.
	fp, _, err := lstatDir(dir)
	if err != nil {
		return nil, fmt.Errorf("stat workspace reuse pool: %w", err)
	}
	if fp.ctimeNanos%int64(time.Second) == 0 {
		log.Warningf("Not pooling workspaces, because the build root's filesystem doesn't record sub-second timestamps")
		return nil, nil
	}
	return &Pool{dir: dir, size: size, workspaces: gds.New[*pooledWorkspace]()}, nil
}

// add adds a workspace to the pool, then evicts workspaces until the pool is
// within its size limit, which may evict the new one.
func (p *Pool) add(e *pooledWorkspace) {
	p.mu.Lock()
	// We value a workspace by its file count, since reusing it saves up to
	// that many links.
	p.workspaces.Add(e, float64(e.totalFiles))
	var evicted []*pooledWorkspace
	for p.workspaces.Len() > p.size {
		v, ok := p.workspaces.Evict()
		if !ok {
			break
		}
		evicted = append(evicted, v)
	}
	p.mu.Unlock()
	// deleteWaitGroup.Go blocks while too many deletions are running, so we
	// start them only after unlocking, to not hold up other tasks' take calls.
	for _, v := range evicted {
		deleteWaitGroup.Go(func() error {
			if err := disk.ForceRemove(context.Background(), v.path); err != nil {
				log.Warningf("Failed to remove evicted workspace %q: %s", v.path, err)
			}
			return nil
		})
	}
}

type pooledWorkspace struct {
	groupID        string
	path           string
	digestFunction repb.DigestFunction_Value
	rootDigest     *repb.Digest
	dirMap         map[string]*repb.Directory
	// The number of files in the input tree that pooled workspaces preserve.
	// It's counted from the input tree, so it includes files in missing.
	totalFiles int
	// The inputs that ReturnToPool found missing from the workspace. See
	// ReturnToPool.
	missing map[string]bool
}

// shouldRemoveBeforePooling returns whether we remove an input file from
// workspaces before pooling them, rather than keeping it for reuse. We remove
// files over the size limit, so that pooled workspaces don't hold large files
// after the file cache evicts them. We also remove empty files, since the tree
// fetcher writes them directly instead of linking them from the file cache, so
// tasks can write to them in place without our noticing.
func shouldRemoveBeforePooling(f *repb.FileNode) bool {
	size := f.GetDigest().GetSizeBytes()
	return size <= 0 || (*poolMaxFileSize > 0 && size > *poolMaxFileSize)
}

// countFilesRecursive returns the number of files in the input subtree rooted
// at d that pooled workspaces preserve, counting a repeated subdirectory once
// per place it appears. It counts from the input tree, so it includes inputs
// that ReturnToPool removed from a pooled workspace. memo holds the counts
// already computed, by directory hash.
func countFilesRecursive(d *repb.Digest, dirMap map[string]*repb.Directory, memo map[string]int) int {
	if n, ok := memo[d.GetHash()]; ok {
		return n
	}
	dir := dirMap[d.GetHash()]
	n := 0
	for _, f := range dir.GetFiles() {
		if !shouldRemoveBeforePooling(f) {
			n++
		}
	}
	for _, child := range dir.GetDirectories() {
		n += countFilesRecursive(child.GetDigest(), dirMap, memo)
	}
	memo[d.GetHash()] = n
	return n
}

// recordDirFingerprints walks the input tree and lstats each input directory in
// the workspace, recording its fingerprint so that ReturnToPool can tell which
// directories the task changed.
func (ws *Workspace) recordDirFingerprints() error {
	var dirs []string
	var walk func(rel string, d *repb.Digest)
	walk = func(rel string, d *repb.Digest) {
		dirs = append(dirs, rel)
		for _, child := range ws.inputDirs[d.GetHash()].GetDirectories() {
			walk(joinRelPath(rel, child.GetName()), child.GetDigest())
		}
	}
	walk("", ws.inputRootDigest)
	fingerprints := make(map[string]dirFingerprint, len(dirs))
	var latestCtime int64
	for rel, s := range ws.lstatDirs(dirs) {
		if s.err != nil {
			return s.err
		}
		if !s.isDir {
			return fmt.Errorf("input directory %q is not a directory", rel)
		}
		fingerprints[rel] = s.fp
		latestCtime = max(latestCtime, s.fp.ctimeNanos)
	}
	if err := waitForTimestampTick(latestCtime); err != nil {
		return err
	}
	ws.dirFingerprints = fingerprints
	return nil
}

// dirStat is the result of lstatting an input directory.
type dirStat struct {
	fp    dirFingerprint
	isDir bool
	err   error
}

// lstatDirs lstats the given workspace-relative directories in parallel, and
// returns the results by path. Both callers lstat every input directory, so
// we use up to 16 workers rather than a goroutine per directory, since
// starting a goroutine costs about as much as an lstat.
func (ws *Workspace) lstatDirs(dirs []string) map[string]dirStat {
	stats := make([]dirStat, len(dirs))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(16, len(dirs)) {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(dirs) {
					return
				}
				s := &stats[i]
				s.fp, s.isDir, s.err = lstatDir(filepath.Join(ws.inputRoot(), dirs[i]))
			}
		})
	}
	wg.Wait()
	byPath := make(map[string]dirStat, len(dirs))
	for i, rel := range dirs {
		byPath[rel] = stats[i]
	}
	return byPath
}

// joinRelPath joins a workspace-relative directory path and the name of an
// entry in it. It's path.Join without the cleaning, which we don't need since
// rel is already clean and names are single path components, and which is slow
// when done for every input file.
func joinRelPath(rel, name string) string {
	if rel == "" {
		return name
	}
	return rel + "/" + name
}

// ReturnToPool checks the workspace against the input tree that was set
// up in it, removes everything that isn't an unchanged input, and moves it
// into the reuse pool. It must be called after the container using the
// workspace has been removed, and before Remove, which then finds nothing to
// delete. If the workspace can't be pooled, it's left for Remove.
func (ws *Workspace) ReturnToPool(ctx context.Context) {
	pool := ws.Opts.Pool
	if pool == nil || ws.dirFingerprints == nil || ws.overlay != nil || ws.vfs != nil {
		return
	}
	ws.mu.Lock()
	removing := ws.removing
	ws.mu.Unlock()
	if removing {
		return
	}

	// Collect the current directory fingerprints, so we can diff against the
	// original fingerprint from before the task was started.
	current := ws.lstatDirs(slices.Collect(maps.Keys(ws.dirFingerprints)))

	// Recursively clean up any paths that differ from the original input root,
	// based on the fingerprints taken just after the task's inputs were
	// fetched.
	//
	// The resulting workspace tree should contain a pristine, unmodified subset
	// of the original input tree, as long as actions adhered to the contract of
	// not overwriting input file data. The `missing` map collects the relative
	// paths of inputs (files, symlinks or whole directories) that the workspace
	// no longer has after the cleanup, either because the task removed or
	// changed them, or because the cleanup removed a file that exceeds the size
	// limit.
	missing := map[string]bool{}
	rootIntact, err := ws.cleanReturnedDir(ctx, "", ws.inputRootDigest, current, missing)
	if err != nil {
		log.CtxWarningf(ctx, "Not pooling workspace: %s", err)
		return
	}

	// The task removed or replaced the input root itself, or changed its mode
	// or owner, so we can't keep anything in it. Remove deletes the workspace
	// instead.
	if !rootIntact {
		log.CtxWarningf(ctx, "Not pooling workspace, because the task removed or replaced its input root, or changed its mode or owner")
		return
	}

	// Move the directory into the pool. Note that this changes its ctime,
	// which is fine because we don't care about the fingerprints anymore at
	// this stage.
	pooledPath := filepath.Join(pool.dir, newRandomBuildDirCandidate())
	if err := os.Rename(ws.rootDir, pooledPath); err != nil {
		log.CtxWarningf(ctx, "Not pooling workspace: %s", err)
		return
	}
	pool.add(&pooledWorkspace{
		groupID:        ws.groupID,
		path:           pooledPath,
		digestFunction: ws.inputDigestFunction,
		rootDigest:     ws.inputRootDigest,
		dirMap:         ws.inputDirs,
		totalFiles:     countFilesRecursive(ws.inputRootDigest, ws.inputDirs, map[string]int{}),
		missing:        missing,
	})
}

// cleanReturnedDir makes the directory at rel hold only the entries of the
// input directory d that the task didn't change, removing everything else. It
// returns false if the task removed or replaced the directory itself, or
// changed its mode or owner, in which case the caller removes it. current holds
// the result of lstatting each input directory once the task finished, before
// the cleanup removed anything, and we add the paths of the inputs we remove to
// missing.
func (ws *Workspace) cleanReturnedDir(ctx context.Context, rel string, d *repb.Digest, current map[string]dirStat, missing map[string]bool) (bool, error) {
	full := filepath.Join(ws.inputRoot(), rel)
	s := current[rel]
	if os.IsNotExist(s.err) {
		return false, nil
	}
	if s.err != nil {
		return false, s.err
	}
	fp := s.fp
	want, ok := ws.dirFingerprints[rel]
	if !ok || !s.isDir || fp.ino != want.ino || fp.mode != want.mode || fp.uid != want.uid || fp.gid != want.gid {
		return false, nil
	}
	dir := ws.inputDirs[d.GetHash()]
	// A directory with an unchanged fingerprint holds the same entries as
	// after input setup, which are just the inputs, so we only read
	// directories that changed.
	if fp != want {
		if err := ws.cleanReturnedEntries(ctx, rel, dir, missing); err != nil {
			return false, err
		}
	}
	for _, f := range dir.GetFiles() {
		if !shouldRemoveBeforePooling(f) {
			continue
		}
		fileRel := joinRelPath(rel, f.GetName())
		if missing[fileRel] {
			continue
		}
		if err := os.Remove(filepath.Join(full, f.GetName())); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		missing[fileRel] = true
	}
	for _, child := range dir.GetDirectories() {
		childRel := joinRelPath(rel, child.GetName())
		if missing[childRel] {
			continue
		}
		childIntact, err := ws.cleanReturnedDir(ctx, childRel, child.GetDigest(), current, missing)
		if err != nil {
			return false, err
		}
		if !childIntact {
			missing[childRel] = true
			if err := disk.ForceRemove(ctx, filepath.Join(full, child.GetName())); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// cleanReturnedEntries removes the entries of the directory at rel that aren't
// in the input directory dir, such as outputs, and the inputs in it that the
// task replaced, and adds the paths of those inputs to missing. Subdirectories
// are left to cleanReturnedDir.
func (ws *Workspace) cleanReturnedEntries(ctx context.Context, rel string, dir *repb.Directory, missing map[string]bool) error {
	full := filepath.Join(ws.inputRoot(), rel)
	entries, err := os.ReadDir(full)
	if err != nil {
		return err
	}
	files := make(map[string]*repb.FileNode, len(dir.GetFiles()))
	for _, f := range dir.GetFiles() {
		files[f.GetName()] = f
	}
	symlinks := make(map[string]string, len(dir.GetSymlinks()))
	for _, s := range dir.GetSymlinks() {
		// The tree fetcher creates empty symlink targets as ".".
		target := s.GetTarget()
		if target == "" {
			target = "."
		}
		symlinks[s.GetName()] = target
	}
	children := make(map[string]*repb.Digest, len(dir.GetDirectories()))
	for _, child := range dir.GetDirectories() {
		children[child.GetName()] = child.GetDigest()
	}
	present := make(map[string]bool, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		entryRel := joinRelPath(rel, name)
		entryFull := filepath.Join(full, name)
		present[name] = true
		target, isSymlink := symlinks[name]
		replaced := false
		switch {
		case children[name] != nil:
			replaced = !entry.IsDir()
		case files[name] != nil:
			replaced = !entry.Type().IsRegular() || !ws.isCachedCopy(ctx, entryFull, files[name])
		case isSymlink:
			if entry.Type()&fs.ModeSymlink == 0 {
				replaced = true
			} else {
				got, err := os.Readlink(entryFull)
				replaced = err != nil || got != target
			}
		default:
			// Not an input, such as an output.
			if err := disk.ForceRemove(ctx, entryFull); err != nil {
				return err
			}
			continue
		}
		if !replaced {
			continue
		}
		missing[entryRel] = true
		if err := disk.ForceRemove(ctx, entryFull); err != nil {
			return err
		}
	}
	for name := range files {
		if !present[name] {
			missing[joinRelPath(rel, name)] = true
		}
	}
	for name := range symlinks {
		if !present[name] {
			missing[joinRelPath(rel, name)] = true
		}
	}
	for name := range children {
		if !present[name] {
			missing[joinRelPath(rel, name)] = true
		}
	}
	return nil
}

// isCachedCopy returns whether the file at fullPath is the file cache's copy
// of node. Input setup hard-links each input file to the file cache's copy, so
// an input that's no longer the file cache's copy is one that the task
// replaced. We only check files in the directories that the task changed,
// which are usually few. Recording each input file's inode during input setup
// instead would take an lstat of every input file before the task starts.
func (ws *Workspace) isCachedCopy(ctx context.Context, fullPath string, node *repb.FileNode) bool {
	fc := ws.env.GetFileCache()
	if fc == nil {
		return false
	}
	cached, err := fc.Stat(ctx, node)
	if err != nil {
		return false
	}
	existing, err := os.Lstat(fullPath)
	if err != nil {
		return false
	}
	return os.SameFile(cached, existing)
}

// reusePooledWorkspace replaces this workspace's directory, which is still
// empty, with the pooled workspace that best matches the input tree, and
// removes every entry in it that the input tree doesn't have. It returns the
// input files already in place, which the tree fetcher can skip, or nil,
// leaving the workspace empty, if no pooled workspace is worth reusing. It
// returns an error if it can't get back to an empty workspace after a failed
// reuse.
func (ws *Workspace) reusePooledWorkspace(ctx context.Context) (map[fspath.Key]*repb.FileNode, error) {
	// TODO: Take the pooled workspace that best matches the input tree, and
	// remove the entries that the input tree doesn't have.
	return nil, nil
}
