package workspace

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	_ "embed"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/overlayfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_util"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/fspath"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/gobwas/glob"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	ci_runner_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
}

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
}

// New creates a new workspace directly under the given parent directory.
func New(env environment.Env, parentDir string, opts *Opts) (*Workspace, error) {
	dirPerms := fs.FileMode(0777)
	var rootDir string
	maxAttempts := 10
	for i := 0; i < maxAttempts; i++ {
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
func (ws *Workspace) CreateOutputDirs() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return WorkspaceMarkedForRemovalError
	}

	return ws.dirHelper.CreateOutputDirs()
}

func (ws *Workspace) prepareVFS(ctx context.Context, layout *container.FileSystemLayout) error {
	if ws.vfs == nil {
		return status.FailedPreconditionError("vfs cannot be null if vfsServer is set")
	}

	invalidatedInodes, err := ws.vfsServer.Prepare(ctx, layout, ws.treeFetcher)
	if err != nil {
		return err
	}
	if err := ws.vfs.PrepareForTask(ctx, ws.task.GetExecutionId(), invalidatedInodes); err != nil {
		return err
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

	opts := &dirtools.DownloadTreeOpts{CaseInsensitive: ws.Opts.CaseInsensitive}
	if ws.vfs == nil {
		opts.RootDir = ws.inputRoot()
	}
	if ws.Opts.Preserve {
		opts.Skip = ws.Inputs
		opts.TrackTransfers = true
	}
	execReq := ws.task.GetExecuteRequest()
	tf, err := dirtools.NewTreeFetcher(ctx, ws.env, execReq.GetInstanceName(), execReq.GetDigestFunction(), layout.Inputs, opts)
	if err != nil {
		return status.WrapErrorf(err, "could not create tree fetcher")
	}
	ws.treeFetcher = tf

	// Start fetching inputs.
	inputsState, err := tf.Start()
	if err != nil {
		return status.WrapErrorf(err, "could not start tree fetcher")
	}

	// Inform VFS about the layout of the input tree and give it access to the
	// running tree fetcher.
	if ws.vfs != nil {
		if err := ws.prepareVFS(ctx, layout); err != nil {
			return err
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
		log.CtxInfof(ctx, "DownloadTree linked %d files in %s, downloaded %d bytes in %s [%2.2f MB/sec]", txInfo.LinkCount, txInfo.LinkDuration, txInfo.BytesTransferred, txInfo.TransferDuration, mbps)
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

// AddCIRunner adds the BuildBuddy CI runner to the workspace root if it doesn't
// already exist.
func (ws *Workspace) AddCIRunner(ctx context.Context) error {
	// Don't add CI runner if the workspace is backed by FUSE.
	if ws.vfs != nil {
		return status.UnimplementedErrorf("AddCIRunner not support on VFS")
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
	var stdoutDigest, stderrDigest *repb.Digest

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		d, err := cachetools.UploadBlob(egCtx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.Stdout))
		if err != nil {
			return status.UnavailableErrorf("upload stdout: %s", err)
		}
		stdoutDigest = d
		return nil
	})
	eg.Go(func() error {
		d, err := cachetools.UploadBlob(egCtx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.Stderr))
		if err != nil {
			return status.UnavailableErrorf("upload stderr: %s", err)
		}
		stderrDigest = d
		return nil
	})
	eg.Go(func() error {
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
			if err := ws.overlay.Apply(egCtx, opts); err != nil {
				return status.WrapError(err, "apply overlay upperdir changes")
			}
		}
		// Don't try to add files to the filecache if VFS is enabled. We use hard links to store data in the file
		// cache and hard links across filesystems are not possible.
		addToFileCache := ws.vfs == nil
		var err error
		txInfo, err = dirtools.UploadTree(egCtx, ws.env, ws.dirHelper, instanceName, digestFunction, filepath.Join(ws.inputRoot(), cmd.GetWorkingDirectory()), cmd, executeResponse.Result, addToFileCache)
		return err
	})
	var logsMu sync.Mutex
	serverLogs := make(map[string]*repb.LogFile, len(cmdResult.AuxiliaryLogs))
	for name, b := range cmdResult.AuxiliaryLogs {
		name, b := name, b
		eg.Go(func() error {
			d, err := cachetools.UploadBlob(egCtx, bsClient, instanceName, digestFunction, bytes.NewReader(b))
			if err != nil {
				log.CtxWarningf(ctx, "Failed to upload auxiliary log %q: %s", name, err)
				return nil
			}
			logsMu.Lock()
			defer logsMu.Unlock()
			serverLogs[name] = &repb.LogFile{
				Digest:        d,
				HumanReadable: true,
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	txInfo.FileCount += 2 // for stdout and stderr
	txInfo.BytesTransferred += int64(len(cmdResult.Stdout) + len(cmdResult.Stderr))
	txInfo.FileCount += int64(len(cmdResult.AuxiliaryLogs))
	for _, b := range cmdResult.AuxiliaryLogs {
		txInfo.BytesTransferred += int64(len(b))
	}
	executeResponse.Result.StdoutDigest = stdoutDigest
	executeResponse.Result.StderrDigest = stderrDigest
	executeResponse.ServerLogs = serverLogs
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

// TaskFinished informs the workspace that task execution is done.
// Returns the transfer stats.
func (ws *Workspace) TaskFinished() (*dirtools.TransferInfo, error) {
	ws.mu.Lock()
	tf := ws.treeFetcher
	ws.mu.Unlock()
	if tf == nil {
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
			if err := os.RemoveAll(filepath.Join(ws.Path(), workingDir, outputPath)); err != nil && !os.IsNotExist(err) {
				return status.UnavailableErrorf("Failed to clean workspace: %s", err)
			}
			// If the output path was a directory, we need to delete any known
			// input files which lived under that output directory.
			for inputKey := range ws.Inputs {
				if fspath.IsParent(outputPath, inputKey.NormalizedString(), ws.Opts.CaseInsensitive) {
					delete(ws.Inputs, inputKey)
				}
			}
			// In case this output path previously pointed to an input file,
			// delete it from the inputs index (this should be pretty uncommon).
			delete(ws.Inputs, fspath.NewKey(outputPath, ws.Opts.CaseInsensitive))
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
