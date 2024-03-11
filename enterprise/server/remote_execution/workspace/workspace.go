package workspace

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	_ "embed"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/overlayfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/gobwas/glob"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	ci_runner_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle"
	gh_actions_runner_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/github_actions_runner/bundle"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// WorkspaceMarkedForRemovalError is returned from workspace operations
	// whenever Remove was previously called on the workspace.
	WorkspaceMarkedForRemovalError = status.UnavailableError("workspace is marked for removal")
)

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
	vfs       *vfs.VFS
	// Action input files known to exist in the workspace, as a map of
	// workspace-relative paths to file nodes.
	// TODO: Make sure these files are written read-only
	// to make sure this map accurately reflects the filesystem.
	Inputs map[string]*repb.FileNode

	mu       sync.Mutex // protects(removing)
	removing bool
}

type Opts struct {
	// NonrootWritable specifies whether the workspace dir, as well as directories
	// created under it, should be writable by nonroot users.
	NonrootWritable bool
	// Preserve specifies whether to preserve all files in the workspace except
	// for output dirs.
	Preserve    bool
	CleanInputs string
	// UseOverlayfs specifies whether the workspace should use overlayfs to
	// allow copy-on-write for workspace inputs.
	UseOverlayfs bool
}

// New creates a new workspace directly under the given parent directory.
func New(env environment.Env, parentDir string, opts *Opts) (*Workspace, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, status.UnavailableErrorf("failed to generate workspace ID")
	}
	rootDir := filepath.Join(parentDir, id.String())
	dirPerms := fs.FileMode(0755)
	if opts.NonrootWritable {
		dirPerms = 0777
	}
	if err := os.MkdirAll(rootDir, dirPerms); err != nil {
		return nil, status.UnavailableErrorf("failed to create workspace at %q: %s", rootDir, err)
	}

	var overlay *overlayfs.Overlay
	if opts.UseOverlayfs {
		overlayOpts := overlayfs.Opts{DirPerms: dirPerms}
		overlay, err = overlayfs.Convert(context.TODO(), rootDir, overlayOpts)
		if err != nil {
			return nil, status.UnavailableErrorf("failed to create workspace overlayfs at %q: %s", rootDir, err)
		}
	}

	return &Workspace{
		env:      env,
		rootDir:  rootDir,
		overlay:  overlay,
		dirPerms: dirPerms,
		Opts:     opts,
		Inputs:   map[string]*repb.FileNode{},
	}, nil
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
	outputDirs := make([]string, 0)
	outputPaths := cmd.GetOutputPaths()
	if len(outputPaths) == 0 {
		outputDirs = cmd.GetOutputDirectories()
		outputPaths = append(cmd.GetOutputFiles(), cmd.GetOutputDirectories()...)
	}
	ws.dirHelper = dirtools.NewDirHelper(ws.inputRoot(), outputDirs, outputPaths, ws.dirPerms)
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

// DownloadInputs downloads any missing inputs for the current action.
func (ws *Workspace) DownloadInputs(ctx context.Context, tree *repb.Tree) (*dirtools.TransferInfo, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return nil, WorkspaceMarkedForRemovalError
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	opts := &dirtools.DownloadTreeOpts{
		NonrootWritable: ws.Opts.NonrootWritable,
	}
	if ws.Opts.Preserve {
		opts.Skip = ws.Inputs
		opts.TrackTransfers = true
	}
	execReq := ws.task.GetExecuteRequest()
	txInfo, err := dirtools.DownloadTree(ctx, ws.env, execReq.GetInstanceName(), execReq.GetDigestFunction(), tree, ws.inputRoot(), opts)
	if err == nil {
		if err := ws.CleanInputsIfNecessary(txInfo.Exists); err != nil {
			return txInfo, err
		}

		for path, node := range txInfo.Transfers {
			ws.Inputs[path] = node
		}
		mbps := (float64(txInfo.BytesTransferred) / float64(1e6)) / float64(txInfo.TransferDuration.Seconds())
		span.SetAttributes(attribute.Int64("file_count", txInfo.FileCount))
		span.SetAttributes(attribute.Int64("bytes_transferred", txInfo.BytesTransferred))
		log.CtxDebugf(ctx, "GetTree downloaded %d bytes in %s [%2.2f MB/sec]", txInfo.BytesTransferred, txInfo.TransferDuration, mbps)
	}
	return txInfo, err
}

// AddCIRunner adds the BuildBuddy CI runner to the workspace root if it doesn't
// already exist.
func (ws *Workspace) AddCIRunner(ctx context.Context) error {
	destPath := path.Join(ws.Path(), ci_runner_bundle.RunnerName)
	exists, err := disk.FileExists(ctx, destPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return os.WriteFile(destPath, ci_runner_bundle.CiRunnerBytes, 0o555)
}

func (ws *Workspace) AddActionsRunner(ctx context.Context) error {
	return os.WriteFile(filepath.Join(ws.Path(), "buildbuddy_github_actions_runner"), gh_actions_runner_bundle.RunnerBytes, 0555)
}

func (ws *Workspace) CleanInputsIfNecessary(keep map[string]*repb.FileNode) error {
	if ws.Opts.CleanInputs == "" {
		return nil
	}
	inputFilesToCleanUp := make(map[string]*repb.FileNode)
	// Curly braces indicate a comma separated list of patterns: https://pkg.go.dev/github.com/gobwas/glob#Compile
	glob, err := glob.Compile(fmt.Sprintf("{%s}", ws.Opts.CleanInputs), os.PathSeparator)
	if err != nil {
		return status.FailedPreconditionErrorf("Invalid glob {%s} used for input cleaning: %s", ws.Opts.CleanInputs, err.Error())
	}
	for path, node := range ws.Inputs {
		if ws.Opts.CleanInputs == "*" || glob.Match(path) {
			inputFilesToCleanUp[path] = node
		}
	}
	for path := range keep {
		delete(inputFilesToCleanUp, path)
	}
	if len(inputFilesToCleanUp) > 0 {
		for path := range inputFilesToCleanUp {
			if err := os.RemoveAll(filepath.Join(ws.Path(), path)); err != nil && !os.IsNotExist(err) {
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
		// Errors uploading stderr/stdout are swallowed.
		var err error
		stdoutDigest, err = cachetools.UploadBlob(egCtx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.Stdout))
		if err != nil {
			log.CtxWarningf(ctx, "Failed to upload stdout: %s", err)
		}
		return nil
	})
	eg.Go(func() error {
		// Errors uploading stderr/stdout are swallowed.
		var err error
		stderrDigest, err = cachetools.UploadBlob(egCtx, bsClient, instanceName, digestFunction, bytes.NewReader(cmdResult.Stderr))
		if err != nil {
			log.CtxWarningf(ctx, "Failed to upload stderr: %s", err)
		}
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
			recyclingEnabled := platform.IsTrue(platform.FindValue(ws.task.GetCommand().GetPlatform(), platform.RecycleRunnerPropertyName))
			opts := overlayfs.ApplyOpts{AllowRename: !recyclingEnabled}
			if err := ws.overlay.Apply(egCtx, opts); err != nil {
				return status.WrapError(err, "apply overlay upperdir changes")
			}
		}
		var err error
		txInfo, err = dirtools.UploadTree(egCtx, ws.env, ws.dirHelper, instanceName, digestFunction, ws.inputRoot(), cmd, executeResponse.Result)
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
	executeResponse.Result.StdoutDigest = stdoutDigest
	executeResponse.Result.StderrDigest = stderrDigest
	executeResponse.ServerLogs = serverLogs
	span.SetAttributes(attribute.Int64("file_count", txInfo.FileCount))
	span.SetAttributes(attribute.Int64("bytes_transferred", txInfo.BytesTransferred))
	return txInfo, nil
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

	// Sometimes removal fails if badly-behaved actions write their
	// directories read-only. Use force-removal to handle these cases.
	if err := disk.ForceRemove(ctx, ws.rootDir); err != nil {
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
		for _, path := range cmd.GetOutputFiles() {
			if err := os.RemoveAll(filepath.Join(ws.Path(), path)); err != nil && !os.IsNotExist(err) {
				return status.UnavailableErrorf("Failed to clean workspace: %s", err)
			}
			// In case this output path was specified as an input path previously,
			// delete it from known files.
			delete(ws.Inputs, path)
			// TODO: If we remove an output file whose path previously pointed to
			// a directory, then we need to remove all `inputs` under that directory.
		}
		for _, outputDirPath := range cmd.GetOutputDirectories() {
			if err := os.RemoveAll(filepath.Join(ws.Path(), outputDirPath)); err != nil && !os.IsNotExist(err) {
				return status.UnavailableErrorf("Failed to clean workspace: %s", err)
			}
			// Need to delete any known input files which lived under that
			// output directory.
			// TODO: This nested loop impl may slow down the action if there are a lot
			// of output directories. If this turns out to be an issue, might need to
			// optimize this further.
			for inputPath := range ws.Inputs {
				if isParent(outputDirPath, inputPath) {
					delete(ws.Inputs, inputPath)
				}
			}
			// In case this output dir previously pointed to an input file, delete it
			// from the inputs index (this should be pretty uncommon).
			delete(ws.Inputs, outputDirPath)
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

func isParent(parent, child string) bool {
	return strings.HasPrefix(child, parent+string(os.PathSeparator))
}
