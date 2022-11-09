package workspace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
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
		return nil, status.UnavailableErrorf("failed to create workspace at %q", rootDir)
	}

	return &Workspace{
		env:      env,
		rootDir:  rootDir,
		dirPerms: dirPerms,
		Opts:     opts,
		Inputs:   map[string]*repb.FileNode{},
	}, nil
}

// Path returns the absolute path to the workspace root directory.
func (ws *Workspace) Path() string {
	return ws.rootDir
}

// SetTask sets the next task to be executed within the workspace.
func (ws *Workspace) SetTask(ctx context.Context, task *repb.ExecutionTask) {
	log.CtxDebugf(ctx, "Assigned task %s to workspace at %q", task.GetExecutionId(), ws.rootDir)
	ws.task = task
	cmd := task.GetCommand()
	outputPaths := cmd.GetOutputPaths()
	if len(outputPaths) == 0 {
		outputPaths = append(cmd.GetOutputFiles(), cmd.GetOutputDirectories()...)
	}
	ws.dirHelper = dirtools.NewDirHelper(ws.Path(), outputPaths, ws.dirPerms)
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
	txInfo, err := dirtools.DownloadTree(ctx, ws.env, ws.task.GetExecuteRequest().GetInstanceName(), tree, ws.rootDir, opts)
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
	destPath := path.Join(ws.Path(), "buildbuddy_ci_runner")
	exists, err := disk.FileExists(ctx, destPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	// TODO(bduffany): Consider doing a fastcopy here instead of a normal copy.
	// The CI runner binary may be on a different device than the runner workspace
	// so we'd have to put it somewhere on the same device before fastcopying.
	srcFile, err := ws.env.GetFileResolver().Open("enterprise/server/cmd/ci_runner/buildbuddy_ci_runner")
	if err != nil {
		return err
	}
	defer srcFile.Close()
	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0555)
	if err != nil {
		return err
	}
	defer destFile.Close()
	_, err = io.Copy(destFile, srcFile)
	return err
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
	for path, _ := range keep {
		delete(inputFilesToCleanUp, path)
	}
	if len(inputFilesToCleanUp) > 0 {
		for path, _ := range inputFilesToCleanUp {
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
func (ws *Workspace) UploadOutputs(ctx context.Context, actionResult *repb.ActionResult, cmdResult *interfaces.CommandResult) (*dirtools.TransferInfo, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removing {
		return nil, WorkspaceMarkedForRemovalError
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	bsClient := ws.env.GetByteStreamClient()
	instanceName := ws.task.GetExecuteRequest().GetInstanceName()

	var txInfo *dirtools.TransferInfo
	var stdoutDigest, stderrDigest *repb.Digest

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// Errors uploading stderr/stdout are swallowed.
		var err error
		stdoutDigest, err = cachetools.UploadBlob(egCtx, bsClient, instanceName, bytes.NewReader(cmdResult.Stdout))
		if err != nil {
			log.CtxWarningf(ctx, "Failed to upload stdout: %s", err)
		}
		return nil
	})
	eg.Go(func() error {
		// Errors uploading stderr/stdout are swallowed.
		var err error
		stderrDigest, err = cachetools.UploadBlob(egCtx, bsClient, instanceName, bytes.NewReader(cmdResult.Stderr))
		if err != nil {
			log.CtxWarningf(ctx, "Failed to upload stderr: %s", err)
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		txInfo, err = dirtools.UploadTree(egCtx, ws.env, ws.dirHelper, instanceName, ws.Path(), actionResult)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	actionResult.StdoutDigest = stdoutDigest
	actionResult.StderrDigest = stderrDigest
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
			for inputPath, _ := range ws.Inputs {
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
