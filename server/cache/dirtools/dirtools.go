package dirtools

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/fspath"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/durationpb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	enableDownloadCompresssion = flag.Bool("cache.client.enable_download_compression", true, "If true, enable compression of downloads from remote caches")
	linkParallelism            = flag.Int("cache.client.filecache_link_parallelism", 0, "Number of goroutines to use when linking inputs from filecache. If 0 uses the value of GOMAXPROCS.")
	inputTreeSetupParallelism  = flag.Int("cache.client.input_tree_setup_parallelism", -1, "Maximum number of concurrent filesystem operations to perform across all tasks when setting up the input tree structure. -1 means no limit.")

	initInputTreeWrangler     sync.Once
	inputTreeWranglerInstance *inputTreeWrangler
)

const (
	// Size of the queue for input tree structure manipulation ops.
	// Should be big enough to feed the workers when they free up.
	// Operations will block when the queue is full.
	inputTreeOpsQueueSize = 4000
)

func groupIDStringFromContext(ctx context.Context) string {
	if c, err := claims.ClaimsFromContext(ctx); err == nil {
		return c.GroupID
	}
	return interfaces.AuthAnonymousUser
}

// downloadDedupeKey controls the granularity at which we dedupe fetches.
// The granularity is driven by file cache properties.
// File cache contents is isolated by group, digest and executable attribute.
type downloadDedupeKey struct {
	groupID  string
	fetchKey fetchKey
}

var DownloadDeduper = singleflight.Group[downloadDedupeKey, *FilePointer]{}

type TransferInfo struct {
	FileCount        int64
	BytesTransferred int64
	TransferDuration time.Duration
	// Transfers tracks the files that were transferred, keyed by their
	// workspace-relative paths.
	Transfers map[string]*repb.FileNode
	// Exists tracks the files that already existed, keyed by their
	// workspace-relative paths.
	Exists map[fspath.Key]*repb.FileNode

	LinkCount    int64
	LinkDuration time.Duration
}

// DirHelper is a poor mans trie that helps us check if a partial path like
// /foo/bar matches one of a set of paths like /foo/bar/baz/bap and /a/b/c/d.
// Really should use a trie but this will do for now.
type DirHelper struct {
	// The directory under which all paths should descend.
	rootDir string

	// Prefixes of any output path -- used to truncate tree traversal if we're
	// uploading non-requested files.
	prefixes map[string]struct{}

	// The full output paths themselves -- used when creating the ActionResult
	// OutputFiles, OutputDirectories, OutputPaths, etc.
	fullPaths map[string]struct{}

	// The directories that should be created before running any commands.
	// (OutputDirectories and parent directories of OutputFiles and OutputPaths)
	dirsToCreate []string

	// The output paths that the client requested. Stored as a map for fast
	// lookup.
	outputPaths map[string]struct{}

	// dirPerms are the permissions used when creating output directories.
	dirPerms fs.FileMode
}

func NewDirHelper(rootDir string, cmd *repb.Command, dirPerms fs.FileMode) *DirHelper {
	// Convert the older output_directories and output_files fields to
	// output_paths, if applicable.
	outputPaths := cmd.GetOutputPaths()
	if len(outputPaths) == 0 {
		outputPaths = append(cmd.GetOutputFiles(), cmd.GetOutputDirectories()...)
	}

	c := &DirHelper{
		rootDir:      rootDir,
		prefixes:     make(map[string]struct{}, 0),
		fullPaths:    make(map[string]struct{}, 0),
		dirsToCreate: make([]string, 0),
		outputPaths:  make(map[string]struct{}, 0),
		dirPerms:     dirPerms,
	}

	// Per the API documentation, create the parent dir of each output path. We
	// are not responsible for creating the output path itself, since we don't
	// know yet whether it's a file or a directory. Instead, the action is
	// responsible for creating these paths.
	for _, outputPath := range outputPaths {
		fullPath := filepath.Join(c.rootDir, outputPath)
		c.fullPaths[fullPath] = struct{}{}
		c.dirsToCreate = append(c.dirsToCreate, filepath.Dir(fullPath))
		c.outputPaths[fullPath] = struct{}{}
		c.prefixes[fullPath] = struct{}{}
	}

	// This is a hack to workaround some misbehaving Bazel rules. The API says
	// that clients shouldn't rely on the executor to create each of the output
	// paths, but it will create their parents. Some clients use the older
	// output_files and output_directories and expect the executor to create all
	// of the output_directories, so go ahead and do that. The directories are
	// also present in output_paths, so they're forgotten after creation here
	// and then selected for upload through the output_paths.
	//
	// As of Bazel 6.0.0, the output directories are included as part of the
	// InputRoot, so this hack is only needed for older clients.
	// See: https://github.com/bazelbuild/bazel/pull/15366 for more info.
	for _, outputDirectory := range cmd.GetOutputDirectories() {
		fullPath := filepath.Join(c.rootDir, outputDirectory)
		c.dirsToCreate = append(c.dirsToCreate, fullPath)
	}

	for _, dir := range c.dirsToCreate {
		for p := dir; p != filepath.Dir(p); p = filepath.Dir(p) {
			c.prefixes[p] = struct{}{}
		}
	}

	return c
}

func (c *DirHelper) CreateOutputDirs() error {
	for _, dir := range c.dirsToCreate {
		if err := os.MkdirAll(dir, c.dirPerms); err != nil {
			return err
		}
	}
	return nil
}

func (c *DirHelper) IsOutputPath(path string) bool {
	_, ok := c.outputPaths[path]
	return ok
}

// If the provided path is a descendant of any of the output_paths, this
// function returns which output_path it is a descendent of and true, otherwise
// it returns an unspecified string and false.
func (c *DirHelper) FindParentOutputPath(path string) (string, bool) {
	for p := filepath.Dir(path); p != filepath.Dir(p); p = filepath.Dir(p) {
		if c.IsOutputPath(p) {
			return p, true
		}
	}
	return "", false
}

func (c *DirHelper) ShouldUploadAnythingInDir(path string) bool {
	// Upload something in this directory if:
	// 1. it is in output_paths
	// 2. any of its children are in output_paths
	// 3. it is a child of anything in output_paths
	if c.IsOutputPath(path) {
		return true
	}
	_, ok := c.prefixes[path]
	if ok {
		return true
	}
	return c.ShouldUploadFile(path)
}

func (c *DirHelper) ShouldUploadFile(path string) bool {
	// Upload the file if it is a child of anything in output_paths
	for p := path; p != filepath.Dir(p); p = filepath.Dir(p) {
		if c.IsOutputPath(p) {
			return true
		}
	}
	return false
}

func trimPathPrefix(fullPath, prefix string) string {
	r := strings.TrimPrefix(fullPath, prefix)
	if len(r) > 0 && r[0] == os.PathSeparator {
		r = r[1:]
	}
	return r
}

// dirToUpload represents a directory to be uploaded to cache.
type dirToUpload struct {
	info     os.FileInfo
	fullPath string

	directory       *repb.Directory
	directoryBytes  []byte
	directoryDigest *repb.Digest
}

func newDirToUpload(digestFunction repb.DigestFunction_Value, fullPath string, info os.FileInfo, dir *repb.Directory) (*dirToUpload, error) {
	data, err := proto.Marshal(dir)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)

	d, err := digest.Compute(reader, digestFunction)
	if err != nil {
		return nil, err
	}
	return &dirToUpload{
		info:            info,
		fullPath:        fullPath,
		directory:       dir,
		directoryBytes:  data,
		directoryDigest: d,
	}, nil
}

func (d *dirToUpload) DirectoryNode() *repb.DirectoryNode {
	return &repb.DirectoryNode{
		Name:   d.info.Name(),
		Digest: d.directoryDigest,
	}
}

// fileToUpload represents a regular file to be uploaded to cache.
type fileToUpload struct {
	fullPath string
	digest   *repb.Digest
	info     os.FileInfo
}

func newFileToUpload(digestFunction repb.DigestFunction_Value, fullPath string, info os.FileInfo) (*fileToUpload, error) {
	d, err := digest.ComputeForFile(fullPath, digestFunction)
	if err != nil {
		return nil, err
	}
	return &fileToUpload{
		digest:   d,
		fullPath: fullPath,
		info:     info,
	}, nil
}

func (f *fileToUpload) OutputFile(rootDir string) *repb.OutputFile {
	return &repb.OutputFile{
		Path:         filepath.ToSlash(trimPathPrefix(f.fullPath, rootDir)),
		Digest:       f.digest,
		IsExecutable: f.info.Mode()&0111 != 0,
	}
}

func (f *fileToUpload) FileNode() *repb.FileNode {
	return &repb.FileNode{
		Name:         f.info.Name(),
		Digest:       f.digest,
		IsExecutable: f.info.Mode()&0111 != 0,
	}
}

func uploadMissingFiles(ctx context.Context, uploader *cachetools.BatchCASUploader, env environment.Env, filesToUpload []*fileToUpload, instanceName string, digestFunction repb.DigestFunction_Value, addToFileCache bool) (alreadyPresentBytes int64, _ error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type batchResult struct {
		files        []*fileToUpload
		presentBytes int64
	}
	batches := make(chan batchResult, 1)
	var wg sync.WaitGroup
	cas := env.GetContentAddressableStorageClient()

	for batch := range slices.Chunk(filesToUpload, 1000) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &repb.FindMissingBlobsRequest{
				DigestFunction: digestFunction,
				InstanceName:   instanceName,
			}
			for _, f := range batch {
				req.BlobDigests = append(req.BlobDigests, f.digest)
			}
			var presentBytes int64
			resp, err := cas.FindMissingBlobs(ctx, req)
			if err != nil {
				log.CtxWarningf(ctx, "Failed to find missing output blobs: %s", err)
			} else {
				missing := make(map[string]struct{}, len(resp.GetMissingBlobDigests()))
				for _, d := range resp.GetMissingBlobDigests() {
					missing[d.GetHash()] = struct{}{}
				}
				missingLen := 0
				for _, uploadableFile := range batch {
					if _, ok := missing[uploadableFile.digest.GetHash()]; ok {
						batch[missingLen] = uploadableFile
						missingLen++
					} else {
						presentBytes += uploadableFile.digest.GetSizeBytes()
					}
				}
				batch = batch[:missingLen]
			}
			select {
			case <-ctx.Done():
				// If the reader errored and returned, don't block forever
			case batches <- batchResult{files: batch, presentBytes: presentBytes}:
			}
		}()
	}

	go func() {
		wg.Wait()
		close(batches)
	}()

	fc := env.GetFileCache()
	for batch := range batches {
		alreadyPresentBytes += batch.presentBytes
		if err := uploadFiles(ctx, uploader, fc, batch.files, addToFileCache); err != nil {
			return 0, err
		}
	}
	return alreadyPresentBytes, nil
}

func uploadFiles(ctx context.Context, uploader *cachetools.BatchCASUploader, fc interfaces.FileCache, filesToUpload []*fileToUpload, addToFileCache bool) error {
	for _, uploadableFile := range filesToUpload {
		// Add output files to the filecache.
		if fc != nil && addToFileCache {
			node := uploadableFile.FileNode()
			if err := fc.AddFile(ctx, node, uploadableFile.fullPath); err != nil {
				log.Warningf("Error adding file to filecache: %s", err)
			}
		}
		f, err := os.Open(uploadableFile.fullPath)
		if err != nil {
			return status.UnavailableErrorf("open output file: %s", err)
		}
		// Note: uploader.Upload closes the file after it is uploaded.
		if err := uploader.Upload(uploadableFile.digest, f); err != nil {
			return err
		}
	}
	return nil
}

// handleSymlink adds the symlink to the directory and actionResult proto so that
// they could be recreated on the Bazel client side if needed.
func handleSymlink(dirHelper *DirHelper, rootDir string, cmd *repb.Command, actionResult *repb.ActionResult, directory *repb.Directory, fqfn string) error {
	target, err := os.Readlink(fqfn)
	if err != nil {
		return err
	}
	symlink := &repb.OutputSymlink{
		Path:   filepath.ToSlash(trimPathPrefix(fqfn, rootDir)),
		Target: filepath.ToSlash(target),
	}
	if !dirHelper.ShouldUploadFile(fqfn) {
		return nil
	}
	directory.Symlinks = append(directory.Symlinks, &repb.SymlinkNode{
		Name:   filepath.ToSlash(filepath.Base(symlink.Path)),
		Target: filepath.ToSlash(symlink.Target),
	})

	// REAPI specification:
	//   `output_symlinks` will only be populated if the command `output_paths` field
	//   was used, and not the pre v2.1 `output_files` or `output_directories` fields.
	//
	// Check whether the current client is using REAPI version before or after v2.1.
	if len(cmd.OutputPaths) > 0 && len(cmd.OutputFiles) == 0 && len(cmd.OutputDirectories) == 0 {
		// REAPI >= v2.1
		if !dirHelper.IsOutputPath(fqfn) {
			return nil
		}
		actionResult.OutputSymlinks = append(actionResult.OutputSymlinks, symlink)
		// REAPI specification:
		//   Servers that wish to be compatible with v2.0 API should still
		//   populate `output_file_symlinks` and `output_directory_symlinks`
		//   in addition to `output_symlinks`.
		//
		// TODO(sluongng): Since v6.0.0, all the output directories are included in
		// Action.input_root_digest. So we should be able to save a `stat()` call by
		// checking wherether the symlink target was included as a directory inside
		// input root or not.
		//
		// Reference:
		//   https://github.com/bazelbuild/bazel/commit/4310aeb36c134e5fc61ed5cdfdf683f3e95f19b7
		symlinkInfo, err := os.Stat(fqfn)
		if err != nil {
			// When encounter a dangling symlink, skip setting it to legacy fields.
			// TODO(sluongng): do we care to log this?
			log.Warningf("Could not find symlink %q's target: %s, skip legacy fields", fqfn, err)
			return nil
		}
		if symlinkInfo.IsDir() {
			actionResult.OutputDirectorySymlinks = append(actionResult.OutputDirectorySymlinks, symlink)
		} else {
			actionResult.OutputFileSymlinks = append(actionResult.OutputFileSymlinks, symlink)
		}
		return nil
	}

	// REAPI < v2.1
	for _, expectedFile := range cmd.OutputFiles {
		if symlink.Path == expectedFile {
			actionResult.OutputFileSymlinks = append(actionResult.OutputFileSymlinks, symlink)
			break
		}
	}
	for _, expectedDir := range cmd.OutputDirectories {
		if symlink.Path == expectedDir {
			actionResult.OutputDirectorySymlinks = append(actionResult.OutputDirectorySymlinks, symlink)
			break
		}
	}
	return nil
}

func UploadTree(ctx context.Context, env environment.Env, dirHelper *DirHelper, instanceName string, digestFunction repb.DigestFunction_Value, rootDir string, cmd *repb.Command, actionResult *repb.ActionResult, addToFileCache bool) (*TransferInfo, error) {
	startTime := time.Now()
	outputDirectoryPaths := make([]string, 0)
	filesToUpload := make([]*fileToUpload, 0)
	visitedDirectories := make([]*dirToUpload, 0)

	visitFile := func(fullPath string, info os.FileInfo) (*repb.FileNode, error) {
		uploadableFile, err := newFileToUpload(digestFunction, fullPath, info)
		if err != nil {
			return nil, err
		}
		filesToUpload = append(filesToUpload, uploadableFile)
		if _, ok := dirHelper.FindParentOutputPath(fullPath); !ok {
			// If this file is *not* a descendant of any output_path but wasn't
			// skipped before the call to uploadFileFn, then it must be
			// appended to OutputFiles.
			actionResult.OutputFiles = append(actionResult.OutputFiles, uploadableFile.OutputFile(rootDir))
		}

		return uploadableFile.FileNode(), nil
	}

	var walkDir func(parentFullPath, dirName string) (*repb.DirectoryNode, error)
	walkDir = func(parentFullPath, dirName string) (*repb.DirectoryNode, error) {
		directory := &repb.Directory{}
		dirFullPath := filepath.Join(parentFullPath, dirName)
		dirInfo, err := os.Stat(dirFullPath)
		if err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(dirFullPath)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			childFullPath := filepath.Join(dirFullPath, info.Name())
			if info.IsDir() {
				// Don't recurse on non-uploadable directories.
				if !dirHelper.ShouldUploadAnythingInDir(childFullPath) {
					continue
				}
				// If this is a dir and it's an output path, then we need to
				// return it as an output_directory.
				isOutputDirectory := dirHelper.IsOutputPath(childFullPath)
				if isOutputDirectory {
					outputDirectoryPaths = append(outputDirectoryPaths, childFullPath)
				}
				dirNode, err := walkDir(dirFullPath, info.Name())
				if err != nil {
					return nil, err
				}
				if _, ok := dirHelper.FindParentOutputPath(childFullPath); !ok && !isOutputDirectory {
					continue
				}
				directory.Directories = append(directory.Directories, dirNode)
			} else if info.Mode().IsRegular() {
				if !dirHelper.ShouldUploadFile(childFullPath) {
					continue
				}
				fileNode, err := visitFile(childFullPath, info)
				if err != nil {
					return nil, err
				}
				directory.Files = append(directory.Files, fileNode)
			} else if info.Mode()&os.ModeSymlink == os.ModeSymlink {
				if err := handleSymlink(dirHelper, rootDir, cmd, actionResult, directory, childFullPath); err != nil {
					return nil, err
				}
			}
		}

		d, err := newDirToUpload(digestFunction, dirFullPath, dirInfo, directory)
		if err != nil {
			return nil, err
		}
		visitedDirectories = append(visitedDirectories, d)
		return d.DirectoryNode(), nil
	}
	if _, err := walkDir(rootDir, ""); err != nil {
		return nil, err
	}

	uploader := cachetools.NewBatchCASUploader(ctx, env, instanceName, digestFunction)

	// Upload output files to the remote cache and also add them to the local
	// cache since they are likely to be used as inputs to subsequent actions.
	alreadyPresentBytes, err := uploadMissingFiles(ctx, uploader, env, filesToUpload, instanceName, digestFunction, addToFileCache)
	if err != nil {
		return nil, err
	}

	// Upload Directory protos.
	// TODO: skip uploading Directory protos which are not part of any tree?
	for _, d := range visitedDirectories {
		rsc := cachetools.NewBytesReadSeekCloser(d.directoryBytes)
		if err := uploader.Upload(d.directoryDigest, rsc); err != nil {
			return nil, err
		}
	}

	// Make Trees for all of the paths specified in output_paths which were
	// directories (which we noted in the filesystem walk above).
	outputDirectoryTrees := make(map[string]*repb.Tree, 0)
	for _, outputDirectoryPath := range outputDirectoryPaths {
		outputDirectoryTrees[outputDirectoryPath] = &repb.Tree{}
	}

	// For each Directory encountered, determine which Tree (if any) it is a
	// part of, and add it.
	for _, d := range visitedDirectories {
		if tree, ok := outputDirectoryTrees[d.fullPath]; ok {
			tree.Root = d.directory
		} else if parentOutputPath, ok := dirHelper.FindParentOutputPath(d.fullPath); ok {
			tree = outputDirectoryTrees[parentOutputPath]
			if tree == nil {
				// Format outputDirectoryPaths with %q so we can see the exact
				// string contents.
				formattedOutputDirectoryPaths := make([]string, 0, len(outputDirectoryPaths))
				for _, p := range outputDirectoryPaths {
					formattedOutputDirectoryPaths = append(formattedOutputDirectoryPaths, fmt.Sprintf("%q", p))
				}
				alert.UnexpectedEvent(
					"malformed_parent_output_path",
					"Tree was not initialized for output at %q "+
						"with parent output path %q. "+
						"Output directory trees were only initialized for output paths [%s], "+
						"which do not contain the parent output path.",
					d.fullPath, parentOutputPath, strings.Join(formattedOutputDirectoryPaths, ", "))
				return nil, status.InternalErrorf("malformed parent output path")
			}
			tree.Children = append(tree.Children, d.directory)
		}
	}

	// Upload Tree protos for each output directory.
	for fullFilePath, tree := range outputDirectoryTrees {
		td, err := uploader.UploadProto(tree)
		if err != nil {
			return nil, err
		}
		actionResult.OutputDirectories = append(actionResult.OutputDirectories, &repb.OutputDirectory{
			Path:       filepath.ToSlash(trimPathPrefix(fullFilePath, rootDir)),
			TreeDigest: td,
		})
	}

	if err := uploader.Wait(); err != nil {
		return nil, err
	}
	uploadStats := uploader.Stats()
	metrics.SkippedOutputBytes.Add(float64(alreadyPresentBytes + uploadStats.DuplicateBytes))
	return &TransferInfo{
		TransferDuration: time.Since(startTime),
		FileCount:        uploadStats.UploadedObjects,
		BytesTransferred: uploadStats.UploadedBytes,
	}, nil
}

type FilePointer struct {
	// FullPath is the absolute path to the file, starting with "/" which refers
	// to the root directory of the local file system.
	// ex: /tmp/remote_execution/abc123/some/package/some_input.go
	FullPath string
	// RelativePath is the workspace-relative path to the file.
	// ex: some/package/some_input.go
	RelativePath string
	FileNode     *repb.FileNode
}

// removeExisting removes any existing file pointed to by the FilePointer if
// it exists in the opts.Skip (pre-existing files) map. This is needed so that
// we overwrite existing files without silently dropping errors when linking
// the file. (Note, it is not needed when writing a fresh copy of the file.)
func removeExisting(fp *FilePointer, opts *DownloadTreeOpts) error {
	pathKey := fspath.NewKey(fp.RelativePath, opts.CaseInsensitive)
	if _, ok := opts.Skip[pathKey]; ok {
		if err := os.Remove(fp.FullPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func writeFile(fp *FilePointer, data []byte, opts *DownloadTreeOpts) error {
	if err := removeExisting(fp, opts); err != nil {
		return err
	}
	var mode os.FileMode = 0644
	if fp.FileNode.IsExecutable {
		mode = 0755
	}
	f, err := os.OpenFile(fp.FullPath, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	//	defer log.Printf("Wrote %d bytes to file %q", len(data), filePath)
	return f.Close()
}

func (ff *BatchFileFetcher) writeToFileCache(fileNode *repb.FileNode, data []byte) error {
	w, err := ff.env.GetFileCache().Writer(ff.ctx, fileNode, ff.digestFunction)
	if err != nil {
		return err
	}
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}
	return nil
}

func copyFile(src *FilePointer, dest *FilePointer, opts *DownloadTreeOpts) error {
	if err := removeExisting(dest, opts); err != nil {
		return err
	}
	return fastcopy.FastCopy(src.FullPath, dest.FullPath)
}

// linkFileFromFileCache attempts to link the given file path from the local
// file cache, and returns whether the linking was successful.
func linkFileFromFileCache(ctx context.Context, fp *FilePointer, fc interfaces.FileCache, opts *DownloadTreeOpts) (bool, error) {
	if err := removeExisting(fp, opts); err != nil {
		return false, err
	}
	return fc.FastLinkFile(ctx, fp.FileNode, fp.FullPath), nil
}

// fetchKey identifies an artifact to be fetched from a remote cache.
type fetchKey struct {
	digest.Key

	// The file cache treats executable and non-executable files as separate
	// entries since the attributes are shared when hardlinking.
	// By differentiating executable and non-executable artifacts, we don't
	// need to worry about attribute differences when interacting with the
	// file cache.
	executable bool
}

func newFetchKey(d *repb.Digest, executable bool) fetchKey {
	return fetchKey{digest.NewKey(d), executable}
}

// FileMap is a map of digests to file pointers containing the contents
// addressed by the digest.
type FileMap map[fetchKey][]*FilePointer

type BatchFileFetcher struct {
	ctx                     context.Context
	env                     environment.Env
	instanceName            string
	digestFunction          repb.DigestFunction_Value
	once                    *sync.Once
	compress                bool
	treeWrangler            *inputTreeWrangler
	filesToFetch            FileMap
	opts                    *DownloadTreeOpts
	onlyDownloadToFileCache bool
	doneErr                 chan error

	mu               sync.Mutex
	remainingFetches map[fetchKey]struct{}
	fetchWaiters     map[fetchKey][]chan struct{}

	statsMu sync.Mutex
	stats   repb.IOStats
}

// newBatchFileFetcher creates a CAS fetcher that can automatically batch small requests and stream large files.
// `fileCache` is optional. If present, it's used to cache a copy of the data for use by future reads.
// `casClient` is optional. If not specified, all requests will use the ByteStream API.
func newBatchFileFetcher(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, filesToFetch FileMap, opts *DownloadTreeOpts) (*BatchFileFetcher, error) {
	if env.GetByteStreamClient() == nil {
		return nil, status.FailedPreconditionError("ByteStreamClient not available")
	}

	remainingFetches := make(map[fetchKey]struct{})
	for k := range filesToFetch {
		remainingFetches[k] = struct{}{}
	}
	return &BatchFileFetcher{
		ctx:                     ctx,
		env:                     env,
		treeWrangler:            getInputTreeWrangler(env),
		instanceName:            instanceName,
		digestFunction:          digestFunction,
		once:                    &sync.Once{},
		compress:                false,
		filesToFetch:            filesToFetch,
		opts:                    opts,
		onlyDownloadToFileCache: opts.RootDir == "",
		doneErr:                 make(chan error, 1),
		remainingFetches:        remainingFetches,
		fetchWaiters:            make(map[fetchKey][]chan struct{}),
	}, nil
}

func (ff *BatchFileFetcher) supportsCompression() bool {
	ff.once.Do(func() {
		if !*enableDownloadCompresssion {
			return
		}
		capabilitiesClient := ff.env.GetCapabilitiesClient()
		if capabilitiesClient == nil {
			log.Warningf("Download compression was enabled but no capabilities client found. Cannot verify cache server capabilities")
			return
		}
		enabled, err := cachetools.SupportsCompression(ff.ctx, capabilitiesClient)
		if err != nil {
			log.Errorf("Error determinining if cache server supports compression: %s", err)
		}
		if enabled {
			ff.compress = true
		} else {
			log.Debugf("Download compression was enabled but remote server did not support compression")
		}
	})
	return ff.compress
}

func (ff *BatchFileFetcher) notifyFetchCompleted(fk fetchKey) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.remainingFetches, fk)
	for _, w := range ff.fetchWaiters[fk] {
		w <- struct{}{}
	}
	delete(ff.fetchWaiters, fk)
}

func (ff *BatchFileFetcher) batchDownloadFiles(ctx context.Context, req *repb.BatchReadBlobsRequest, opts *DownloadTreeOpts) error {
	casClient := ff.env.GetContentAddressableStorageClient()
	if casClient == nil {
		return status.FailedPreconditionErrorf("cannot batch download files when casClient is not set")
	}
	responses, err := cachetools.BatchReadBlobs(ctx, ff.env.GetContentAddressableStorageClient(), req)
	if err != nil {
		return err
	}

	ff.statsMu.Lock()
	for _, res := range responses {
		if res.Err != nil {
			continue
		}
		// TODO: measure compressed size
		ff.stats.FileDownloadSizeBytes += res.Digest.GetSizeBytes()
		ff.stats.FileDownloadCount += 1
	}
	ff.statsMu.Unlock()

	fileCache := ff.env.GetFileCache()
	for _, res := range responses {
		if res.Err != nil {
			log.CtxInfof(ctx, "Failed to download %s: %s", digest.String(res.Digest), res.Err)
			return digest.MissingDigestError(res.Digest)
		}
		d := res.Digest
		for _, executable := range []bool{false, true} {
			fetchKey := newFetchKey(d, executable)
			ptrs, ok := ff.filesToFetch[fetchKey]
			if !ok || len(ptrs) == 0 {
				continue
			}
			ptr := ptrs[0]
			if ff.onlyDownloadToFileCache {
				if err := ff.writeToFileCache(ptr.FileNode, res.Data); err != nil {
					return err
				}
			} else {
				if err := writeFile(ptr, res.Data, opts); err != nil {
					return err
				}
				if fileCache != nil {
					if err := fileCache.AddFile(ff.ctx, ptr.FileNode, ptr.FullPath); err != nil {
						log.Warningf("Error adding file to filecache: %s", err)
					}
				}
				// Only need to write the first file explicitly; the rest of the files can
				// be fast-copied from the first.
				for _, dest := range ptrs[1:] {
					if err := copyFile(ptr, dest, opts); err != nil {
						return err
					}
				}
			}
			ff.notifyFetchCompleted(fetchKey)
		}
	}
	return nil
}

func (ff *BatchFileFetcher) checkAndMaybeLinkFromFileCache(filePointers []*FilePointer, opts *DownloadTreeOpts) error {
	if ff.onlyDownloadToFileCache {
		if len(filePointers) == 0 {
			return status.FailedPreconditionErrorf("file pointers list is empty")
		}
		// All the file pointers refer to the same artifact so we only need to
		// check once.
		fp := filePointers[0]
		if !ff.env.GetFileCache().ContainsFile(ff.ctx, fp.FileNode) {
			return status.NotFoundErrorf("File %s not found in cache", fp.FileNode.Digest.Hash)
		}
	} else {
		err := ff.treeWrangler.LinkFromFileCache(ff.ctx, filePointers, opts)
		if err != nil {
			return err
		}
	}
	ff.statsMu.Lock()
	ff.stats.LocalCacheHits += int64(len(filePointers))
	ff.statsMu.Unlock()
	return nil
}

type digestToFetch struct {
	key fetchKey
	fps []*FilePointer
}

func (ff *BatchFileFetcher) FetchFiles(opts *DownloadTreeOpts) (retErr error) {
	defer func() {
		if retErr != nil {
			ff.doneErr <- retErr
		}
	}()

	newRequest := func() *repb.BatchReadBlobsRequest {
		r := &repb.BatchReadBlobsRequest{
			InstanceName:   ff.instanceName,
			DigestFunction: ff.digestFunction,
		}
		if ff.supportsCompression() {
			r.AcceptableCompressors = append(r.AcceptableCompressors, repb.Compressor_ZSTD)
		}
		return r
	}

	linkEG, _ := errgroup.WithContext(ff.ctx)
	if runtime.GOOS == "darwin" {
		// Macs don't appear to like too much parallelism, with overall
		// performance dropping with high parallelism.
		linkEG.SetLimit(5)
	} else {
		limit := *linkParallelism
		if limit == 0 {
			limit = runtime.GOMAXPROCS(0)
		}
		linkEG.SetLimit(limit)
	}

	fetchQueue := make(chan digestToFetch, 100)

	linkStart := time.Now()
	linkEG.Go(func() error {
		// Note: filesToFetch is keyed by digest, so all files in `filePointers` have
		// the digest represented by dk.
		//
		// Attempt to link digests from the file cache. Digests that are not
		// present in the filecache will be added to the fetchQueue channel.
		for dk, filePointers := range ff.filesToFetch {
			filePointers := filePointers

			rn := digest.NewCASResourceName(dk.ToDigest(), ff.instanceName, ff.digestFunction)
			// Write empty files directly (skip checking cache and downloading).
			if rn.IsEmpty() && !ff.onlyDownloadToFileCache {
				for _, fp := range filePointers {
					if err := writeFile(fp, []byte(""), opts); err != nil {
						return err
					}
				}
				continue
			}

			linkEG.Go(func() error {
				// If the digest is in the file cache, there's nothing
				// more to do.
				if err := ff.checkAndMaybeLinkFromFileCache(filePointers, opts); err == nil {
					ff.notifyFetchCompleted(dk)
					return nil
				}

				// Otherwise, queue the digest to be fetched.
				fetchQueue <- digestToFetch{key: dk, fps: filePointers}
				return nil
			})
		}
		return nil
	})

	// Read work off the fetchQueue channel and generate batch read requests
	// to download data.
	eg, ctx := errgroup.WithContext(ff.ctx)
	eg.Go(func() error {
		req := newRequest()
		currentBatchRequestSize := int64(0)
		for f := range fetchQueue {
			// If the file exceeds our gRPC max size, it'll never
			// fit in the batch call, so we'll have to bytestream
			// it.
			size := f.key.SizeBytes
			if size > rpcutil.GRPCMaxSizeBytes || ff.env.GetContentAddressableStorageClient() == nil {
				eg.Go(func() error {
					if len(f.fps) == 0 {
						return status.FailedPreconditionError("empty file pointer list for key")
					}
					dedupeKey := downloadDedupeKey{groupID: groupIDStringFromContext(ctx), fetchKey: f.key}
					if ff.onlyDownloadToFileCache {
						return ff.bytestreamReadToFilecache(ctx, ff.env.GetByteStreamClient(), dedupeKey, f.fps)
					} else {
						return ff.bytestreamReadToFilesystem(ctx, ff.env.GetByteStreamClient(), dedupeKey, f.fps, opts)
					}
				})
				continue
			}

			// If the digest would push our current batch request
			// size over the gRPC max, dispatch the request and
			// start a new one.
			if currentBatchRequestSize+size > rpcutil.GRPCMaxSizeBytes {
				reqCopy := req
				eg.Go(func() error {
					return ff.batchDownloadFiles(ctx, reqCopy, opts)
				})
				req = newRequest()
				currentBatchRequestSize = 0
			}

			// Add the file to our current batch request and
			// increment our size.
			req.Digests = append(req.Digests, f.key.ToDigest())
			currentBatchRequestSize += size
		}

		// Make sure we fire the last request if there is one.
		if len(req.Digests) > 0 {
			reqCopy := req
			eg.Go(func() error {
				return ff.batchDownloadFiles(ctx, reqCopy, opts)
			})
		}
		return nil
	})

	// Close the fetchQueue channel after we are done linking so that the
	// fetch queue can terminate once all the digests are fetched.
	_ = linkEG.Wait()
	close(fetchQueue)

	ff.statsMu.Lock()
	ff.stats.LocalCacheLinkDuration = durationpb.New(time.Since(linkStart))
	ff.statsMu.Unlock()

	return eg.Wait()
}

func (ff *BatchFileFetcher) GetStats() *repb.IOStats {
	ff.statsMu.Lock()
	defer ff.statsMu.Unlock()
	return ff.stats.CloneVT()
}

func (ff *BatchFileFetcher) bytestreamReadToWriter(ctx context.Context, bsClient bspb.ByteStreamClient, fileNode *repb.FileNode, w interfaces.CommittedWriteCloser) error {
	resourceName := digest.NewCASResourceName(fileNode.Digest, ff.instanceName, ff.digestFunction)
	if ff.supportsCompression() {
		resourceName.SetCompressor(repb.Compressor_ZSTD)
	}

	if err := cachetools.GetBlob(ctx, bsClient, resourceName, w); err != nil {
		_ = w.Close()
		return err
	}

	if err := w.Commit(); err != nil {
		return status.WrapError(err, "could not commit byte stream writer")
	}

	if err := w.Close(); err != nil {
		return status.WrapError(err, "could not close byte stream writer")
	}

	ff.statsMu.Lock()
	ff.stats.FileDownloadSizeBytes += fileNode.GetDigest().SizeBytes
	ff.stats.FileDownloadCount += 1
	ff.statsMu.Unlock()

	return nil
}

// bytestreamReadToFilesystem streams a blob to the filesystem locations
// listed in fps. The blob is fetched once and linked into the remaining
// locations.
// The blob is optionally added to the file cache, if the file cache
// is enabled.
func (ff *BatchFileFetcher) bytestreamReadToFilesystem(ctx context.Context, bsClient bspb.ByteStreamClient, dedupeKey downloadDedupeKey, fps []*FilePointer, opts *DownloadTreeOpts) error {
	fp, _, err := DownloadDeduper.Do(ctx, dedupeKey, func(ctx context.Context) (*FilePointer, error) {
		fp0 := fps[0]

		var mode os.FileMode = 0644
		if fp0.FileNode.IsExecutable {
			mode = 0755
		}
		f, err := os.OpenFile(fp0.FullPath, os.O_RDWR|os.O_CREATE, mode)
		if err != nil {
			return nil, err
		}
		w := ioutil.NewCustomCommitWriteCloser(f)

		if err := ff.bytestreamReadToWriter(ctx, bsClient, fp0.FileNode, w); err != nil {
			return nil, err
		}

		fileCache := ff.env.GetFileCache()
		if fileCache != nil {
			if err := fileCache.AddFile(ff.ctx, fp0.FileNode, fp0.FullPath); err != nil {
				log.Warningf("Error adding file to filecache: %s", err)
			}
		}

		return fp0, nil
	})
	if err != nil {
		return err
	}

	// Depending on whether or not this bytestream request was deduped, fp
	// will either be == fps[0], or a different fp from a concurrent request
	// made by the same user.
	// Check for that case, to avoid copying a file over itself, and copy fp
	// to all of the destination fps.
	for _, dest := range fps {
		if fp == dest {
			continue
		}
		if err := copyFile(fp, dest, opts); err != nil {
			return err
		}
	}

	ff.notifyFetchCompleted(dedupeKey.fetchKey)

	return nil
}

// bytestreamReadToFilecache streams a blob directly into the filecache.
func (ff *BatchFileFetcher) bytestreamReadToFilecache(ctx context.Context, bsClient bspb.ByteStreamClient, dedupeKey downloadDedupeKey, fps []*FilePointer) error {
	_, _, err := DownloadDeduper.Do(ctx, dedupeKey, func(ctx context.Context) (*FilePointer, error) {
		fp0 := fps[0]

		w, err := ff.env.GetFileCache().Writer(ctx, fp0.FileNode, ff.digestFunction)
		if err != nil {
			return nil, status.WrapError(err, "could not create filecache writer")
		}

		if err := ff.bytestreamReadToWriter(ctx, bsClient, fp0.FileNode, w); err != nil {
			return nil, err
		}

		return fp0, nil
	})
	if err != nil {
		return err
	}

	ff.notifyFetchCompleted(dedupeKey.fetchKey)

	return nil
}

// Fetch blocks until the specified input has been fetched.
// TODO(vadim): prioritize fetches referenced through this call since this is
// a signal that this input is immediately needed.
func (ff *BatchFileFetcher) Fetch(ctx context.Context, node *repb.FileNode) error {
	key := newFetchKey(node.GetDigest(), node.GetIsExecutable())
	if _, ok := ff.filesToFetch[key]; !ok {
		return status.InvalidArgumentErrorf("attempting to fetch digest not in tree")
	}

	ff.mu.Lock()
	// Check if the fetch has already completed.
	if _, ok := ff.remainingFetches[key]; !ok {
		ff.mu.Unlock()
		return nil
	}

	done := make(chan struct{}, 1)
	ff.fetchWaiters[key] = append(ff.fetchWaiters[key], done)
	ff.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ff.doneErr:
		return status.WrapError(err, "fetcher failed")
	case <-done:
		return nil
	}
}

func DirMapFromTree(tree *repb.Tree, digestFunction repb.DigestFunction_Value) (rootDigest *repb.Digest, dirMap map[digest.Key]*repb.Directory, err error) {
	dirMap = make(map[digest.Key]*repb.Directory, 1+len(tree.Children))

	rootDigest, err = digest.ComputeForMessage(tree.Root, digestFunction)
	if err != nil {
		return nil, nil, err
	}
	dirMap[digest.NewKey(rootDigest)] = tree.Root

	for _, child := range tree.Children {
		d, err := digest.ComputeForMessage(child, digestFunction)
		if err != nil {
			return nil, nil, err
		}
		dirMap[digest.NewKey(d)] = child
	}

	return rootDigest, dirMap, nil
}

// To be used when a symlink exists and an Exists error has been returned, but
// the caller wants to ensure the *correct* file has been linked.
func checkSymlink(oldName, newName string) bool {
	pointee, err := os.Readlink(newName)
	if err != nil {
		return false
	}
	return pointee == oldName
}

type DownloadTreeOpts struct {
	// CaseInsensitive specifies whether the filesystem is case-insensitive.
	// If true, the paths will be normalized to lowercase.
	CaseInsensitive bool
	// Skip specifies file paths to skip, along with their file nodes. If the
	// file metadata or contents to be downloaded don't match the file in this
	// map, then it is re-downloaded (not skipped).
	Skip map[fspath.Key]*repb.FileNode
	// RootDir specifies the destination directory for the downloaded tree.
	// If not specified, tree digests will be downloaded directly into the filecache.
	RootDir string
	// TrackTransfers specifies whether to record the full set of files downloaded
	// and return them in TransferInfo.Transfers.
	TrackTransfers bool
}

type inputTreeRequest interface {
	Do() error
}

type mkdirAllRequest struct {
	path string
	perm os.FileMode
}

func (mar *mkdirAllRequest) Do() error {
	// Directories are created in tree traversal order, so parents should always
	// exist. os.Mkdir should work and be faster than os.MkdirAll so try it
	// first.
	err := os.Mkdir(mar.path, mar.perm)
	if err == nil {
		return err
	}
	return os.MkdirAll(mar.path, mar.perm)
}

type symlinkRequest struct {
	oldname string
	newname string
}

func (slr *symlinkRequest) Do() error {
	err := os.Symlink(slr.oldname, slr.newname)
	if err == nil {
		return nil
	}
	if !os.IsExist(err) {
		return err
	}
	if checkSymlink(slr.oldname, slr.newname) {
		return nil
	}
	// Attempt to blow away the existing
	// file(s) at that location and re-link.
	if err := os.RemoveAll(slr.newname); err != nil {
		return err
	}
	// Now that the symlink has been removed
	// try one more time to link it.
	if err := os.Symlink(slr.oldname, slr.newname); err != nil {
		return err
	}
	return nil
}

type linkFromFileCacheRequest struct {
	ctx          context.Context
	fileCache    interfaces.FileCache
	filePointers []*FilePointer
	opts         *DownloadTreeOpts
}

func (lffcr *linkFromFileCacheRequest) Do() error {
	numFilesLinked := 0
	for _, fp := range lffcr.filePointers {
		if lffcr.fileCache != nil {
			linked, err := linkFileFromFileCache(lffcr.ctx, fp, lffcr.fileCache, lffcr.opts)
			if err != nil {
				return err
			}
			if !linked {
				break
			}
			numFilesLinked++
		}
	}
	// If we successfully linked all files from the file cache, no need to
	// download this digest. Note: We may not successfully link all files
	// if the digest expires from the cache while the above loop is in progress.
	if numFilesLinked == len(lffcr.filePointers) {
		return nil
	}
	return status.NotFoundErrorf("could not link all file pointers")
}

type taskRequest struct {
	ctx    context.Context
	req    inputTreeRequest
	respCh chan error
}

func (tr *taskRequest) Do() {
	select {
	case <-tr.ctx.Done():
		tr.respCh <- tr.ctx.Err()
	default:
		select {
		case tr.respCh <- tr.req.Do():
		case <-tr.ctx.Done():
			tr.respCh <- tr.ctx.Err()
		}
	}
}

// inputTreeWrangler is a shared worker pool for performing input tree structure
// operations (mkdir, symlink, filecache link, etc) with the ability to apply
// a total limit on concurrent operations across all active actions to prevent
// performing too many operations on filesystems that don't do well beyond
// a certain level of concurrency.
type inputTreeWrangler struct {
	env     environment.Env
	limiter *semaphore.Weighted
	reqs    chan taskRequest

	mkdirAllLatencyUsec          prometheus.Observer
	symlinkLatencyUsec           prometheus.Observer
	linkFromFileCacheLatencyUsec prometheus.Observer
}

func newInputTreeWrangler(env environment.Env) *inputTreeWrangler {
	w := &inputTreeWrangler{
		env:                          env,
		mkdirAllLatencyUsec:          metrics.InputTreeSetupOpLatencyUsec.With(prometheus.Labels{metrics.OpLabel: "mkdirall"}),
		symlinkLatencyUsec:           metrics.InputTreeSetupOpLatencyUsec.With(prometheus.Labels{metrics.OpLabel: "symlink"}),
		linkFromFileCacheLatencyUsec: metrics.InputTreeSetupOpLatencyUsec.With(prometheus.Labels{metrics.OpLabel: "link_from_file_cache"}),
	}
	if *inputTreeSetupParallelism != -1 {
		w.limiter = semaphore.NewWeighted(int64(*inputTreeSetupParallelism))
	}
	return w
}

func (w *inputTreeWrangler) scheduleRequest(ctx context.Context, req inputTreeRequest) error {
	if w.limiter != nil {
		if err := w.limiter.Acquire(ctx, 1); err != nil {
			return err
		}
		defer w.limiter.Release(1)
	}
	return req.Do()
}

func (w *inputTreeWrangler) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	start := time.Now()
	err := w.scheduleRequest(ctx, &mkdirAllRequest{path: path, perm: perm})
	if err != nil {
		return err
	}
	w.mkdirAllLatencyUsec.Observe(float64(time.Since(start).Microseconds()))
	return nil
}

func (w *inputTreeWrangler) Symlink(ctx context.Context, oldname string, newname string) error {
	start := time.Now()
	err := w.scheduleRequest(ctx, &symlinkRequest{oldname: oldname, newname: newname})
	if err != nil {
		return err
	}
	w.symlinkLatencyUsec.Observe(float64(time.Since(start).Microseconds()))
	return nil
}

func (w *inputTreeWrangler) LinkFromFileCache(ctx context.Context, filePointers []*FilePointer, opts *DownloadTreeOpts) error {
	start := time.Now()
	err := w.scheduleRequest(ctx, &linkFromFileCacheRequest{ctx: ctx, fileCache: w.env.GetFileCache(), filePointers: filePointers, opts: opts})
	if err != nil {
		return err
	}
	w.linkFromFileCacheLatencyUsec.Observe(float64(time.Since(start).Microseconds()))
	return nil
}

func getInputTreeWrangler(env environment.Env) *inputTreeWrangler {
	initInputTreeWrangler.Do(func() {
		inputTreeWranglerInstance = newInputTreeWrangler(env)
	})
	w := *inputTreeWranglerInstance
	w.env = env
	return &w
}

type TreeFetcher struct {
	ctx            context.Context
	env            environment.Env
	tree           *repb.Tree
	instanceName   string
	digestFunction repb.DigestFunction_Value
	opts           *DownloadTreeOpts

	filesToFetch map[fetchKey][]*FilePointer

	ff             *BatchFileFetcher
	txInfo         *TransferInfo
	fetchStartTime time.Time
	done           chan error
}

func DownloadTree(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, tree *repb.Tree, opts *DownloadTreeOpts) (*TransferInfo, error) {
	tf, err := NewTreeFetcher(ctx, env, instanceName, digestFunction, tree, opts)
	if err != nil {
		return nil, err
	}
	if err := tf.Start(); err != nil {
		return nil, err
	}
	return tf.Wait()
}

func NewTreeFetcher(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, tree *repb.Tree, opts *DownloadTreeOpts) (*TreeFetcher, error) {
	return &TreeFetcher{
		ctx:            ctx,
		env:            env,
		tree:           tree,
		instanceName:   instanceName,
		digestFunction: digestFunction,
		opts:           opts,

		txInfo: &TransferInfo{},

		done: make(chan error, 1),
	}, nil
}

// Starts begins fetching the tree asynchronously.
func (f *TreeFetcher) Start() error {
	ctx := f.ctx
	treeWrangler := getInputTreeWrangler(f.env)

	f.fetchStartTime = time.Now()

	rootDirectoryDigest, dirMap, err := DirMapFromTree(f.tree, f.digestFunction)
	if err != nil {
		return err
	}

	trackTransfersFn := func(relPath string, node *repb.FileNode) {}
	trackExistsFn := func(relPath string, node *repb.FileNode) {}
	if f.opts.TrackTransfers {
		f.txInfo.Transfers = map[string]*repb.FileNode{}
		f.txInfo.Exists = map[fspath.Key]*repb.FileNode{}
		trackTransfersFn = func(relPath string, node *repb.FileNode) {
			f.txInfo.Transfers[relPath] = node
		}
		trackExistsFn = func(relPath string, node *repb.FileNode) {
			f.txInfo.Exists[fspath.NewKey(relPath, f.opts.CaseInsensitive)] = node
		}
	}

	onlyDownloadToFileCache := f.opts.RootDir == ""
	dirPerms := fs.FileMode(0777)
	f.filesToFetch = make(map[fetchKey][]*FilePointer, 0)
	var fetchDirFn func(dir *repb.Directory, parentDir string) error
	fetchDirFn = func(dir *repb.Directory, parentDir string) error {
		for _, fileNode := range dir.GetFiles() {
			func(node *repb.FileNode, location string) {
				d := node.GetDigest()
				fullPath := filepath.Join(location, node.Name)
				relPath := trimPathPrefix(fullPath, f.opts.RootDir)
				pathKey := fspath.NewKey(relPath, f.opts.CaseInsensitive)
				skippedNode, ok := f.opts.Skip[pathKey]
				if ok {
					trackExistsFn(relPath, node)
				}
				if ok && nodesEqual(node, skippedNode) {
					return
				}
				dk := newFetchKey(d, node.IsExecutable)
				f.filesToFetch[dk] = append(f.filesToFetch[dk], &FilePointer{
					FileNode:     node,
					FullPath:     fullPath,
					RelativePath: relPath,
				})
				trackTransfersFn(relPath, node)
			}(fileNode, parentDir)
		}
		for _, child := range dir.GetDirectories() {
			newRoot := filepath.Join(parentDir, child.GetName())
			if !onlyDownloadToFileCache {
				if err := treeWrangler.MkdirAll(ctx, newRoot, dirPerms); err != nil {
					return err
				}
			}
			rn := digest.NewResourceName(child.GetDigest(), f.instanceName, rspb.CacheType_CAS, f.digestFunction)
			if rn.IsEmpty() && rn.GetDigest().SizeBytes == 0 {
				continue
			}
			childDir, ok := dirMap[digest.NewKey(child.GetDigest())]
			if !ok {
				if child.GetDigest() == nil {
					log.CtxInfof(ctx, "Directory child digest is nil (parentDir=%q, childName=%q)", parentDir, child.GetName())
				}
				return digest.MissingDigestError(child.GetDigest())
			}
			if err := fetchDirFn(childDir, newRoot); err != nil {
				return err
			}
		}
		if !onlyDownloadToFileCache {
			for _, symlinkNode := range dir.GetSymlinks() {
				nodeAbsPath := filepath.Join(parentDir, symlinkNode.GetName())
				if err := treeWrangler.Symlink(ctx, symlinkNode.GetTarget(), nodeAbsPath); err != nil {
					return err
				}
			}
		}
		return nil
	}
	// Create the directory structure and track files to download.
	if err := fetchDirFn(dirMap[digest.NewKey(rootDirectoryDigest)], f.opts.RootDir); err != nil {
		return err
	}

	ff, err := newBatchFileFetcher(ctx, f.env, f.instanceName, f.digestFunction, f.filesToFetch, f.opts)
	if err != nil {
		return err
	}
	f.ff = ff

	go func() {
		// Download any files into the directory structure.
		f.done <- f.ff.FetchFiles(f.opts)
		close(f.done)
	}()

	return nil
}

// Wait blocks until all transfers are complete.
// TODO(vadim): add method to cancel pending fetches
func (f *TreeFetcher) Wait() (*TransferInfo, error) {
	var err error
	select {
	case err = <-f.done:
	case <-f.ctx.Done():
		err = f.ctx.Err()
	}

	// Update the stats even if an error is returned.
	stats := f.ff.GetStats()
	f.txInfo.BytesTransferred = stats.GetFileDownloadSizeBytes()
	f.txInfo.FileCount = stats.GetFileDownloadCount()
	f.txInfo.LinkCount = stats.GetLocalCacheHits()
	f.txInfo.LinkDuration = stats.GetLocalCacheLinkDuration().AsDuration()
	f.txInfo.TransferDuration = time.Since(f.fetchStartTime)
	return f.txInfo, err
}

// Fetch blocks until the specified node has been fetched.
func (f *TreeFetcher) Fetch(ctx context.Context, node *repb.FileNode) error {
	return f.ff.Fetch(ctx, node)
}

func nodesEqual(a *repb.FileNode, b *repb.FileNode) bool {
	return a.GetName() == b.GetName() &&
		a.GetDigest().GetHash() == b.GetDigest().GetHash() &&
		a.GetDigest().GetSizeBytes() == b.GetDigest().GetSizeBytes() &&
		a.GetIsExecutable() == b.GetIsExecutable()
}
