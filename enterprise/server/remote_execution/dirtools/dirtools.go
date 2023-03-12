package dirtools

import (
	"bytes"
	"context"
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const gRPCMaxSize = int64(4000000)

var (
	enableDownloadCompresssion = flag.Bool("cache.client.enable_download_compression", false, "If true, enable compression of downloads from remote caches")
)

type TransferInfo struct {
	FileCount        int64
	BytesTransferred int64
	TransferDuration time.Duration
	// Transfers tracks the files that were transferred, keyed by their
	// workspace-relative paths.
	Transfers map[string]*repb.FileNode
	// Exists tracks the files that already existed, keyed by their
	// workspace-relative paths.
	Exists map[string]*repb.FileNode
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

func NewDirHelper(rootDir string, outputDirectories, outputPaths []string, dirPerms fs.FileMode) *DirHelper {
	c := &DirHelper{
		rootDir:      rootDir,
		prefixes:     make(map[string]struct{}, 0),
		fullPaths:    make(map[string]struct{}, 0),
		dirsToCreate: make([]string, 0),
		outputPaths:  make(map[string]struct{}, 0),
		dirPerms:     dirPerms,
	}

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
	for _, outputDirectory := range outputDirectories {
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

type fileToUpload struct {
	resourceName *digest.ResourceName
	info         os.FileInfo
	fullFilePath string

	// If this was a directory, this is set, because the bytes to be
	// uploaded are the contents of a directory proto, not the contents
	// of the file at fullFilePath.
	data []byte
	dir  *repb.Directory
}

func newDirToUpload(instanceName, parentDir string, info os.FileInfo, dir *repb.Directory) (*fileToUpload, error) {
	data, err := proto.Marshal(dir)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)
	r, err := cachetools.ComputeDigest(reader, instanceName)
	if err != nil {
		return nil, err
	}

	return &fileToUpload{
		fullFilePath: filepath.Join(parentDir, info.Name()),
		info:         info,
		resourceName: r,
		data:         data,
		dir:          dir,
	}, nil
}

func newFileToUpload(instanceName, parentDir string, info os.FileInfo) (*fileToUpload, error) {
	fullFilePath := filepath.Join(parentDir, info.Name())
	ad, err := cachetools.ComputeFileDigest(fullFilePath, instanceName)
	if err != nil {
		return nil, err
	}
	return &fileToUpload{
		fullFilePath: fullFilePath,
		info:         info,
		resourceName: ad,
	}, nil
}

func (f *fileToUpload) ReadSeekCloser() (io.ReadSeekCloser, error) {
	if f.data != nil {
		return cachetools.NewBytesReadSeekCloser(f.data), nil
	}
	file, err := os.Open(f.fullFilePath)
	// Note: Caller is responsible for closing.
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f *fileToUpload) OutputFile(rootDir string) *repb.OutputFile {
	return &repb.OutputFile{
		Path:         trimPathPrefix(f.fullFilePath, rootDir),
		Digest:       f.resourceName.GetDigest(),
		IsExecutable: f.info.Mode()&0111 != 0,
	}
}

func (f *fileToUpload) FileNode() *repb.FileNode {
	return &repb.FileNode{
		Name:         f.info.Name(),
		Digest:       f.resourceName.GetDigest(),
		IsExecutable: f.info.Mode()&0111 != 0,
	}
}

func (f *fileToUpload) OutputDirectory(rootDir string) *repb.OutputDirectory {
	return &repb.OutputDirectory{
		Path:       trimPathPrefix(f.fullFilePath, rootDir),
		TreeDigest: f.resourceName.GetDigest(),
	}
}

func (f *fileToUpload) DirNode() *repb.DirectoryNode {
	return &repb.DirectoryNode{
		Name:   f.info.Name(),
		Digest: f.resourceName.GetDigest(),
	}
}

func uploadFiles(ctx context.Context, env environment.Env, instanceName string, filesToUpload []*fileToUpload) error {
	uploader := cachetools.NewBatchCASUploader(ctx, env, instanceName)
	fc := env.GetFileCache()

	for _, uploadableFile := range filesToUpload {
		// Add output files to the filecache.
		if fc != nil && uploadableFile.dir == nil {
			node := uploadableFile.FileNode()
			fc.AddFile(node, uploadableFile.fullFilePath)
		}

		rsc, err := uploadableFile.ReadSeekCloser()
		if err != nil {
			return err
		}
		// Note: uploader.Upload closes the file after it is uploaded.
		if err := uploader.Upload(uploadableFile.resourceName.GetDigest(), rsc); err != nil {
			return err
		}
	}

	return uploader.Wait()
}

func UploadTree(ctx context.Context, env environment.Env, dirHelper *DirHelper, instanceName, rootDir string, actionResult *repb.ActionResult) (*TransferInfo, error) {
	txInfo := &TransferInfo{}
	startTime := time.Now()
	treesToUpload := make([]string, 0)
	filesToUpload := make([]*fileToUpload, 0)
	uploadFileFn := func(parentDir string, info os.FileInfo) (*repb.FileNode, error) {
		uploadableFile, err := newFileToUpload(instanceName, parentDir, info)
		if err != nil {
			return nil, err
		}
		filesToUpload = append(filesToUpload, uploadableFile)
		fqfn := filepath.Join(parentDir, info.Name())
		if _, ok := dirHelper.FindParentOutputPath(fqfn); !ok {
			// If this file is *not* a descendant of any output_path but wasn't
			// skipped before the call to uploadFileFn, then it must be
			// appended to OutputFiles.
			actionResult.OutputFiles = append(actionResult.OutputFiles, uploadableFile.OutputFile(rootDir))
		}

		return uploadableFile.FileNode(), nil
	}

	var uploadDirFn func(parentDir, dirName string) (*repb.DirectoryNode, error)
	uploadDirFn = func(parentDir, dirName string) (*repb.DirectoryNode, error) {
		directory := &repb.Directory{}
		fullPath := filepath.Join(parentDir, dirName)
		dirInfo, err := os.Stat(fullPath)
		if err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(fullPath)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			fqfn := filepath.Join(fullPath, info.Name())
			if info.Mode().IsDir() {
				// Don't recurse on non-uploadable directories.
				if !dirHelper.ShouldUploadAnythingInDir(fqfn) {
					continue
				}
				if dirHelper.IsOutputPath(fqfn) {
					treesToUpload = append(treesToUpload, fqfn)
				}
				dirNode, err := uploadDirFn(fullPath, info.Name())
				if err != nil {
					return nil, err
				}
				txInfo.FileCount += 1
				txInfo.BytesTransferred += dirNode.GetDigest().GetSizeBytes()
				directory.Directories = append(directory.Directories, dirNode)
			} else if info.Mode().IsRegular() {
				if !dirHelper.ShouldUploadFile(fqfn) {
					continue
				}
				fileNode, err := uploadFileFn(fullPath, info)
				if err != nil {
					return nil, err
				}

				txInfo.FileCount += 1
				txInfo.BytesTransferred += fileNode.GetDigest().GetSizeBytes()
				directory.Files = append(directory.Files, fileNode)
			} else if info.Mode()&os.ModeSymlink == os.ModeSymlink {
				target, err := os.Readlink(fqfn)
				if err != nil {
					return nil, err
				}
				directory.Symlinks = append(directory.Symlinks, &repb.SymlinkNode{
					Name:   trimPathPrefix(fqfn, rootDir),
					Target: target,
				})
			}
		}

		uploadableDir, err := newDirToUpload(instanceName, parentDir, dirInfo, directory)
		if err != nil {
			return nil, err
		}
		filesToUpload = append(filesToUpload, uploadableDir)
		return uploadableDir.DirNode(), nil
	}
	if _, err := uploadDirFn(rootDir, ""); err != nil {
		return nil, err
	}
	if err := uploadFiles(ctx, env, instanceName, filesToUpload); err != nil {
		return nil, err
	}

	// Make Trees of all of the paths specified in output_paths which were
	// directories (which we noted in the filesystem walk above).
	trees := make(map[string]*repb.Tree, 0)
	for _, treeToUpload := range treesToUpload {
		trees[treeToUpload] = &repb.Tree{}
	}

	// For each of the filesToUpload, determine which Tree (if any) it is a part
	// of and add it.
	for _, f := range filesToUpload {
		if f.dir == nil {
			continue
		}
		fqfn := f.fullFilePath
		if tree, ok := trees[fqfn]; ok {
			tree.Root = f.dir
		} else if treePath, ok := dirHelper.FindParentOutputPath(fqfn); ok {
			tree = trees[treePath]
			tree.Children = append(tree.Children, f.dir)
		}
	}

	for fullFilePath, tree := range trees {
		td, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, tree)
		if err != nil {
			return nil, err
		}
		actionResult.OutputDirectories = append(actionResult.OutputDirectories, &repb.OutputDirectory{
			Path:       trimPathPrefix(fullFilePath, rootDir),
			TreeDigest: td,
		})
	}

	endTime := time.Now()
	txInfo.TransferDuration = endTime.Sub(startTime)
	return txInfo, nil
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
	if _, ok := opts.Skip[fp.RelativePath]; ok {
		if err := os.Remove(fp.FullPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func writeFile(fp *FilePointer, data []byte) error {
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

func copyFile(src *FilePointer, dest *FilePointer, opts *DownloadTreeOpts) error {
	if err := removeExisting(dest, opts); err != nil {
		return err
	}
	return fastcopy.FastCopy(src.FullPath, dest.FullPath)
}

// linkFileFromFileCache attempts to link the given file path from the local
// file cache, and returns whether the linking was successful.
func linkFileFromFileCache(d *repb.Digest, fp *FilePointer, fc interfaces.FileCache, opts *DownloadTreeOpts) (bool, error) {
	if err := removeExisting(fp, opts); err != nil {
		return false, err
	}
	return fc.FastLinkFile(fp.FileNode, fp.FullPath), nil
}

// FileMap is a map of digests to file pointers containing the contents
// addressed by the digest.
type FileMap map[digest.Key][]*FilePointer

type BatchFileFetcher struct {
	ctx          context.Context
	env          environment.Env
	instanceName string
	once         *sync.Once
	compress     bool

	statsMu sync.Mutex
	stats   repb.IOStats
}

// NewBatchFileFetcher creates a CAS fetcher that can automatically batch small requests and stream large files.
// `fileCache` is optional. If present, it's used to cache a copy of the data for use by future reads.
// `casClient` is optional. If not specified, all requests will use the ByteStream API.
func NewBatchFileFetcher(ctx context.Context, env environment.Env, instanceName string) *BatchFileFetcher {
	return &BatchFileFetcher{
		ctx:          ctx,
		env:          env,
		instanceName: instanceName,
		once:         &sync.Once{},
		compress:     false,
	}
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

func (ff *BatchFileFetcher) batchDownloadFiles(ctx context.Context, req *repb.BatchReadBlobsRequest, filesToFetch FileMap, opts *DownloadTreeOpts) error {
	casClient := ff.env.GetContentAddressableStorageClient()
	if casClient == nil {
		return status.FailedPreconditionErrorf("cannot batch download files when casClient is not set")
	}

	rsp, err := casClient.BatchReadBlobs(ctx, req)
	if err != nil {
		return err
	}

	ff.statsMu.Lock()
	for _, fileResponse := range rsp.GetResponses() {
		if fileResponse.GetStatus().GetCode() != int32(codes.OK) {
			continue
		}
		ff.stats.FileDownloadSizeBytes += fileResponse.GetDigest().GetSizeBytes()
		ff.stats.FileDownloadCount += 1
	}
	ff.statsMu.Unlock()

	fileCache := ff.env.GetFileCache()
	for _, fileResponse := range rsp.GetResponses() {
		if fileResponse.GetStatus().GetCode() != int32(codes.OK) {
			return digest.MissingDigestError(fileResponse.GetDigest())
		}
		d := fileResponse.GetDigest()
		ptrs, ok := filesToFetch[digest.NewKey(d)]
		if !ok {
			return status.InternalErrorf("Fetched unrequested file: %q", d)
		}
		if len(ptrs) == 0 {
			continue
		}
		data := fileResponse.GetData()
		if fileResponse.GetCompressor() == repb.Compressor_ZSTD {
			data, err = compression.DecompressZstd(nil, data)
			if err != nil {
				return err
			}
		}

		ptr := ptrs[0]
		if err := writeFile(ptr, data); err != nil {
			return err
		}
		if fileCache != nil {
			fileCache.AddFile(ptr.FileNode, ptr.FullPath)
		}
		// Only need to write the first file explicitly; the rest of the files can
		// be fast-copied from the first.
		for _, dest := range ptrs[1:] {
			if err := copyFile(ptr, dest, opts); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ff *BatchFileFetcher) FetchFiles(filesToFetch FileMap, opts *DownloadTreeOpts) error {
	newRequest := func() *repb.BatchReadBlobsRequest {
		r := &repb.BatchReadBlobsRequest{
			InstanceName: ff.instanceName,
		}
		if ff.supportsCompression() {
			r.AcceptableCompressors = append(r.AcceptableCompressors, repb.Compressor_ZSTD)
		}
		return r
	}
	req := newRequest()

	currentBatchRequestSize := int64(0)
	eg, ctx := errgroup.WithContext(ff.ctx)

	fileCache := ff.env.GetFileCache()
	// Note: filesToFetch is keyed by digest, so all files in `filePointers` have
	// the digest represented by dk.
	for dk, filePointers := range filesToFetch {
		d := dk.ToDigest()

		rn := digest.NewResourceName(dk.ToDigest(), ff.instanceName, rspb.CacheType_CAS)
		// Write empty files directly (skip checking cache and downloading).
		if rn.IsEmpty() {
			for _, fp := range filePointers {
				if err := writeFile(fp, []byte("")); err != nil {
					return err
				}
			}
			continue
		}

		// Attempt to link files from the local file cache.
		numFilesLinked := 0
		for _, fp := range filePointers {
			d := fp.FileNode.GetDigest()
			if fileCache != nil {
				linked, err := linkFileFromFileCache(d, fp, fileCache, opts)
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
		if numFilesLinked == len(filePointers) {
			continue
		}

		// At this point we need to download the contents of the digest.
		// If the file exceeds our gRPC max size, it'll never
		// fit in the batch call, so we'll have to bytestream
		// it.
		size := d.GetSizeBytes()
		if size > gRPCMaxSize || ff.env.GetContentAddressableStorageClient() == nil {
			func(d *repb.Digest, fps []*FilePointer) {
				eg.Go(func() error {
					return ff.bytestreamReadFiles(ctx, ff.instanceName, d, fps, opts)
				})
			}(d, filePointers)
			continue
		}

		// If the digest would push our current batch request
		// size over the gRPC max, dispatch the request and
		// start a new one.
		if currentBatchRequestSize+size > gRPCMaxSize {
			func(req *repb.BatchReadBlobsRequest) {
				eg.Go(func() error {
					return ff.batchDownloadFiles(ctx, req, filesToFetch, opts)
				})
			}(req)
			req = newRequest()
			currentBatchRequestSize = 0
		}

		// Add the file to our current batch request and
		// increment our size.
		req.Digests = append(req.Digests, d)
		currentBatchRequestSize += size
	}

	// Make sure we fire the last request if there is one.
	if len(req.Digests) > 0 {
		eg.Go(func() error {
			return ff.batchDownloadFiles(ctx, req, filesToFetch, opts)
		})
	}
	return eg.Wait()
}

func (ff *BatchFileFetcher) GetStats() *repb.IOStats {
	ff.statsMu.Lock()
	defer ff.statsMu.Unlock()
	return proto.Clone(&ff.stats).(*repb.IOStats)
}

// bytestreamReadFiles reads the given digest from the bytestream and creates
// files pointing to those contents.
func (ff *BatchFileFetcher) bytestreamReadFiles(ctx context.Context, instanceName string, d *repb.Digest, fps []*FilePointer, opts *DownloadTreeOpts) error {
	bsClient := ff.env.GetByteStreamClient()
	if bsClient == nil {
		return status.FailedPreconditionErrorf("cannot bytestream read files when bsClient is not set")
	}

	if len(fps) == 0 {
		return nil
	}
	fp := fps[0]
	var mode os.FileMode = 0644
	if fp.FileNode.IsExecutable {
		mode = 0755
	}
	f, err := os.OpenFile(fp.FullPath, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	resourceName := digest.NewResourceName(fp.FileNode.Digest, instanceName, rspb.CacheType_CAS)
	if ff.supportsCompression() {
		resourceName.SetCompressor(repb.Compressor_ZSTD)
	}
	if err := cachetools.GetBlob(ctx, bsClient, resourceName, f); err != nil {
		return err
	}

	ff.statsMu.Lock()
	ff.stats.FileDownloadSizeBytes += d.GetSizeBytes()
	ff.stats.FileDownloadCount += 1
	ff.statsMu.Unlock()

	if err := f.Close(); err != nil {
		return err
	}
	fileCache := ff.env.GetFileCache()
	if fileCache != nil {
		fileCache.AddFile(fp.FileNode, fp.FullPath)
	}

	// The rest of the files in the list all have the same digest, so we can
	// FastCopy them from the first file.
	for _, dest := range fps[1:] {
		if err := copyFile(fp, dest, opts); err != nil {
			return err
		}
	}
	return nil
}

func fetchDir(ctx context.Context, bsClient bspb.ByteStreamClient, reqDigest *digest.ResourceName) (*repb.Directory, error) {
	dir := &repb.Directory{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, reqDigest, dir); err != nil {
		return nil, err
	}
	return dir, nil
}

func DirMapFromTree(tree *repb.Tree) (rootDigest *repb.Digest, dirMap map[digest.Key]*repb.Directory, err error) {
	dirMap = make(map[digest.Key]*repb.Directory, 1+len(tree.Children))

	rootDigest, err = digest.ComputeForMessage(tree.Root, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, nil, err
	}
	dirMap[digest.NewKey(rootDigest)] = tree.Root

	for _, child := range tree.Children {
		d, err := digest.ComputeForMessage(child, repb.DigestFunction_SHA256)
		if err != nil {
			return nil, nil, err
		}
		dirMap[digest.NewKey(d)] = child
	}

	return rootDigest, dirMap, nil
}

type DownloadTreeOpts struct {
	// NonrootWritable specifies whether directories should be made writable
	// by users other than root. Does not affect file permissions.
	NonrootWritable bool
	// Skip specifies file paths to skip, along with their file nodes. If the digest
	// and executable bit of a file to be downloaded doesn't match the digest
	// and executable bit of the file in this map, then it is re-downloaded (not skipped).
	Skip map[string]*repb.FileNode
	// TrackTransfers specifies whether to record the full set of files downloaded
	// and return them in TransferInfo.Transfers.
	TrackTransfers bool
}

func DownloadTree(ctx context.Context, env environment.Env, instanceName string, tree *repb.Tree, rootDir string, opts *DownloadTreeOpts) (*TransferInfo, error) {
	txInfo := &TransferInfo{}
	startTime := time.Now()

	rootDirectoryDigest, dirMap, err := DirMapFromTree(tree)
	if err != nil {
		return nil, err
	}

	trackTransfersFn := func(relPath string, node *repb.FileNode) {}
	trackExistsFn := func(relPath string, node *repb.FileNode) {}
	if opts.TrackTransfers {
		txInfo.Transfers = map[string]*repb.FileNode{}
		txInfo.Exists = map[string]*repb.FileNode{}
		trackTransfersFn = func(relPath string, node *repb.FileNode) {
			txInfo.Transfers[relPath] = node
		}
		trackExistsFn = func(relPath string, node *repb.FileNode) {
			txInfo.Exists[relPath] = node
		}
	}

	dirPerms := fs.FileMode(0755)
	if opts.NonrootWritable {
		dirPerms = 0777
	}

	filesToFetch := make(map[digest.Key][]*FilePointer, 0)
	var fetchDirFn func(dir *repb.Directory, parentDir string) error
	fetchDirFn = func(dir *repb.Directory, parentDir string) error {
		for _, fileNode := range dir.GetFiles() {
			func(node *repb.FileNode, location string) {
				d := node.GetDigest()
				fullPath := filepath.Join(location, node.Name)
				relPath := trimPathPrefix(fullPath, rootDir)
				skippedNode, ok := opts.Skip[relPath]
				if ok {
					trackExistsFn(relPath, node)
				}
				if ok && nodesEqual(node, skippedNode) {
					return
				}
				dk := digest.NewKey(d)
				filesToFetch[dk] = append(filesToFetch[dk], &FilePointer{
					FileNode:     node,
					FullPath:     fullPath,
					RelativePath: relPath,
				})
				trackTransfersFn(relPath, node)
			}(fileNode, parentDir)
		}
		for _, child := range dir.GetDirectories() {
			newRoot := filepath.Join(parentDir, child.GetName())
			if err := os.MkdirAll(newRoot, dirPerms); err != nil {
				return err
			}
			rn := digest.NewResourceName(child.GetDigest(), instanceName, rspb.CacheType_CAS)
			if rn.IsEmpty() && rn.GetDigest().SizeBytes == 0 {
				continue
			}
			childDir, ok := dirMap[digest.NewKey(child.GetDigest())]
			if !ok {
				return digest.MissingDigestError(child.GetDigest())
			}
			if err := fetchDirFn(childDir, newRoot); err != nil {
				return err
			}
		}
		for _, symlinkNode := range dir.GetSymlinks() {
			nodeAbsPath := filepath.Join(parentDir, symlinkNode.GetName())
			if err := os.Symlink(symlinkNode.GetTarget(), nodeAbsPath); err != nil {
				return err
			}
		}
		return nil
	}
	// Create the directory structure and track files to download.
	if err := fetchDirFn(dirMap[digest.NewKey(rootDirectoryDigest)], rootDir); err != nil {
		return nil, err
	}

	ff := NewBatchFileFetcher(ctx, env, instanceName)

	// Download any files into the directory structure.
	if err := ff.FetchFiles(filesToFetch, opts); err != nil {
		return nil, err
	}
	endTime := time.Now()
	txInfo.TransferDuration = endTime.Sub(startTime)
	stats := ff.GetStats()
	txInfo.BytesTransferred = stats.GetFileDownloadSizeBytes()
	txInfo.FileCount = stats.GetFileDownloadCount()

	return txInfo, nil
}

func nodesEqual(a *repb.FileNode, b *repb.FileNode) bool {
	return a.GetDigest().GetHash() == b.GetDigest().GetHash() &&
		a.GetDigest().GetSizeBytes() == b.GetDigest().GetSizeBytes() &&
		a.GetIsExecutable() == b.GetIsExecutable()
}
