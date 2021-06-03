package dirtools

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/fastcopy"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

const gRPCMaxSize = int64(4000000)

type TransferInfo struct {
	FileCount        int64
	BytesTransferred int64
	TransferDuration time.Duration
	// Transfers tracks the digests of files that were transferred, keyed by their
	// workspace-relative paths.
	Transfers map[string]*repb.Digest
}

// DirHelper is a poor mans trie that helps us check if a partial path like
// /foo/bar matches one of a set of paths like /foo/bar/baz/bap and /a/b/c/d.
// Really should use a trie but this will do for now.
type DirHelper struct {
	// The directory under which all paths should descend.
	rootDir string

	// Prefixes of any output path -- used to truncate tree
	// traversal if we're uploading non-requested files.
	prefixes map[string]struct{}

	// The full output paths themselves -- used when creating
	// the ActionResult OutputFiles and OutputDirs, etc.
	fullPaths map[string]struct{}

	// The directories that should be created before running any
	// commands. (Parent dirs of requested outputs)
	dirsToCreate []string

	outputDirs []string
}

func NewDirHelper(rootDir string, command *repb.Command) *DirHelper {
	c := &DirHelper{
		rootDir:      rootDir,
		prefixes:     make(map[string]struct{}, 0),
		fullPaths:    make(map[string]struct{}, 0),
		dirsToCreate: make([]string, 0),
		outputDirs:   make([]string, 0),
	}

	for _, outputFile := range command.GetOutputFiles() {
		fullPath := filepath.Join(c.rootDir, outputFile)
		c.fullPaths[fullPath] = struct{}{}
		c.dirsToCreate = append(c.dirsToCreate, filepath.Dir(fullPath))
	}
	for _, outputDir := range command.GetOutputDirectories() {
		fullPath := filepath.Join(c.rootDir, outputDir)
		c.fullPaths[fullPath] = struct{}{}
		c.dirsToCreate = append(c.dirsToCreate, fullPath)
		c.outputDirs = append(c.outputDirs, fullPath)
	}

	// STOPSHIP(tylerw): Do we need an updated copy of the remote APIs proto?
	// if outputPaths := command.GetOutputPaths(); len(outputPaths) > 0 {
	// 	c.dirsToCreate = make([]string, 0)
	// 	for _, outputPath :=  range outputPaths {
	// 		c.dirsToCreate = append(c.dirsToCreate, filepath.Join(rootDir, filepath.Dir(outputPath)))
	// 	}
	// }

	for _, dir := range c.dirsToCreate {
		for p := dir; p != filepath.Dir(p); p = filepath.Dir(p) {
			c.prefixes[p] = struct{}{}
		}
	}

	return c
}
func (c *DirHelper) CreateOutputDirs() error {
	for _, dir := range c.dirsToCreate {
		if err := disk.EnsureDirectoryExists(dir); err != nil {
			return err
		}
	}
	return nil
}
func (c *DirHelper) MatchesOutputDir(path string) (string, bool) {
	for _, d := range c.outputDirs {
		for p := path; p != filepath.Dir(p); p = filepath.Dir(p) {
			if p == d {
				return d, true
			}
		}
	}
	return "", false
}
func (c *DirHelper) ShouldBeUploaded(path string) bool {
	// a file is an output file
	// a file is a prefix of an output file
	// a file is an output directory or inside of one
	if c.MatchesOutputFile(path) {
		return true
	}
	if c.MatchesOutputFilePrefix(path) {
		return true
	}
	if _, ok := c.MatchesOutputDir(path); ok {
		return true
	}
	return false
}
func (c *DirHelper) MatchesOutputFilePrefix(path string) bool {
	_, ok := c.prefixes[path]
	return ok
}
func (c *DirHelper) MatchesOutputFile(path string) bool {
	_, ok := c.fullPaths[path]
	return ok
}

func trimPathPrefix(fullPath, prefix string) string {
	r := strings.TrimPrefix(fullPath, prefix)
	if len(r) > 0 && r[0] == os.PathSeparator {
		r = r[1:]
	}
	return r
}

type fileToUpload struct {
	ad           *digest.InstanceNameDigest
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
	ad, err := cachetools.ComputeDigest(reader, instanceName)
	if err != nil {
		return nil, err
	}

	return &fileToUpload{
		fullFilePath: filepath.Join(parentDir, info.Name()),
		info:         info,
		ad:           ad,
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
		ad:           ad,
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
		Digest:       f.ad.Digest,
		IsExecutable: f.info.Mode()&0111 != 0,
	}
}

func (f *fileToUpload) FileNode() *repb.FileNode {
	return &repb.FileNode{
		Name:         f.info.Name(),
		Digest:       f.ad.Digest,
		IsExecutable: f.info.Mode()&0111 != 0,
	}
}

func (f *fileToUpload) OutputDirectory(rootDir string) *repb.OutputDirectory {
	return &repb.OutputDirectory{
		Path:       trimPathPrefix(f.fullFilePath, rootDir),
		TreeDigest: f.ad.Digest,
	}
}

func (f *fileToUpload) DirNode() *repb.DirectoryNode {
	return &repb.DirectoryNode{
		Name:   f.info.Name(),
		Digest: f.ad.Digest,
	}
}

func batchUploadFiles(ctx context.Context, env environment.Env, req *repb.BatchUpdateBlobsRequest) error {
	casClient := env.GetContentAddressableStorageClient()
	rsp, err := casClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		return err
	}
	for _, fileResponse := range rsp.GetResponses() {
		if fileResponse.GetStatus().GetCode() != int32(codes.OK) {
			return gstatus.Error(codes.Code(fileResponse.GetStatus().GetCode()), fmt.Sprintf("Error uploading file: %v", fileResponse.GetDigest()))
		}
	}
	return nil
}

func uploadFiles(ctx context.Context, env environment.Env, instanceName string, filesToUpload []*fileToUpload) error {
	uploader, err := cachetools.NewBatchCASUploader(ctx, env, instanceName)
	if err != nil {
		return err
	}
	fc := env.GetFileCache()

	for _, uploadableFile := range filesToUpload {
		// Add output files to the filecache.
		if fc != nil && uploadableFile.dir == nil {
			fc.AddFile(uploadableFile.ad.Digest, uploadableFile.fullFilePath)
		}

		rsc, err := uploadableFile.ReadSeekCloser()
		if err != nil {
			return err
		}
		// Note: uploader.Upload closes the file after it is uploaded.
		if err := uploader.Upload(uploadableFile.ad.Digest, rsc); err != nil {
			return err
		}
	}

	return uploader.Wait()
}

func UploadTree(ctx context.Context, env environment.Env, dirHelper *DirHelper, instanceName, rootDir string, actionResult *repb.ActionResult) (*TransferInfo, error) {
	txInfo := &TransferInfo{}
	startTime := time.Now()
	filesToUpload := make([]*fileToUpload, 0)
	uploadFileFn := func(parentDir string, info os.FileInfo) (*repb.FileNode, error) {
		uploadableFile, err := newFileToUpload(instanceName, parentDir, info)
		if err != nil {
			return nil, err
		}
		filesToUpload = append(filesToUpload, uploadableFile)
		fqfn := filepath.Join(parentDir, info.Name())
		if _, ok := dirHelper.MatchesOutputDir(fqfn); !ok {
			// If this file does *not* match an output dir but wasn't
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
		files, err := ioutil.ReadDir(fullPath)
		if err != nil {
			return nil, err
		}
		for _, info := range files {
			fqfn := filepath.Join(fullPath, info.Name())
			if info.Mode().IsDir() {
				// Don't recurse on non-uploadable directories.
				if !dirHelper.ShouldBeUploaded(fqfn) {
					continue
				}
				dirNode, err := uploadDirFn(fullPath, info.Name())
				if err != nil {
					return nil, err
				}
				txInfo.FileCount += 1
				txInfo.BytesTransferred += dirNode.GetDigest().GetSizeBytes()
				directory.Directories = append(directory.Directories, dirNode)
			} else if info.Mode().IsRegular() {
				if !dirHelper.ShouldBeUploaded(fqfn) {
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

	trees := make(map[string]*repb.Tree, 0)
	for _, outputDir := range dirHelper.outputDirs {
		trees[outputDir] = &repb.Tree{}
	}

	for _, f := range filesToUpload {
		if f.dir == nil {
			continue
		}
		fqfn := f.fullFilePath
		if tree, ok := trees[fqfn]; ok {
			tree.Root = f.dir
		} else {
			if treePath, ok := dirHelper.MatchesOutputDir(fqfn); ok {
				tree = trees[treePath]
				tree.Children = append(tree.Children, f.dir)
			}
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

type filePointer struct {
	// fullPath is the absolute path to the file, starting with "/" which refers
	// to the root directory of the local file system.
	// ex: /tmp/remote_execution/abc123/some/package/some_input.go
	fullPath string
	// relativePath is the workspace-relative path to the file.
	// ex: some/package/some_input.go
	relativePath string
	fileNode     *repb.FileNode
}

// removeExisting removes any existing file pointed to by the filePointer if
// it exists in the opts.Skip (pre-existing files) map. This is needed so that
// we overwrite existing files without silently dropping errors when linking
// the file. (Note, it is not needed when writing a fresh copy of the file.)
func removeExisting(fp *filePointer, opts *GetTreeOpts) error {
	if _, ok := opts.Skip[fp.relativePath]; ok {
		if err := os.Remove(fp.fullPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func writeFile(fp *filePointer, data []byte) error {
	var mode os.FileMode = 0644
	if fp.fileNode.IsExecutable {
		mode = 0755
	}
	f, err := os.OpenFile(fp.fullPath, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	//	defer log.Printf("Wrote %d bytes to file %q", len(data), filePath)
	return f.Close()
}

func copyFile(src *filePointer, dest *filePointer, opts *GetTreeOpts) error {
	if err := removeExisting(dest, opts); err != nil {
		return err
	}
	return fastcopy.FastCopy(src.fullPath, dest.fullPath)
}

// linkFileFromFileCache attempts to link the given file path from the local
// file cache, and returns whether the linking was successful.
func linkFileFromFileCache(d *repb.Digest, fp *filePointer, fc interfaces.FileCache, opts *GetTreeOpts) (bool, error) {
	if err := removeExisting(fp, opts); err != nil {
		return false, err
	}
	return fc.FastLinkFile(d, fp.fullPath), nil
}

// fileMap is a map of digests to file pointers containing the contents
// addressed by the digest.
type fileMap map[digest.Key][]*filePointer

func batchDownloadFiles(ctx context.Context, env environment.Env, req *repb.BatchReadBlobsRequest, filesToFetch fileMap, opts *GetTreeOpts) error {
	casClient := env.GetContentAddressableStorageClient()
	fc := env.GetFileCache()

	rsp, err := casClient.BatchReadBlobs(ctx, req)
	if err != nil {
		return err
	}
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
		ptr := ptrs[0]
		if err := writeFile(ptr, fileResponse.GetData()); err != nil {
			return err
		}
		if fc != nil {
			fc.AddFile(d, ptr.fullPath)
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

func fetchFiles(ctx context.Context, env environment.Env, instanceName string, filesToFetch fileMap, opts *GetTreeOpts) error {
	req := &repb.BatchReadBlobsRequest{
		InstanceName: instanceName,
	}
	currentBatchRequestSize := int64(0)
	eg, ctx := errgroup.WithContext(ctx)
	fc := env.GetFileCache()

	// Note: filesToFetch is keyed by digest, so all files in `filePointers` have
	// the digest represented by dk.
	for dk, filePointers := range filesToFetch {
		d := dk.ToDigest()

		// Write empty files directly (skip checking cache and downloading).
		if d.GetHash() == digest.EmptySha256 {
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
			d := fp.fileNode.GetDigest()
			if fc != nil {
				linked, err := linkFileFromFileCache(d, fp, fc, opts)
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
		if size > gRPCMaxSize {
			func(d *repb.Digest, fps []*filePointer) {
				eg.Go(func() error {
					return bytestreamReadFiles(ctx, env, instanceName, d, fps, opts)
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
					return batchDownloadFiles(ctx, env, req, filesToFetch, opts)
				})
			}(req)
			req = &repb.BatchReadBlobsRequest{
				InstanceName: instanceName,
			}
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
			return batchDownloadFiles(ctx, env, req, filesToFetch, opts)
		})
	}
	return eg.Wait()
}

// bytestreamReadFiles reads the given digest from the bytestream and creates
// files pointing to those contents.
func bytestreamReadFiles(ctx context.Context, env environment.Env, instanceName string, d *repb.Digest, fps []*filePointer, opts *GetTreeOpts) error {
	if len(fps) == 0 {
		return nil
	}
	fp := fps[0]
	var mode os.FileMode = 0644
	if fp.fileNode.IsExecutable {
		mode = 0755
	}
	f, err := os.OpenFile(fp.fullPath, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	if err := cachetools.GetBlob(ctx, env.GetByteStreamClient(), digest.NewInstanceNameDigest(fp.fileNode.Digest, instanceName), f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if fc := env.GetFileCache(); fc != nil {
		fc.AddFile(fp.fileNode.Digest, fp.fullPath)
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

func fetchDir(ctx context.Context, bsClient bspb.ByteStreamClient, reqDigest *digest.InstanceNameDigest) (*repb.Directory, error) {
	dir := &repb.Directory{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, reqDigest, dir); err != nil {
		return nil, err
	}
	return dir, nil
}

func dirMapFromTree(tree *repb.Tree) (rootDigest *repb.Digest, dirMap map[digest.Key]*repb.Directory, err error) {
	dirMap = make(map[digest.Key]*repb.Directory, 1+len(tree.Children))

	rootDigest, err = digest.ComputeForMessage(tree.Root)
	if err != nil {
		return nil, nil, err
	}
	dirMap[digest.NewKey(rootDigest)] = tree.Root

	for _, child := range tree.Children {
		d, err := digest.ComputeForMessage(child)
		if err != nil {
			return nil, nil, err
		}
		dirMap[digest.NewKey(d)] = child
	}

	return rootDigest, dirMap, nil
}

func dirMapFromRootDirectoryDigest(ctx context.Context, env environment.Env, d *digest.InstanceNameDigest) (map[digest.Key]*repb.Directory, error) {
	dirMap := make(map[digest.Key]*repb.Directory, 0)
	nextPageToken := ""
	for {
		stream, err := env.GetContentAddressableStorageClient().GetTree(ctx, &repb.GetTreeRequest{
			RootDigest:   d.Digest,
			InstanceName: d.GetInstanceName(),
			PageToken:    nextPageToken,
		})
		if err != nil {
			return nil, err
		}
		for {
			rsp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			nextPageToken = rsp.GetNextPageToken()
			for _, child := range rsp.GetDirectories() {
				d, err := digest.ComputeForMessage(child)
				if err != nil {
					return nil, err
				}
				dirMap[digest.NewKey(d)] = child
			}
		}
		if nextPageToken == "" {
			break
		}
	}
	return dirMap, nil
}

type GetTreeOpts struct {
	// Skip specifies file paths to skip, along with their digests. If the digest
	// of a file to be downloaded doesn't match the digest of the file in this
	// map, then it is re-downloaded (not skipped).
	Skip map[string]*repb.Digest
	// TrackTransfers specifies whether to record the full set of files downloaded
	// and return them in TransferInfo.Transfers.
	TrackTransfers bool
}

func GetTree(ctx context.Context, env environment.Env, instanceName string, tree *repb.Tree, rootDir string, opts *GetTreeOpts) (*TransferInfo, error) {
	txInfo := &TransferInfo{}
	startTime := time.Now()

	rootDirectoryDigest, dirMap, err := dirMapFromTree(tree)
	if err != nil {
		return nil, err
	}

	return getTree(ctx, env, instanceName, rootDirectoryDigest, rootDir, dirMap, startTime, txInfo, opts)
}

func GetTreeFromRootDirectoryDigest(ctx context.Context, env environment.Env, d *digest.InstanceNameDigest, rootDir string, opts *GetTreeOpts) (*TransferInfo, error) {
	txInfo := &TransferInfo{}
	startTime := time.Now()

	// IO: fetch the tree
	dirMap, err := dirMapFromRootDirectoryDigest(ctx, env, d)
	if err != nil {
		log.Debugf("Failed to fetch root directory tree: %s", err)
		if gstatus.Code(err) == codes.NotFound {
			return nil, digest.MissingDigestError(d.Digest)
		}
		return nil, err
	}

	return getTree(ctx, env, d.GetInstanceName(), d.Digest, rootDir, dirMap, startTime, txInfo, opts)
}

func getTree(ctx context.Context, env environment.Env, instanceName string, rootDirectoryDigest *repb.Digest, rootDir string, dirMap map[digest.Key]*repb.Directory, startTime time.Time, txInfo *TransferInfo, opts *GetTreeOpts) (*TransferInfo, error) {
	trackFn := func(relPath string, digest *repb.Digest) {}
	if opts.TrackTransfers {
		txInfo.Transfers = map[string]*repb.Digest{}
		trackFn = func(relPath string, digest *repb.Digest) {
			txInfo.Transfers[relPath] = digest
		}
	}

	filesToFetch := make(map[digest.Key][]*filePointer, 0)
	var fetchDirFn func(dir *repb.Directory, parentDir string) error
	fetchDirFn = func(dir *repb.Directory, parentDir string) error {
		for _, fileNode := range dir.GetFiles() {
			func(node *repb.FileNode, location string) {
				d := node.GetDigest()
				fullPath := filepath.Join(location, node.Name)
				relPath := trimPathPrefix(fullPath, rootDir)
				if skipDigest, ok := opts.Skip[relPath]; ok && digestsEqual(skipDigest, d) {
					return
				}
				dk := digest.NewKey(d)
				if _, ok := filesToFetch[dk]; !ok {
					// TODO: If the fetch is fulfilled via file cache, don't increment
					// this count.
					txInfo.BytesTransferred += d.GetSizeBytes()
				}
				filesToFetch[dk] = append(filesToFetch[dk], &filePointer{
					fileNode:     node,
					fullPath:     fullPath,
					relativePath: relPath,
				})
				txInfo.FileCount += 1
				trackFn(relPath, d)
			}(fileNode, parentDir)
		}
		for _, child := range dir.GetDirectories() {
			newRoot := filepath.Join(parentDir, child.GetName())
			if err := disk.EnsureDirectoryExists(newRoot); err != nil {
				return err
			}
			childDir, ok := dirMap[digest.NewKey(child.GetDigest())]
			if !ok {
				return digest.MissingDigestError(child.GetDigest())
			}
			fetchDirFn(childDir, newRoot)
		}
		for _, symlinkNode := range dir.GetSymlinks() {
			if err := os.Symlink(symlinkNode.GetTarget(), symlinkNode.GetName()); err != nil {
				return err
			}
		}
		return nil
	}
	// Create the directory structure and track files to download.
	if err := fetchDirFn(dirMap[digest.NewKey(rootDirectoryDigest)], rootDir); err != nil {
		return nil, err
	}
	// Download any files into the directory structure.
	if err := fetchFiles(ctx, env, instanceName, filesToFetch, opts); err != nil {
		return nil, err
	}
	endTime := time.Now()
	txInfo.TransferDuration = endTime.Sub(startTime)

	return txInfo, nil
}

func digestsEqual(a *repb.Digest, b *repb.Digest) bool {
	return a.GetHash() == b.GetHash() && a.GetSizeBytes() == b.GetSizeBytes()
}
