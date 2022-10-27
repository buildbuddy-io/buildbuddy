package cachetools

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	uploadBufSizeBytes    = 1000000 // 1MB
	gRPCMaxSize           = int64(4000000)
	maxCompressionBufSize = int64(4000000)
)

var (
	enableUploadCompresssion = flag.Bool("cache.client.enable_upload_compression", false, "If true, enable compression of uploads to remote caches")
)

type StreamBlobOpts struct {
	Offset int64
	Limit  int64
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

func GetBlobChunk(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, opts *StreamBlobOpts, out io.Writer) (int64, error) {
	if bsClient == nil {
		return 0, status.FailedPreconditionError("ByteStreamClient not configured")
	}
	if r.GetDigest().GetHash() == digest.EmptySha256 {
		return 0, nil
	}

	req := &bspb.ReadRequest{
		ResourceName: r.DownloadString(),
		ReadOffset:   opts.Offset,
		ReadLimit:    opts.Limit,
	}
	stream, err := bsClient.Read(ctx, req)
	if err != nil {
		if gstatus.Code(err) == gcodes.NotFound {
			return 0, digest.MissingDigestError(r.GetDigest())
		}
		return 0, err
	}

	var wc io.WriteCloser = nopCloser{out}
	if r.GetCompressor() == repb.Compressor_ZSTD {
		decompressor, err := compression.NewZstdDecompressor(out)
		if err != nil {
			return 0, err
		}
		wc = decompressor
	}

	written := int64(0)
	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			if err := wc.Close(); err != nil {
				return 0, err
			}
			break
		}
		if err != nil {
			return 0, err
		}
		n, err := wc.Write(rsp.Data)
		if err != nil {
			return 0, err
		}
		written += int64(n)
	}
	return written, nil
}

// TODO(vadim): return # of bytes written for consistency with GetBlobChunk
func GetBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, out io.Writer) error {
	_, err := GetBlobChunk(ctx, bsClient, r, &StreamBlobOpts{Offset: 0, Limit: 0}, out)
	return err
}

func ComputeDigest(in io.ReadSeeker, instanceName string) (*digest.ResourceName, error) {
	d, err := digest.Compute(in)
	if err != nil {
		return nil, err
	}
	return digest.NewResourceName(d, instanceName), nil
}

func ComputeFileDigest(fullFilePath, instanceName string) (*digest.ResourceName, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ComputeDigest(f, instanceName)
}

func UploadFromReader(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, in io.Reader) (*repb.Digest, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("ByteStreamClient not configured")
	}
	if r.GetDigest().GetHash() == digest.EmptySha256 {
		return r.GetDigest(), nil
	}
	resourceName, err := r.UploadString()
	if err != nil {
		return nil, err
	}
	stream, err := bsClient.Write(ctx)
	if err != nil {
		return nil, err
	}

	var rc io.ReadCloser = io.NopCloser(in)
	if r.GetCompressor() == repb.Compressor_ZSTD {
		rbuf := make([]byte, 0, uploadBufSizeBytes)
		cbuf := make([]byte, 0, uploadBufSizeBytes)
		reader, err := compression.NewZstdChunkingCompressor(in, rbuf[:uploadBufSizeBytes], cbuf[:uploadBufSizeBytes])
		if err != nil {
			return nil, status.InternalErrorf("Failed to compress blob: %s", err)
		}
		rc = reader
	}
	defer rc.Close()

	buf := make([]byte, uploadBufSizeBytes)
	bytesUploaded := int64(0)
	for {
		n, err := rc.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		readDone := err == io.EOF

		req := &bspb.WriteRequest{
			Data:         buf[:n],
			ResourceName: resourceName,
			WriteOffset:  bytesUploaded,
			FinishWrite:  readDone,
		}
		if err := stream.Send(req); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		bytesUploaded += int64(len(req.Data))
		if readDone {
			break
		}

	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	return r.GetDigest(), nil
}

func GetActionResult(ctx context.Context, acClient repb.ActionCacheClient, ar *digest.ResourceName) (*repb.ActionResult, error) {
	if acClient == nil {
		return nil, status.FailedPreconditionError("ActionCacheClient not configured")
	}
	req := &repb.GetActionResultRequest{
		ActionDigest: ar.GetDigest(),
		InstanceName: ar.GetInstanceName(),
	}
	return acClient.GetActionResult(ctx, req)
}

func UploadActionResult(ctx context.Context, acClient repb.ActionCacheClient, r *digest.ResourceName, ar *repb.ActionResult) error {
	if acClient == nil {
		return status.FailedPreconditionError("ActionCacheClient not configured")
	}
	req := &repb.UpdateActionResultRequest{
		InstanceName: r.GetInstanceName(),
		ActionDigest: r.GetDigest(),
		ActionResult: ar,
	}
	_, err := acClient.UpdateActionResult(ctx, req)
	return err
}

func UploadProto(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)
	resourceName, err := ComputeDigest(reader, instanceName)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return UploadFromReader(ctx, bsClient, resourceName, reader)
}

func UploadBlob(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, in io.ReadSeeker) (*repb.Digest, error) {
	resourceName, err := ComputeDigest(in, instanceName)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return UploadFromReader(ctx, bsClient, resourceName, in)
}

func UploadFile(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName, fullFilePath string) (*repb.Digest, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	resourceName, err := ComputeDigest(f, instanceName)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return UploadFromReader(ctx, bsClient, resourceName, f)
}

func GetBlobAsProto(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.ResourceName, out proto.Message) error {
	buf := bytes.NewBuffer(make([]byte, 0, r.GetDigest().GetSizeBytes()))
	if err := GetBlob(ctx, bsClient, r, buf); err != nil {
		return err
	}
	return proto.Unmarshal(buf.Bytes(), out)
}

func GetActionAndCommand(ctx context.Context, bsClient bspb.ByteStreamClient, actionDigest *digest.ResourceName) (*repb.Action, *repb.Command, error) {
	action := &repb.Action{}
	if err := GetBlobAsProto(ctx, bsClient, actionDigest, action); err != nil {
		return nil, nil, status.WrapErrorf(err, "could not fetch action")
	}
	cmd := &repb.Command{}
	if err := GetBlobAsProto(ctx, bsClient, digest.NewResourceName(action.GetCommandDigest(), actionDigest.GetInstanceName()), cmd); err != nil {
		return nil, nil, status.WrapErrorf(err, "could not fetch command")
	}
	return action, cmd, nil
}

func readProtoFromCache(ctx context.Context, cache interfaces.Cache, r *digest.ResourceName, out proto.Message) error {
	data, err := cache.GetDeprecated(ctx, r.GetDigest())
	if err != nil {
		if gstatus.Code(err) == gcodes.NotFound {
			return digest.MissingDigestError(r.GetDigest())
		}
		return err
	}
	return proto.Unmarshal([]byte(data), out)
}

func ReadProtoFromCAS(ctx context.Context, cache interfaces.Cache, d *digest.ResourceName, out proto.Message) error {
	cas, err := namespace.CASCache(ctx, cache, d.GetInstanceName())
	if err != nil {
		return err
	}
	return readProtoFromCache(ctx, cas, d, out)
}

func ReadProtoFromAC(ctx context.Context, cache interfaces.Cache, d *digest.ResourceName, out proto.Message) error {
	ac, err := namespace.ActionCache(ctx, cache, d.GetInstanceName())
	if err != nil {
		return err
	}
	return readProtoFromCache(ctx, ac, d, out)
}

func UploadBytesToCache(ctx context.Context, cache interfaces.Cache, in io.ReadSeeker) (*repb.Digest, error) {
	d, err := digest.Compute(in)
	if err != nil {
		return nil, err
	}
	if d.GetHash() == digest.EmptySha256 {
		return d, nil
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	wc, err := cache.WriterDeprecated(ctx, d)
	if err != nil {
		return nil, err
	}
	defer wc.Close()
	_, err = io.Copy(wc, in)
	if err != nil {
		return nil, err
	}
	return d, wc.Commit()
}

func UploadBytesToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, in io.ReadSeeker) (*repb.Digest, error) {
	cas, err := namespace.CASCache(ctx, cache, instanceName)
	if err != nil {
		return nil, err
	}
	return UploadBytesToCache(ctx, cas, in)
}

func uploadProtoToCache(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)
	return UploadBytesToCache(ctx, cache, reader)
}

func UploadBlobToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, blob []byte) (*repb.Digest, error) {
	reader := bytes.NewReader(blob)
	cas, err := namespace.CASCache(ctx, cache, instanceName)
	if err != nil {
		return nil, err
	}
	return UploadBytesToCache(ctx, cas, reader)
}

func UploadProtoToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	cas, err := namespace.CASCache(ctx, cache, instanceName)
	if err != nil {
		return nil, err
	}
	return uploadProtoToCache(ctx, cas, instanceName, in)
}

func SupportsCompression(ctx context.Context, capabilitiesClient repb.CapabilitiesClient) (bool, error) {
	rsp, err := capabilitiesClient.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	if err != nil {
		return false, err
	}
	supportsBytestreamCompression := false
	for _, compressorValue := range rsp.GetCacheCapabilities().GetSupportedCompressors() {
		if compressorValue == repb.Compressor_ZSTD {
			supportsBytestreamCompression = true
			break
		}
	}

	supportsBatchUpdateCompression := false
	for _, compressorValue := range rsp.GetCacheCapabilities().GetSupportedBatchUpdateCompressors() {
		if compressorValue == repb.Compressor_ZSTD {
			supportsBatchUpdateCompression = true
			break
		}
	}
	return supportsBytestreamCompression && supportsBatchUpdateCompression, nil
}

// BatchCASUploader uploads many files to CAS concurrently, batching small
// uploads together and falling back to bytestream uploads for large files.
type BatchCASUploader struct {
	ctx             context.Context
	env             environment.Env
	once            *sync.Once
	compress        bool
	eg              *errgroup.Group
	bufferPool      *bytebufferpool.Pool
	unsentBatchReq  *repb.BatchUpdateBlobsRequest
	uploads         map[digest.Key]struct{}
	instanceName    string
	unsentBatchSize int64
}

// NewBatchCASUploader returns an uploader to be used only for the given request
// context (it should not be used outside the lifecycle of the request).
func NewBatchCASUploader(ctx context.Context, env environment.Env, instanceName string) *BatchCASUploader {
	eg, ctx := errgroup.WithContext(ctx)
	return &BatchCASUploader{
		ctx:             ctx,
		env:             env,
		once:            &sync.Once{},
		compress:        false,
		eg:              eg,
		bufferPool:      bytebufferpool.New(uploadBufSizeBytes),
		unsentBatchReq:  &repb.BatchUpdateBlobsRequest{InstanceName: instanceName},
		unsentBatchSize: 0,
		instanceName:    instanceName,
		uploads:         make(map[digest.Key]struct{}),
	}
}

func (ul *BatchCASUploader) supportsCompression() bool {
	ul.once.Do(func() {
		if !*enableUploadCompresssion {
			return
		}
		capabilitiesClient := ul.env.GetCapabilitiesClient()
		if capabilitiesClient == nil {
			log.Warningf("Upload compression was enabled but no capabilities client found. Cannot verify cache server capabilities")
			return
		}
		enabled, err := SupportsCompression(ul.ctx, capabilitiesClient)
		if err != nil {
			log.Errorf("Error determinining if cache server supports compression: %s", err)
		}
		if enabled {
			ul.compress = true
		} else {
			log.Debugf("Upload compression was enabled but remote server did not support compression")
		}
	})
	return ul.compress
}

// Upload adds the given content to the current batch or begins a streaming
// upload if it exceeds the maximum batch size. It closes r when it is no
// longer needed.
func (ul *BatchCASUploader) Upload(d *repb.Digest, rsc io.ReadSeekCloser) error {
	// De-dupe uploads by digest.
	dk := digest.NewKey(d)
	if _, ok := ul.uploads[dk]; ok {
		return rsc.Close()
	}
	ul.uploads[dk] = struct{}{}

	rsc.Seek(0, 0)
	r := io.ReadCloser(rsc)

	compressor := repb.Compressor_IDENTITY
	if ul.supportsCompression() {
		compressor = repb.Compressor_ZSTD
	}

	if d.GetSizeBytes() > gRPCMaxSize {
		resourceName := digest.NewResourceName(d, ul.instanceName)
		resourceName.SetCompressor(compressor)

		byteStreamClient := ul.env.GetByteStreamClient()
		if byteStreamClient == nil {
			return status.InvalidArgumentError("missing bytestream client")
		}
		ul.eg.Go(func() error {
			defer r.Close()
			_, err := UploadFromReader(ul.ctx, byteStreamClient, resourceName, r)
			return err
		})
		return nil
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := r.Close(); err != nil {
		return err
	}

	if compressor == repb.Compressor_ZSTD {
		b = compression.CompressZstd(nil, b)
	}
	additionalSize := int64(len(b))
	if ul.unsentBatchSize+additionalSize > gRPCMaxSize {
		ul.flushCurrentBatch()
	}
	ul.unsentBatchReq.Requests = append(ul.unsentBatchReq.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest:     d,
		Data:       b,
		Compressor: compressor,
	})
	ul.unsentBatchSize += additionalSize
	return nil
}

func (ul *BatchCASUploader) UploadProto(in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if err := ul.Upload(d, NewBytesReadSeekCloser(data)); err != nil {
		return nil, err
	}
	return d, nil
}

func (ul *BatchCASUploader) UploadFile(path string) (*repb.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(f)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	// Add output files to the filecache.
	if ul.env.GetFileCache() != nil {
		ul.env.GetFileCache().AddFile(&repb.FileNode{Digest: d, IsExecutable: isExecutable(info)}, path)
	}

	// Note: uploader.Upload will close the file.
	if err := ul.Upload(d, f); err != nil {
		return nil, err
	}
	return d, nil
}

func (ul *BatchCASUploader) flushCurrentBatch() error {
	casClient := ul.env.GetContentAddressableStorageClient()
	if casClient == nil {
		return status.InvalidArgumentError("missing CAS client")
	}

	req := ul.unsentBatchReq
	ul.unsentBatchReq = &repb.BatchUpdateBlobsRequest{InstanceName: ul.instanceName}
	ul.unsentBatchSize = 0
	ul.eg.Go(func() error {
		rsp, err := casClient.BatchUpdateBlobs(ul.ctx, req)
		if err != nil {
			return err
		}
		for _, fileResponse := range rsp.GetResponses() {
			if fileResponse.GetStatus().GetCode() != int32(codes.OK) {
				return gstatus.Error(codes.Code(fileResponse.GetStatus().GetCode()), fmt.Sprintf("Error uploading file: %v", fileResponse.GetDigest()))
			}
		}
		return nil
	})
	return nil
}

func (ul *BatchCASUploader) Wait() error {
	if len(ul.unsentBatchReq.GetRequests()) > 0 {
		if err := ul.flushCurrentBatch(); err != nil {
			return err
		}
	}
	return ul.eg.Wait()
}

type bytesReadSeekCloser struct {
	io.ReadSeeker
}

func NewBytesReadSeekCloser(b []byte) io.ReadSeekCloser {
	return &bytesReadSeekCloser{bytes.NewReader(b)}
}
func (*bytesReadSeekCloser) Close() error { return nil }

// UploadDirectoryToCAS uploads all the files in a given directory to the CAS
// as well as the directory structure, and returns the digest of the root
// Directory proto that can be used to fetch the uploaded contents.
func UploadDirectoryToCAS(ctx context.Context, env environment.Env, instanceName, rootDirPath string) (*repb.Digest, *repb.Digest, error) {
	ul := NewBatchCASUploader(ctx, env, instanceName)

	// Recursively find and upload all descendant dirs.
	visited, rootDirectoryDigest, err := uploadDir(ul, rootDirPath, nil /*=visited*/)
	if err != nil {
		return nil, nil, err
	}
	if len(visited) == 0 {
		return nil, nil, status.InternalError("empty directory list after uploading directory tree; this should never happen")
	}
	// Upload the tree, which consists of the root dir as well as all descendant
	// dirs.
	rootTree := &repb.Tree{Root: visited[0], Children: visited[1:]}
	treeDigest, err := ul.UploadProto(rootTree)
	if err != nil {
		return nil, nil, err
	}
	if err := ul.Wait(); err != nil {
		return nil, nil, err
	}
	return rootDirectoryDigest, treeDigest, nil
}

func uploadDir(ul *BatchCASUploader, dirPath string, visited []*repb.Directory) ([]*repb.Directory, *repb.Digest, error) {
	dir := &repb.Directory{}
	// Append the directory before doing any other work, so that the root
	// directory is located at visited[0] at the end of recursion.
	visited = append(visited, dir)
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(dirPath, name)

		if entry.IsDir() {
			var d *repb.Digest
			visited, d, err = uploadDir(ul, path, visited)
			if err != nil {
				return nil, nil, err
			}
			dir.Directories = append(dir.Directories, &repb.DirectoryNode{
				Name:   name,
				Digest: d,
			})
		} else if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return nil, nil, err
			}
			d, err := ul.UploadFile(path)
			if err != nil {
				return nil, nil, err
			}
			dir.Files = append(dir.Files, &repb.FileNode{
				Name:         name,
				Digest:       d,
				IsExecutable: isExecutable(info),
			})
		} else if entry.Type()&os.ModeSymlink == os.ModeSymlink {
			target, err := os.Readlink(path)
			if err != nil {
				return nil, nil, err
			}
			dir.Symlinks = append(dir.Symlinks, &repb.SymlinkNode{
				Name:   name,
				Target: target,
			})
		}
	}
	digest, err := ul.UploadProto(dir)
	if err != nil {
		return nil, nil, err
	}
	return visited, digest, nil
}

func UploadProtoToAC(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	ac, err := namespace.ActionCache(ctx, cache, instanceName)
	if err != nil {
		return nil, err
	}
	return uploadProtoToCache(ctx, ac, instanceName, in)
}

func isExecutable(info os.FileInfo) bool {
	return info.Mode()&0100 != 0
}

func GetTreeFromRootDirectoryDigest(ctx context.Context, casClient repb.ContentAddressableStorageClient, r *digest.ResourceName) (*repb.Tree, error) {
	var dirs []*repb.Directory
	nextPageToken := ""
	for {
		stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
			RootDigest:   r.GetDigest(),
			InstanceName: r.GetInstanceName(),
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
				dirs = append(dirs, child)
			}
		}
		if nextPageToken == "" {
			break
		}
	}

	if len(dirs) == 0 {
		return &repb.Tree{Root: &repb.Directory{}}, nil
	}

	return &repb.Tree{
		Root:     dirs[0],
		Children: dirs[1:],
	}, nil
}
