package cachetools

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	uploadBufSizeBytes = 1000000 // 1MB
	gRPCMaxSize        = int64(4000000)
)

// TODO(tylerw): This could probably go into util/ and be used by the BuildBuddy
// UI to introspect cache objects.

func GetBlob(ctx context.Context, bsClient bspb.ByteStreamClient, d *digest.InstanceNameDigest, out io.Writer) error {
	if bsClient == nil {
		return status.FailedPreconditionError("ByteStreamClient not configured")
	}
	if d.GetHash() == digest.EmptySha256 {
		return nil
	}
	req := &bspb.ReadRequest{
		ResourceName: digest.DownloadResourceName(d.Digest, d.GetInstanceName()),
		ReadOffset:   0,
		ReadLimit:    d.GetSizeBytes(),
	}
	stream, err := bsClient.Read(ctx, req)
	if err != nil {
		if gstatus.Code(err) == gcodes.NotFound {
			return digest.MissingDigestError(d.Digest)
		}
		return err
	}

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		out.Write(rsp.Data)
	}
	return nil
}

func ComputeDigest(in io.ReadSeeker, instanceName string) (*digest.InstanceNameDigest, error) {
	d, err := digest.Compute(in)
	if err != nil {
		return nil, err
	}
	return digest.NewInstanceNameDigest(d, instanceName), nil
}

func ComputeFileDigest(fullFilePath, instanceName string) (*digest.InstanceNameDigest, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ComputeDigest(f, instanceName)
}

func UploadFromReader(ctx context.Context, bsClient bspb.ByteStreamClient, ad *digest.InstanceNameDigest, in io.ReadSeeker) (*repb.Digest, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("ByteStreamClient not configured")
	}
	if ad.Digest.GetHash() == digest.EmptySha256 {
		return ad.Digest, nil
	}
	resourceName, err := digest.UploadResourceName(ad.Digest, ad.GetInstanceName())
	if err != nil {
		return nil, err
	}
	stream, err := bsClient.Write(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, uploadBufSizeBytes)
	bytesUploaded := int64(0)
	for {
		n, err := in.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		readDone := err == io.EOF

		req := &bspb.WriteRequest{
			ResourceName: resourceName,
			WriteOffset:  bytesUploaded,
			Data:         buf[:n],
			FinishWrite:  readDone,
		}
		if err := stream.Send(req); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		bytesUploaded += int64(n)
		if readDone {
			break
		}

	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	return ad.Digest, nil
}

func GetActionResult(ctx context.Context, acClient repb.ActionCacheClient, ad *digest.InstanceNameDigest) (*repb.ActionResult, error) {
	if acClient == nil {
		return nil, status.FailedPreconditionError("ActionCacheClient not configured")
	}
	req := &repb.GetActionResultRequest{
		ActionDigest: ad.Digest,
		InstanceName: ad.GetInstanceName(),
	}
	return acClient.GetActionResult(ctx, req)
}

func UploadActionResult(ctx context.Context, acClient repb.ActionCacheClient, ad *digest.InstanceNameDigest, ar *repb.ActionResult) error {
	if acClient == nil {
		return status.FailedPreconditionError("ActionCacheClient not configured")
	}
	req := &repb.UpdateActionResultRequest{
		InstanceName: ad.GetInstanceName(),
		ActionDigest: ad.Digest,
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
	ad, err := ComputeDigest(reader, instanceName)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return UploadFromReader(ctx, bsClient, ad, reader)
}

func UploadBlob(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, in io.ReadSeeker) (*repb.Digest, error) {
	ad, err := ComputeDigest(in, instanceName)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return UploadFromReader(ctx, bsClient, ad, in)
}

func UploadFile(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName, fullFilePath string) (*repb.Digest, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ad, err := ComputeDigest(f, instanceName)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return UploadFromReader(ctx, bsClient, ad, f)
}

func GetBlobAsProto(ctx context.Context, bsClient bspb.ByteStreamClient, d *digest.InstanceNameDigest, out proto.Message) error {
	buf := bytes.NewBuffer(make([]byte, 0, d.GetSizeBytes()))
	if err := GetBlob(ctx, bsClient, d, buf); err != nil {
		return err
	}
	return proto.Unmarshal(buf.Bytes(), out)
}

func readProtoFromCache(ctx context.Context, cache interfaces.Cache, d *digest.InstanceNameDigest, out proto.Message) error {
	data, err := cache.Get(ctx, d.Digest)
	if err != nil {
		if gstatus.Code(err) == gcodes.NotFound {
			return digest.MissingDigestError(d.Digest)
		}
		return err
	}
	return proto.Unmarshal([]byte(data), out)
}

func ReadProtoFromCAS(ctx context.Context, cache interfaces.Cache, d *digest.InstanceNameDigest, out proto.Message) error {
	cas := namespace.CASCache(cache, d.GetInstanceName())
	return readProtoFromCache(ctx, cas, d, out)
}

func ReadProtoFromAC(ctx context.Context, cache interfaces.Cache, d *digest.InstanceNameDigest, out proto.Message) error {
	ac := namespace.ActionCache(cache, d.GetInstanceName())
	return readProtoFromCache(ctx, ac, d, out)
}

func UploadBytesToCache(ctx context.Context, cache interfaces.Cache, in io.ReadSeeker) (*repb.Digest, error) {
	d, err := digest.Compute(in)
	if err != nil {
		return nil, err
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	wc, err := cache.Writer(ctx, d)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(wc, in)
	if err != nil {
		return nil, err
	}
	return d, wc.Close()
}

func UploadBytesToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, in io.ReadSeeker) (*repb.Digest, error) {
	cas := namespace.CASCache(cache, instanceName)
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
	cas := namespace.CASCache(cache, instanceName)
	return UploadBytesToCache(ctx, cas, reader)
}

func UploadProtoToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	cas := namespace.CASCache(cache, instanceName)
	return uploadProtoToCache(ctx, cas, instanceName, in)
}

// BatchCASUploader uploads many files to CAS concurrently, batching small
// uploads together and falling back to bytestream uploads for large files.
type BatchCASUploader struct {
	ctx              context.Context
	eg               *errgroup.Group
	cache            interfaces.Cache
	byteStreamClient bspb.ByteStreamClient
	casClient        repb.ContentAddressableStorageClient
	unsentBatchReq   *repb.BatchUpdateBlobsRequest
	unsentBatchSize  int64
	instanceName     string
	uploads          map[digest.Key]struct{}
}

// NewBatchCASUploader returns an uploader to be used only for the given request
// context (it should not be used outside the lifecycle of the request).
func NewBatchCASUploader(ctx context.Context, env environment.Env, instanceName string) (*BatchCASUploader, error) {
	bsClient := env.GetByteStreamClient()
	if bsClient == nil {
		return nil, status.InvalidArgumentError("Missing bytestream client")
	}
	casClient := env.GetContentAddressableStorageClient()
	if casClient == nil {
		return nil, status.InvalidArgumentError("Missing CAS client")
	}
	cache := env.GetCache()
	if cache == nil {
		return nil, status.InvalidArgumentError("Missing cache")
	}

	eg, ctx := errgroup.WithContext(ctx)
	return &BatchCASUploader{
		ctx:              ctx,
		eg:               eg,
		cache:            cache,
		byteStreamClient: bsClient,
		casClient:        casClient,
		unsentBatchReq:   &repb.BatchUpdateBlobsRequest{InstanceName: instanceName},
		unsentBatchSize:  0,
		instanceName:     instanceName,
		uploads:          make(map[digest.Key]struct{}),
	}, nil
}

// Upload adds the given content to the current batch or begins a streaming
// upload if it exceeds the maximum batch size. It closes r when it is no
// longer needed.
func (ul *BatchCASUploader) Upload(d *repb.Digest, r io.ReadSeekCloser) error {
	// De-dupe uploads by digest.
	dk := digest.NewKey(d)
	if _, ok := ul.uploads[dk]; ok {
		return r.Close()
	}
	ul.uploads[dk] = struct{}{}

	r.Seek(0, 0)

	if d.GetSizeBytes() > gRPCMaxSize {
		ul.eg.Go(func() error {
			defer r.Close()
			_, err := UploadFromReader(ul.ctx, ul.byteStreamClient, digest.NewInstanceNameDigest(d, ul.instanceName), r)
			return err
		})
		return nil
	}

	if ul.unsentBatchSize+d.GetSizeBytes() > gRPCMaxSize {
		ul.flushCurrentBatch()
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := r.Close(); err != nil {
		return err
	}
	ul.unsentBatchReq.Requests = append(ul.unsentBatchReq.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest: d,
		Data:   b,
	})
	ul.unsentBatchSize += d.GetSizeBytes()
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
	// Note: uploader.Upload will close the file.
	if err := ul.Upload(d, f); err != nil {
		return nil, err
	}
	return d, nil
}

func (ul *BatchCASUploader) flushCurrentBatch() {
	req := ul.unsentBatchReq
	ul.unsentBatchReq = &repb.BatchUpdateBlobsRequest{InstanceName: ul.instanceName}
	ul.unsentBatchSize = 0
	ul.eg.Go(func() error {
		rsp, err := ul.casClient.BatchUpdateBlobs(ul.ctx, req)
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
}

func (ul *BatchCASUploader) Wait() error {
	if len(ul.unsentBatchReq.GetRequests()) > 0 {
		ul.flushCurrentBatch()
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
func UploadDirectoryToCAS(ctx context.Context, env environment.Env, instanceName, rootDirPath string) (*repb.Digest, error) {
	ul, err := NewBatchCASUploader(ctx, env, instanceName)
	if err != nil {
		return nil, err
	}

	// Recursively find and upload all descendant dirs.
	visited, rootDirectoryDigest, err := uploadDir(ul, rootDirPath, nil /*=visited*/)
	if err != nil {
		return nil, err
	}
	if len(visited) == 0 {
		return nil, status.InternalError("empty directory list after uploading directory tree; this should never happen")
	}
	// Upload the tree, which consists of the root dir as well as all descendant
	// dirs.
	rootTree := &repb.Tree{Root: visited[0], Children: visited[1:]}
	if _, err := ul.UploadProto(rootTree); err != nil {
		return nil, err
	}
	if err := ul.Wait(); err != nil {
		return nil, err
	}
	return rootDirectoryDigest, nil
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
			d, err := ul.UploadFile(path)
			if err != nil {
				return nil, nil, err
			}
			info, err := entry.Info()
			dir.Files = append(dir.Files, &repb.FileNode{
				Name:         name,
				Digest:       d,
				IsExecutable: info.Mode()&0100 != 0,
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
	ac := namespace.ActionCache(cache, instanceName)
	return uploadProtoToCache(ctx, ac, instanceName, in)
}
