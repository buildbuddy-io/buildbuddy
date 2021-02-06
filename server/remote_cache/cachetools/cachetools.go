package cachetools

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/namespace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	uploadBufSizeBytes = 1000000 // 1MB
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
		req := &bspb.WriteRequest{
			ResourceName: resourceName,
			WriteOffset:  bytesUploaded,
			Data:         buf[:n],
			FinishWrite:  err == io.EOF,
		}
		err = stream.Send(req)
		bytesUploaded += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
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

func uploadBytesToCache(ctx context.Context, cache interfaces.Cache, in io.ReadSeeker) (*repb.Digest, error) {
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
	return d, err
}

func uploadProtoToCache(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)
	return uploadBytesToCache(ctx, cache, reader)
}

func UploadBlobToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, blob []byte) (*repb.Digest, error) {
	reader := bytes.NewReader(blob)
	cas := namespace.CASCache(cache, instanceName)
	return uploadBytesToCache(ctx, cas, reader)
}

func UploadProtoToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	cas := namespace.CASCache(cache, instanceName)
	return uploadProtoToCache(ctx, cas, instanceName, in)
}

func UploadProtoToAC(ctx context.Context, cache interfaces.Cache, instanceName string, in proto.Message) (*repb.Digest, error) {
	ac := namespace.ActionCache(cache, instanceName)
	return uploadProtoToCache(ctx, ac, instanceName, in)
}
