package migration_operators

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func ByteStreamCopy(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx, cancel := context.WithCancel(usageutil.DisableUsageTracking(ctx))
	defer cancel()
	primary, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}

	for _, d := range b.Digests {
		if d.SizeBytes == 0 {
			continue;
		}
		// TODO(jdhollen): This should be using compression, when available.
		r := digest.NewCASResourceName(d, b.InstanceName, b.DigestFunction)
		readStream, err := primary.Read(ctx, &bspb.ReadRequest{ResourceName: r.DownloadString()})
		if err != nil {
			return err
		}
		writeClient, err := secondary.Write(ctx)
		if err != nil {
			return err
		}
		err = writeClient.Send(&bspb.WriteRequest{
			ResourceName: r.NewUploadString(),
			WriteOffset:  0,
		})
		if err != nil {
			return err
		}
		offset := int64(0)
		for {
			res, err := readStream.Recv()
			if err == io.EOF {
				if res != nil && len(res.Data) > 0 {
					writeClient.Send(&bspb.WriteRequest{
						Data:        res.Data,
						WriteOffset: offset,
					})
					offset += int64(len(res.Data))
				}
				break
			}
			if err != nil {
				return err
			}
			err = writeClient.Send(&bspb.WriteRequest{
				Data:        res.Data,
				WriteOffset: offset,
			})
			if err != nil {
				return err
			}
			offset += int64(len(res.Data))
		}
		writeClient.Send(&bspb.WriteRequest{
			FinishWrite: true,
			WriteOffset: offset,
		})
		if _, writeErr := writeClient.CloseAndRecv(); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func ByteStreamReadAndVerify(ctx context.Context, router interfaces.CacheRoutingService, verify bool, groupID string, b *batch_operator.DigestBatch) error {
	_, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}
	anyErr := error(nil)
	for _, d := range b.Digests {
		r := digest.NewCASResourceName(d, b.InstanceName, b.DigestFunction)
		// TODO(jdhollen): Should we decompress client-side? we never intend to do this much regardless.
		if !verify {
			r.SetCompressor(remote_execution.Compressor_ZSTD)
		}
		readStream, err := secondary.Read(ctx, &bspb.ReadRequest{ResourceName: r.DownloadString()})
		if err != nil {
			return err
		}
		bytesRead := int64(0)
		for {
			res, err := readStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			bytesRead += int64(len(res.Data))
		}
		if verify && bytesRead != d.GetSizeBytes() {
			anyErr = status.InternalErrorf("Size mismatch for digest %s: expected %d, got %d", d.GetHash(), d.GetSizeBytes(), bytesRead)
			log.CtxWarningf(ctx, "Read validation error for group %s: %s", groupID, anyErr)
		}
	}
	return anyErr
}
