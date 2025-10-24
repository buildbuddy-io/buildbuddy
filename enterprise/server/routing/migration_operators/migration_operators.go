package migration_operators

import (
	"context"
	"io"
	"maps"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func findMissingInSecondary(ctx context.Context, router interfaces.CacheRoutingService, b *batch_operator.DigestBatch) ([]*repb.Digest, error) {
	_, cas, err := router.GetCASClients(ctx)
	if err != nil {
		return nil, err
	}
	res, err := cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   b.InstanceName,
		DigestFunction: b.DigestFunction,
		BlobDigests:    b.Digests,
	})
	if err != nil {
		return nil, err
	}

	return res.GetMissingBlobDigests(), nil
}

func ByteStreamCopy(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx, cancel := context.WithCancel(usageutil.DisableUsageTracking(ctx))
	defer cancel()

	missing, err := findMissingInSecondary(ctx, router, b)
	if err != nil {
		return err
	}

	primary, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}

	for _, d := range missing {
		if d.SizeBytes == 0 {
			log.CtxInfof(ctx, "Unexpected empty digest in Bytestream copy request: %s", d.GetHash())
			continue
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
	ctx, cancel := context.WithCancel(usageutil.DisableUsageTracking(ctx))
	defer cancel()
	_, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}
	anyErr := error(nil)
	for _, d := range b.Digests {
		r := digest.NewCASResourceName(d, b.InstanceName, b.DigestFunction)
		// TODO(jdhollen): Should we decompress client-side? we never intend to do this much regardless.
		if !verify {
			r.SetCompressor(repb.Compressor_ZSTD)
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

func CASBatchCopy(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	missing, err := findMissingInSecondary(ctx, router, b)
	if err != nil {
		return err
	}

	primary, secondary, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	currentBatchSize := int64(0)
	batch := []*repb.Digest{}

	flush := func() error {
		currentBatchSize = 0
		// TODO(jdhollen): this should use compression when possible.
		data, err := primary.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			Digests:        batch,
		})
		if err != nil {
			return err
		}
		reqs := make([]*repb.BatchUpdateBlobsRequest_Request, len(data.GetResponses()))
		for i, r := range data.GetResponses() {
			reqs[i] = &repb.BatchUpdateBlobsRequest_Request{Digest: r.GetDigest(), Data: r.GetData(), Compressor: r.GetCompressor()}
		}
		_, err = secondary.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{InstanceName: b.InstanceName, DigestFunction: b.DigestFunction, Requests: reqs})
		return err
	}

	for _, d := range missing {
		if currentBatchSize+d.GetSizeBytes() >= rpcutil.GRPCMaxSizeBytes {
			if err := flush(); err != nil {
				return err
			}
		}

		currentBatchSize += d.GetSizeBytes()
		batch = append(batch, d)
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	return nil
}

func CASBatchReadAndVerify(ctx context.Context, router interfaces.CacheRoutingService, verify bool, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	_, secondary, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	currentBatchSize := int64(0)
	batch := []*repb.Digest{}

	flush := func() error {
		currentBatchSize = 0
		// TODO(jdhollen): this should use compression and decompress after the fact.
		data, err := secondary.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			Digests:        batch,
		})
		if err != nil {
			return err
		}
		anyErr := error(nil)
		for _, r := range data.GetResponses() {
			if r.GetCompressor() == repb.Compressor_IDENTITY {
				if len(r.GetData()) != int(r.GetDigest().GetSizeBytes()) {
					anyErr = status.InternalErrorf("Size mismatch for digest %s: expected %d, got %d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes(), len(r.GetData()))
					log.CtxWarningf(ctx, "Read validation error for group %s: %s", groupID, anyErr)
					// Allow check to continue -- would be good to know if there are lots.
				}
			} else {
				// TODO(jdhollen): Decompress (if needed) and verify
				log.CtxWarningf(ctx, "Unexpected compressed blob in read validation.")
				return status.InternalErrorf("Unexpected compressed blob when performing read validation for %s", groupID)
			}
		}
		return anyErr
	}

	for _, d := range b.Digests {
		if currentBatchSize+d.GetSizeBytes() >= rpcutil.GRPCMaxSizeBytes {
			if err := flush(); err != nil {
				return err
			}
		}

		currentBatchSize += d.GetSizeBytes()
		batch = append(batch, d)
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	return nil
}

func RoutedCopy(ctx context.Context, groupID string, casCopy batch_operator.DigestOperator, bytestreamCopy batch_operator.DigestOperator, b *batch_operator.DigestBatch) error {
	smallStuff := []*repb.Digest{}
	bigStuff := []*repb.Digest{}
	for _, d := range b.Digests {
		if d.GetSizeBytes() >= cachetools.BatchUploadLimitBytes {
			bigStuff = append(bigStuff, d)
		} else {
			smallStuff = append(smallStuff, d)
		}
	}

	enqueueErr := error(nil)
	if len(smallStuff) > 0 {
		success := casCopy.Enqueue(ctx, b.InstanceName, smallStuff, b.DigestFunction)
		if !success {
			log.CtxWarningf(ctx, "Failed to enqueue cas sync operations for group %s", groupID)
			enqueueErr = status.ResourceExhaustedErrorf("Failed to enqueue CAS sync")
		}

	}
	if len(bigStuff) > 0 {
		success := bytestreamCopy.Enqueue(ctx, b.InstanceName, bigStuff, b.DigestFunction)
		if !success {
			log.CtxWarningf(ctx, "Failed to enqueue bytestream sync operations for group %s", groupID)
			enqueueErr = status.ResourceExhaustedErrorf("Failed to enqueue bytestream sync")
		}
	}
	return enqueueErr
}

func GetTreeMirrorOperator(ctx context.Context, router interfaces.CacheRoutingService, copyOperator batch_operator.DigestOperator, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	primary, _, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	var anyErr error = nil
	digestsToCheckAndCopy := map[digest.Key]*repb.Digest{}
	for _, d := range b.Digests {
		stream, err := primary.GetTree(ctx, &repb.GetTreeRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			RootDigest:     d,
		})
		if err != nil {
			log.CtxWarningf(ctx, "Tree copy error for group %s: %s", groupID, anyErr)
			// Allow check to continue -- would be good to know if there are lots.
		}
		for {
			rsp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.CtxWarningf(ctx, "Failed to stream tree for migration. digest: %v, instance_name: %s, digest_function: %s. Error: %s", d, b.InstanceName, b.DigestFunction, err)
				anyErr = err
				break
			}
			for _, dir := range rsp.GetDirectories() {
				for _, childDir := range dir.GetDirectories() {
					digestsToCheckAndCopy[digest.NewKey(childDir.GetDigest())] = childDir.GetDigest()
				}
				for _, childFile := range dir.GetFiles() {
					digestsToCheckAndCopy[digest.NewKey(childFile.GetDigest())] = childFile.GetDigest()
				}
			}
		}
	}

	// The copy operator will handle missing checks, so don't bother calling FindMissing here.
	digests := slices.Collect(maps.Values(digestsToCheckAndCopy))
	copyOperator.Enqueue(ctx, b.InstanceName, digests, b.DigestFunction)
	return anyErr
}
