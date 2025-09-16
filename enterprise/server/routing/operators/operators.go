package operators

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const maxSizeForBatching = 1024 * 1024 * 2      // 2 MB, half of the typical max gRPC message size.
const maxSizeForBatchRequests = 1024 * 1024 * 4 // 4 MB, the typical max gRPC message size.

// Creates a composite operator that copies digests from one cache to another.
// The flow of this operator is as follows:
//
//  1. Check if the digests are actually missing from the target cache.
//  2. If so, schedule a copy--either by BatchReadBlobs/BatchUpdateBlobs (if the
//     file is small) or by ByteStream Read/Write (if the file is big).
//
// This allows us to group many small reads into a single BatchReadBlobs
// request (and avoid a bunch of round trips) and lets us keep small reads
// separate from streamed ByteStream copies.  Keeping the two in the same
// queue would make it harder to set limits for scheduling, since a large
// number of large reads could cause the queue to fill up.
func NewCopyOperator(env environment.Env) (batch_operator.BatchDigestOperator, error) {
	router := env.GetCacheRoutingService()
	if router == nil {
		return nil, status.FailedPreconditionError("No routing service registered!")
	}
	casOperator, err := NewCASBatchCopyOperator(env, router)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Failed to initialize CAS copy operator: %s", err.Error())
	}
	bsOperator, err := NewByteStreamCopyOperator(env, router)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Failed to initialize bytestream copy operator: %s", err.Error())
	}
	return NewFindMissingAndCopyOperator(env, router, casOperator, bsOperator)
}

func byteStreamCopy(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	primary, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}

	for _, d := range b.Digests {
		// XXX: compression?! This defaults to IDENTITY.
		r := digest.NewCASResourceName(d, b.InstanceName, b.DigestFunction)
		// XXX: Closing streams etc.
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
				break
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
	}
	return nil
}

func NewByteStreamCopyOperator(env environment.Env, router interfaces.CacheRoutingService) (batch_operator.BatchDigestOperator, error) {
	return batch_operator.New(
		env,
		"byte_stream_copy",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return byteStreamCopy(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			MaxDigestsPerBatch: 1,
			MaxBatchesPerGroup: 1000,
		},
	)
}

func casBatchCopy(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	primary, secondary, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	currentBatchSize := int64(0)
	batch := []*repb.Digest{}

	flush := func() error {
		currentBatchSize = 0
		// XXX: Compression?!
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

	for _, d := range b.Digests {
		if currentBatchSize+d.GetSizeBytes() >= maxSizeForBatchRequests {
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

func NewCASBatchCopyOperator(env environment.Env, router interfaces.CacheRoutingService) (batch_operator.BatchDigestOperator, error) {
	return batch_operator.New(
		env,
		"cas_batch_copy",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return casBatchCopy(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			MaxDigestsPerBatch: 1000,
			MaxBatchesPerGroup: 100,
		},
	)
}

func findMissingCheckAndDirect(ctx context.Context, router interfaces.CacheRoutingService, casOperator batch_operator.BatchDigestOperator, bsOperator batch_operator.BatchDigestOperator, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	_, secondary, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	res, err := secondary.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   b.InstanceName,
		DigestFunction: b.DigestFunction,
		BlobDigests:    b.Digests,
	})
	if err != nil {
		return err
	}

	smallStuff := []*repb.Digest{}
	bigStuff := []*repb.Digest{}
	for _, d := range res.GetMissingBlobDigests() {
		if d.GetSizeBytes() >= maxSizeForBatching {
			bigStuff = append(bigStuff, d)
		} else {
			smallStuff = append(smallStuff, d)
		}
	}

	if len(smallStuff) > 0 {
		// XXX: Log on enqueue failure.
		casOperator.Enqueue(ctx, b.InstanceName, smallStuff, b.DigestFunction)
	}
	if len(bigStuff) > 0 {
		// XXX: Log on enqueue failure.
		bsOperator.Enqueue(ctx, b.InstanceName, bigStuff, b.DigestFunction)
	}
	// XXX: Should this care about errors above?
	return nil
}

func NewFindMissingAndCopyOperator(env environment.Env, router interfaces.CacheRoutingService, casOperator batch_operator.BatchDigestOperator, bsOperator batch_operator.BatchDigestOperator) (batch_operator.BatchDigestOperator, error) {
	return batch_operator.New(
		env,
		"find_missing_and_copy",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return findMissingCheckAndDirect(ctx, router, casOperator, bsOperator, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			MaxDigestsPerBatch: 1000,
			MaxBatchesPerGroup: 100,
		},
	)
}

func findMissing(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	_, secondary, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	_, err = secondary.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   b.InstanceName,
		DigestFunction: b.DigestFunction,
		BlobDigests:    b.Digests,
	})
	return err
}

func NewFindMissingOperator(env environment.Env) (batch_operator.BatchDigestOperator, error) {
	router := env.GetCacheRoutingService()
	if router == nil {
		return nil, status.FailedPreconditionError("No routing service registered!")
	}
	return batch_operator.New(
		env,
		"find_missing",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return findMissing(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			MaxDigestsPerBatch: 1000,
			MaxBatchesPerGroup: 100,
		},
	)
}

func readAndVerifyCASBatch(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	ctx = usageutil.DisableUsageTracking(ctx)
	_, secondary, err := router.GetCASClients(ctx)
	if err != nil {
		return err
	}
	currentBatchSize := int64(0)
	batch := []*repb.Digest{}

	flush := func() error {
		currentBatchSize = 0
		// XXX: Compression?!
		data, err := secondary.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			Digests:        batch,
		})
		if err != nil {
			return err
		}
		for _, r := range data.GetResponses() {
			// Decompress (if needed) and verify
			if r.GetCompressor() == repb.Compressor_IDENTITY {
				if len(r.GetData()) != int(r.GetDigest().GetSizeBytes()) {
					// Uh.. shouldn't happen.
					// XXX: Log error.
					return nil
				}
			} else {
				// XXX: Log error.
				return nil
			}
		}
		return err
	}

	for _, d := range b.Digests {
		if currentBatchSize+d.GetSizeBytes() >= maxSizeForBatchRequests {
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

func readAndVerifyByteStream(ctx context.Context, router interfaces.CacheRoutingService, groupID string, b *batch_operator.DigestBatch) error {
	_, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}
	for _, d := range b.Digests {
		r := digest.NewCASResourceName(d, b.InstanceName, b.DigestFunction)
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
		if bytesRead != d.GetSizeBytes() {
			// XXX: Log ~!
			return nil
		}
	}
	return nil
}

type ReadOperator struct {
	bsOperator  batch_operator.BatchDigestOperator
	casOperator batch_operator.BatchDigestOperator
}

// Enqueue implements batch_operator.BatchDigestOperator.
func (r *ReadOperator) Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) bool {
	smallStuff := []*repb.Digest{}
	bigStuff := []*repb.Digest{}
	for _, d := range digests {
		if d.GetSizeBytes() >= maxSizeForBatching {
			bigStuff = append(bigStuff, d)
		} else {
			smallStuff = append(smallStuff, d)
		}
	}

	success := true
	if len(smallStuff) > 0 {
		// XXX: Log on enqueue failure.
		success = success && r.casOperator.Enqueue(ctx, instanceName, smallStuff, digestFunction)
	}
	if len(bigStuff) > 0 {
		// XXX: Log on enqueue failure.
		success = success && r.bsOperator.Enqueue(ctx, instanceName, bigStuff, digestFunction)
	}
	return success
}

// EnqueueByResourceName implements batch_operator.BatchDigestOperator.
func (r *ReadOperator) EnqueueByResourceName(ctx context.Context, rn *digest.CASResourceName) bool {
	if rn.GetDigest().GetSizeBytes() >= maxSizeForBatching {
		return r.bsOperator.EnqueueByResourceName(ctx, rn)
	} else {
		return r.casOperator.EnqueueByResourceName(ctx, rn)
	}
}

// ForceBatchingForTesting implements batch_operator.BatchDigestOperator.
func (r *ReadOperator) ForceBatchingForTesting() {
	r.bsOperator.ForceBatchingForTesting()
	r.casOperator.ForceBatchingForTesting()
}

// ForceFlushBatchesForTesting implements batch_operator.BatchDigestOperator.
func (r *ReadOperator) ForceFlushBatchesForTesting(ctx context.Context) {
	r.bsOperator.ForceFlushBatchesForTesting(ctx)
	r.casOperator.ForceFlushBatchesForTesting(ctx)
}

// ForceShutdownForTesting implements batch_operator.BatchDigestOperator.
func (r *ReadOperator) ForceShutdownForTesting() {
	r.bsOperator.ForceShutdownForTesting()
	r.casOperator.ForceShutdownForTesting()
}

func (r *ReadOperator) Start(hc interfaces.HealthChecker) {
	r.bsOperator.Start(hc)
	r.casOperator.Start(hc)
}

func NewReadOperator(env environment.Env) (batch_operator.BatchDigestOperator, error) {
	router := env.GetCacheRoutingService()
	if router == nil {
		return nil, status.FailedPreconditionError("No routing service registered!")
	}
	name := "cas_read_and_verify"
	casOperator, err := batch_operator.New(
		env,
		name,
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return readAndVerifyCASBatch(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			MaxDigestsPerBatch: 1000,
			MaxBatchesPerGroup: 100,
		},
	)
	if err != nil {
		return nil, status.InternalErrorf("Failed to initialize cas read operator: %s", err.Error())
	}
	bsOperator, err := batch_operator.New(
		env,
		name,
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return readAndVerifyByteStream(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			MaxDigestsPerBatch: 1000,
			MaxBatchesPerGroup: 100,
		},
	)
	if err != nil {
		return nil, status.InternalErrorf("Failed to initialize bytestream read operator: %s", err.Error())
	}

	return &ReadOperator{
		bsOperator:  bsOperator,
		casOperator: casOperator,
	}, nil
}
