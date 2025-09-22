package operators

import (
	"context"
	"io"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const maxSizeForBatching = 1024 * 1024 * 2      // 2 MB, half of the typical max gRPC message size.
const maxSizeForBatchRequests = 1024 * 1024 * 4 // 4 MB, the typical max gRPC message size.

var (
	migrationFMQueueSize        = flag.Int("cache_proxy.migration.findmissing_queue_size", 1_000_000, "The length of the holding queue from which we create AC migration batches (shared across all group IDs).")
	migrationFMDigestsPerBatch  = flag.Int("cache_proxy.migration.findmissing_digests_per_batch", 10_000, "The number of digests to flush out to the AC migration operator in a single batch, per (group ID, instance name) tuple.")
	migrationFMBatchesPerGroup  = flag.Int("cache_proxy.migration.findmissing_batches_per_group", 50, "The number of batches to hold for the AC migration operator, per (group ID, instance name) tuple.")
	migrationFMDigestsPerGroup  = flag.Int("cache_proxy.migration.findmissing_digests_per_group", 200_000, "The number of digests that we're willing to hold in batches per Group ID (across all instance names).")
	migrationFMBatchInterval    = flag.Duration("cache_proxy.migration.cas_batch_interval", 10*time.Second, "The time interval to wait between flushing AC batches, one batch (group ID, instance name) tuple per flush.")
	migrationCASQueueSize       = flag.Int("cache_proxy.migration.cas_queue_size", 1_000_000, "The length of the holding queue from which we create AC migration batches (shared across all group IDs).")
	migrationCASDigestsPerBatch = flag.Int("cache_proxy.migration.cas_digests_per_batch", 1_000, "The number of digests to flush out to the AC migration operator in a single batch, per (group ID, instance name) tuple.")
	migrationCASBatchesPerGroup = flag.Int("cache_proxy.migration.cas_batches_per_group", 500, "The number of batches to hold for the AC migration operator, per (group ID, instance name) tuple.")
	migrationCASDigestsPerGroup = flag.Int("cache_proxy.migration.cas_digests_per_group", 500_000, "The number of digests that we're willing to hold in batches per Group ID (across all instance names).")
	migrationCASBatchInterval   = flag.Duration("cache_proxy.migration.cas_batch_interval", 10*time.Second, "The time interval to wait between flushing AC batches, one batch (group ID, instance name) tuple per flush.")
	migrationBSQueueSize        = flag.Int("cache_proxy.migration.bs_queue_size", 1_000_000, "The length of the holding queue from which we create AC migration batches (shared across all group IDs).")
	migrationBSDigestsPerBatch  = flag.Int("cache_proxy.migration.bs_digests_per_batch", 100, "The number of digests to flush out to the AC migration operator in a single batch, per (group ID, instance name) tuple.")
	migrationBSBatchesPerGroup  = flag.Int("cache_proxy.migration.bs_batches_per_group", 10_000, "The number of batches to hold for the AC migration operator, per (group ID, instance name) tuple.")
	migrationBSDigestsPerGroup  = flag.Int("cache_proxy.migration.bs_digests_per_group", 30_000, "The number of digests that we're willing to hold in batches per Group ID (across all instance names).")
	migrationBSBatchInterval    = flag.Duration("cache_proxy.migration.bs_batch_interval", 10*time.Millisecond, "The time interval to wait between flushing bytestream batches, one batch (group ID, instance name) tuple per flush.")
)

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
	ctx, cancel := context.WithCancel(usageutil.DisableUsageTracking(ctx))
	defer cancel()
	primary, secondary, err := router.GetBSClients(ctx)
	if err != nil {
		return err
	}

	for _, d := range b.Digests {
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

func NewByteStreamCopyOperator(env environment.Env, router interfaces.CacheRoutingService) (batch_operator.BatchDigestOperator, error) {
	return batch_operator.New(
		env,
		"byte_stream_copy",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return byteStreamCopy(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			QueueSize:          *migrationBSQueueSize,
			BatchInterval:      *migrationBSBatchInterval,
			MaxDigestsPerGroup: *migrationBSDigestsPerGroup,
			MaxDigestsPerBatch: *migrationBSDigestsPerBatch,
			MaxBatchesPerGroup: *migrationBSBatchesPerGroup,
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
			QueueSize:          *migrationCASQueueSize,
			BatchInterval:      *migrationCASBatchInterval,
			MaxDigestsPerGroup: *migrationCASDigestsPerGroup,
			MaxDigestsPerBatch: *migrationCASDigestsPerBatch,
			MaxBatchesPerGroup: *migrationCASBatchesPerGroup,
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

	enqueueErr := error(nil)
	if len(smallStuff) > 0 {
		success := casOperator.Enqueue(ctx, b.InstanceName, smallStuff, b.DigestFunction)
		if !success {
			log.CtxWarningf(ctx, "Failed to enqueue cas sync operations for group %s", groupID)
			enqueueErr = status.ResourceExhaustedErrorf("Failed to enqueue CAS sync")
		}

	}
	if len(bigStuff) > 0 {
		success := bsOperator.Enqueue(ctx, b.InstanceName, bigStuff, b.DigestFunction)
		if !success {
			log.CtxWarningf(ctx, "Failed to enqueue bytestream sync operations for group %s", groupID)
			enqueueErr = status.ResourceExhaustedErrorf("Failed to enqueue bytestream sync")
		}
	}
	return enqueueErr
}

func NewFindMissingAndCopyOperator(env environment.Env, router interfaces.CacheRoutingService, casOperator batch_operator.BatchDigestOperator, bsOperator batch_operator.BatchDigestOperator) (batch_operator.BatchDigestOperator, error) {
	return batch_operator.New(
		env,
		"find_missing_and_copy",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return findMissingCheckAndDirect(ctx, router, casOperator, bsOperator, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			QueueSize:          *migrationFMQueueSize,
			BatchInterval:      *migrationFMBatchInterval,
			MaxDigestsPerGroup: *migrationFMDigestsPerGroup,
			MaxDigestsPerBatch: *migrationFMDigestsPerBatch,
			MaxBatchesPerGroup: *migrationFMBatchesPerGroup,
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
			QueueSize:          *migrationFMQueueSize,
			BatchInterval:      *migrationFMBatchInterval,
			MaxDigestsPerGroup: *migrationFMDigestsPerGroup,
			MaxDigestsPerBatch: *migrationFMDigestsPerBatch,
			MaxBatchesPerGroup: *migrationFMBatchesPerGroup,
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
	anyErr := error(nil)
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
			anyErr = status.InternalErrorf("Size mismatch for digest %s: expected %d, got %d", d.GetHash(), d.GetSizeBytes(), bytesRead)
			log.CtxWarningf(ctx, "Read validation error for group %s: %s", groupID, anyErr)
		}
	}
	return anyErr
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
		success = success && r.casOperator.Enqueue(ctx, instanceName, smallStuff, digestFunction)
	}
	if len(bigStuff) > 0 {
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
	casOperator, err := batch_operator.New(
		env,
		"cas_read_and_verify",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return readAndVerifyCASBatch(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			QueueSize:          *migrationCASQueueSize,
			BatchInterval:      *migrationCASBatchInterval,
			MaxDigestsPerGroup: *migrationCASDigestsPerGroup,
			MaxDigestsPerBatch: *migrationCASDigestsPerBatch,
			MaxBatchesPerGroup: *migrationCASBatchesPerGroup,
		},
	)
	if err != nil {
		return nil, status.InternalErrorf("Failed to initialize cas read operator: %s", err.Error())
	}
	bsOperator, err := batch_operator.New(
		env,
		"byte_stream_read_and_verify",
		func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
			return readAndVerifyByteStream(ctx, router, groupID, b)
		},
		batch_operator.BatchDigestOperatorConfig{
			QueueSize:          *migrationBSQueueSize,
			BatchInterval:      *migrationBSBatchInterval,
			MaxDigestsPerGroup: *migrationBSDigestsPerGroup,
			MaxDigestsPerBatch: *migrationBSDigestsPerBatch,
			MaxBatchesPerGroup: *migrationBSBatchesPerGroup,
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
