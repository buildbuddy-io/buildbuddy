package routing_byte_stream_client

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	bytestreamMigrationOpTimeout = flag.Duration("cache_proxy.migration.bytestream_timeout", 60*time.Second, "Timeout for bytestream operations that happen during cache migrations.")
)

type RoutingByteStreamClient struct {
	acClient     map[string]bspb.ByteStreamClient
	router       interfaces.CacheRoutingService
	copyOp       interfaces.DigestOperator
	readOp       interfaces.DigestOperator
	readVerifyOp interfaces.DigestOperator
}

func New(env environment.Env) (bspb.ByteStreamClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	bytestreamCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamCopy(ctx, env.GetCacheRoutingService(), groupID, b)
	}
	bytestreamReadClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamReadAndVerify(ctx, env.GetCacheRoutingService(), false, groupID, b)
	}
	bytestreamVerifyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamReadAndVerify(ctx, env.GetCacheRoutingService(), true, groupID, b)
	}
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionError("An authenticator is required to route and migrate between caches.")
	}
	copyOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-copy", bytestreamCopyClosure, *bytestreamMigrationOpTimeout)
	readOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-read", bytestreamReadClosure, *bytestreamMigrationOpTimeout)
	readVerifyOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-read-verify", bytestreamVerifyClosure, *bytestreamMigrationOpTimeout)

	return &RoutingByteStreamClient{
		router:       routingService,
		copyOp:       copyOp,
		readOp:       readOp,
		readVerifyOp: readVerifyOp,
	}, nil
}

func (r *RoutingByteStreamClient) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest, opts ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	primaryClient, _, err := r.router.GetBSClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.QueryWriteStatus(ctx, req, opts...)
}

func (r *RoutingByteStreamClient) Read(ctx context.Context, req *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	primaryClient, _, err := r.router.GetBSClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.Read(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	singleRandValue := rand.Float64()
	if singleRandValue < c.GetBackgroundReadVerifyFraction() {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.readVerifyOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	} else if singleRandValue < c.GetBackgroundReadFraction() {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.readOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	} else if (singleRandValue) < c.GetBackgroundCopyFraction() {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.copyOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	}
	return rsp, nil
}

type wrappedWriteStream struct {
	ctx context.Context
	op  interfaces.DigestOperator
	rn  *digest.CASResourceName
	bspb.ByteStream_WriteClient
	success bool
}

func (w *wrappedWriteStream) Send(req *bspb.WriteRequest) error {
	if req.GetResourceName() != "" {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			w.rn = rn
		}
	}

	err := w.ByteStream_WriteClient.Send(req)
	if err == nil && req.GetFinishWrite() && w.rn != nil {
		w.success = true
	}
	return err
}

func (w *wrappedWriteStream) CloseAndRecv() (*bspb.WriteResponse, error) {
	resp, err := w.ByteStream_WriteClient.CloseAndRecv()
	if w.success && err == nil && w.rn != nil {
		w.op.EnqueueByResourceName(w.ctx, w.rn)
	}
	return resp, err
}

func (r *RoutingByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	primaryClient, secondaryClient, err := r.router.GetBSClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return primaryClient.Write(ctx, opts...)
	}

	singleRandValue := rand.Float64()
	dualWrite := singleRandValue < c.GetDualWriteFraction()
	if dualWrite && secondaryClient != nil {
		return newDualWriteClient(ctx, primaryClient, secondaryClient, opts...)
	}

	rsp, err := primaryClient.Write(ctx, opts...)
	if err != nil {
		return rsp, err
	}

	if (singleRandValue) < c.GetBackgroundCopyFraction() {
		return &wrappedWriteStream{
			ctx:                    ctx,
			op:                     r.copyOp,
			ByteStream_WriteClient: rsp,
		}, nil
	}
	return rsp, nil
}

func newDualWriteClient(ctx context.Context, primary bspb.ByteStreamClient, secondary bspb.ByteStreamClient, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	primaryStream, err := primary.Write(ctx, opts...)
	if err != nil {
		return nil, err
	}
	secondaryCtx, secondaryCancel := background.ExtendContextForFinalization(ctx, 5*time.Second)
	secondaryCtx = usageutil.DisableUsageTracking(secondaryCtx)
	secondaryStream, err := secondary.Write(secondaryCtx, opts...)
	if err != nil {
		log.CtxWarningf(secondaryCtx, "Failed to start secondary write, falling back to primary: %s", err)
		secondaryCancel()
		return primaryStream, nil
	}

	stream := &dualWriteStream{
		ByteStream_WriteClient: primaryStream,
		secondary:              secondaryStream,
		buffer:                 make(chan *clientOp, 5),
		secondaryCtx:           secondaryCtx,
		secondaryCancel:        secondaryCancel,
	}

	go stream.processSecondary()
	return stream, nil
}

type streamOp int

const (
	send streamOp = iota
	closeSend
	closeAndRecv
)

type clientOp struct {
	op  streamOp
	req *bspb.WriteRequest
}

type dualWriteStream struct {
	bspb.ByteStream_WriteClient
	secondary       bspb.ByteStream_WriteClient
	buffer          chan *clientOp
	secondaryCtx    context.Context
	secondaryCancel context.CancelFunc
}

func (d *dualWriteStream) Send(req *bspb.WriteRequest) error {
	err := d.ByteStream_WriteClient.Send(req)
	if err != nil {
		d.secondaryCancel()
		close(d.buffer)
		return err
	}
	opToQueue := &clientOp{
		op:  send,
		req: req,
	}
	select {
	case d.buffer <- opToQueue:
	case <-d.secondaryCtx.Done():
	}

	return nil
}

func (d *dualWriteStream) CloseAndRecv() (*bspb.WriteResponse, error) {
	go func() {
		// Either the secondary write will finish, or the extended context
		// will time out.  Either way, we don't want to block the primary
		// response here.
		opToQueue := &clientOp{
			op: closeAndRecv,
		}
		select {
		case d.buffer <- opToQueue:
		case <-d.secondaryCtx.Done():
		}
	}()
	return d.ByteStream_WriteClient.CloseAndRecv()
}

func (d *dualWriteStream) processSecondary() {
	defer d.secondaryCancel()

	for {
		select {
		case op, ok := <-d.buffer:
			if !ok {
				return
			}

			switch op.op {
			case send:
				if err := d.secondary.Send(op.req); err != nil {
					log.CtxWarningf(d.secondaryCtx, "Secondary stream failed: %s", err)
					metrics.ProxySecondarySyncWriteDigests.WithLabelValues(
						"bs_server",
						gstatus.Code(err).String(),
					).Add(1)
					return
				}
			case closeSend:
				err := d.secondary.CloseSend()
				if err != nil {
					log.CtxWarningf(d.secondaryCtx, "Secondary stream failed: %s", err)
				}
				metrics.ProxySecondarySyncWriteDigests.WithLabelValues(
					"bs_server",
					gstatus.Code(err).String(),
				).Add(1)
				return
			case closeAndRecv:
				_, err := d.secondary.CloseAndRecv()
				if err != nil {
					log.CtxWarningf(d.secondaryCtx, "Secondary stream failed: %s", err)
				}
				metrics.ProxySecondarySyncWriteDigests.WithLabelValues(
					"bs_server",
					gstatus.Code(err).String(),
				).Add(1)
				return
			}
		case <-d.secondaryCtx.Done():
			log.CtxWarningf(d.secondaryCtx, "Secondary stream context closed early: %s", d.secondaryCtx.Err())
			metrics.ProxySecondarySyncWriteDigests.WithLabelValues(
				"bs_server",
				gstatus.Code(d.secondaryCtx.Err()).String(),
			).Add(1)
			return
		}
	}
}

func (d *dualWriteStream) CloseSend() error {
	go func() {
		// Either the secondary write will finish, or the extended context
		// will time out.  Either way, we don't want to block the primary
		// response here.
		opToQueue := &clientOp{
			op: closeSend,
		}
		select {
		case d.buffer <- opToQueue:
		case <-d.secondaryCtx.Done():
		}
	}()
	return d.ByteStream_WriteClient.CloseSend()
}
