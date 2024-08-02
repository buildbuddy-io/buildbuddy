package content_addressable_storage_server_proxy

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	remoteAccessTimeBatchUpdateSize      = flag.Int("cache_proxy.remote_atime_batch_size", 10*1000, "The maximum number of blob digests to send in a single request for updating blob access times in the remote cache.")
	remoteAccessTimeBatchUpdatesPerGroup = flag.Int("cache_proxy.remote_atime_batches_per_group", 100, "The maximum number of FindMissingBlobRequests accumulate per group for updating blob access times in the remote cache.")
	remoteAccessTimeBatchUpdateFrequency = flag.Duration("cache_proxy.remote_atime_update_frequency", 1*time.Minute, "The frequency with which to update blob access times in the remote cache.")
)

// Store the FindMissingBlobsRequest as a struct so we can de-dupe digests.
type accessTimeUpdate struct {
	instanceName   string
	digests        map[digest.Key]struct{}
	digestFunction repb.DigestFunction_Value
}

func fromProto(proto *repb.FindMissingBlobsRequest) *accessTimeUpdate {
	digests := make(map[digest.Key]struct{})
	for _, protoDigest := range proto.BlobDigests {
		digests[digest.NewKey(protoDigest)] = struct{}{}
	}
	return &accessTimeUpdate{
		instanceName:   proto.InstanceName,
		digests:        digests,
		digestFunction: proto.DigestFunction,
	}
}

func (u *accessTimeUpdate) toProto() *repb.FindMissingBlobsRequest {
	return &repb.FindMissingBlobsRequest{
		InstanceName:   u.instanceName,
		DigestFunction: u.digestFunction,
	}
}

// Returns true if the two provided accessTimeUpdates can be merged by
// appending their digests fields.
func mergeable(u1 *accessTimeUpdate, u2 *accessTimeUpdate) bool {
	return u1.instanceName == u2.instanceName && u1.digestFunction == u2.digestFunction
}

// Merges the 'src' accessTimeUpdate into the 'dest' accessTimeUpdate.
func merge(dest *accessTimeUpdate, src *accessTimeUpdate) {
	for digest := range src.digests {
		dest.digests[digest] = struct{}{}
	}
}

// The set of pending access time updates to send per group.
type accessTimeUpdates struct {
	mu   sync.Mutex // Protects jwt and reqs
	jwt  *string
	reqs []*accessTimeUpdate
}

type accessTimeUpdater struct {
	authenticator interfaces.Authenticator

	// Pending access time updates, keyed by group ID.
	updates      map[string]*accessTimeUpdates
	remote       repb.ContentAddressableStorageClient
	shutDownChan chan struct{}
}

func (u *accessTimeUpdater) groupID(ctx context.Context) string {
	user, err := u.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (u *accessTimeUpdater) enqueueAccessTimeUpdates(ctx context.Context, req *repb.FindMissingBlobsRequest) {
	groupID := u.groupID(ctx)
	jwt := u.authenticator.TrustedJWTFromAuthContext(ctx)
	updates, ok := u.updates[groupID]
	if !ok {
		updates := &accessTimeUpdates{}
		u.updates[groupID] = updates
	}
	// Always use the most recent JWT for a group for remote atime updates.
	updates.jwt = &jwt
	updates.mu.Lock()
	defer updates.mu.Unlock()

	newUpdate := fromProto(req)

	// Search the pending updates for one this request can be batched with.
	for _, update := range updates.reqs {
		if mergeable(update, newUpdate) {

			// Ensure these requests don't get too large. Note that this isn't
			// exact, but it's not super important.
			if len(update.digests) < *remoteAccessTimeBatchUpdateSize {
				merge(update, newUpdate)
			} else {
				log.CtxWarningf(ctx, "FindMissingBlobsRequest for remote atime updates for group %s overflowed, creating another.", groupID)
				// Keep going in case there's another batchable request later.
			}
		}
	}

	if len(updates.reqs) > *remoteAccessTimeBatchUpdatesPerGroup {
		log.CtxInfof(ctx, "Too many pending FindMissingBlobsRequests for updating remote atimes for group %s, dropping one.", groupID)
	} else {
		updates.reqs = append(updates.reqs, newUpdate)
	}
}

func (u *accessTimeUpdater) processAccessTimeUpdates() {
	ticker := time.NewTicker(*remoteAccessTimeBatchUpdateFrequency)
	defer ticker.Stop()
	for {
		select {
		case <-u.shutDownChan:
			return
		case <-ticker.C:
			// Send one batch of remote atime updates per group each tick.
			for groupID, updates := range u.updates {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				u.updateAccessTimes(ctx, groupID, updates)
				cancel()
			}
		}
	}
}

func (u *accessTimeUpdater) updateAccessTimes(ctx context.Context, groupID string, updates *accessTimeUpdates) {
	updates.mu.Lock()
	if len(updates.reqs) < 1 {
		updates.mu.Unlock()
		return
	}

	req := updates.reqs[0].toProto()
	if len(updates.reqs) == 1 {
		updates.reqs = []*accessTimeUpdate{}
	} else {
		updates.reqs = updates.reqs[1:]
	}
	updates.mu.Unlock()

	log.CtxDebugf(ctx, "Asynchronously processing %d atime updates for group %s", len(req.BlobDigests), groupID)

	// TODO(iain): check if group is propagated (it should be in the JWT).
	ctx = u.authenticator.AuthContextFromTrustedJWT(ctx, *updates.jwt)
	if _, err := u.remote.FindMissingBlobs(ctx, req); err != nil {
		log.CtxWarningf(ctx, "Error sending FindMissingBlobs request to update remote atimes for group %s: %s", groupID, err)
	}
}

type CASServerProxy struct {
	env    environment.Env
	local  repb.ContentAddressableStorageClient
	remote repb.ContentAddressableStorageClient

	accessTimeUpdater accessTimeUpdater
}

func Register(env *real_environment.RealEnv) error {
	casServer, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageServerProxy: %s", err)
	}
	env.SetCASServer(casServer)
	return nil
}

func New(env environment.Env) (*CASServerProxy, error) {
	local := env.GetLocalCASClient()
	if local == nil {
		return nil, fmt.Errorf("A local ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	remote := env.GetContentAddressableStorageClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	proxy := CASServerProxy{
		env:    env,
		local:  local,
		remote: remote,
		accessTimeUpdater: accessTimeUpdater{
			authenticator: env.GetAuthenticator(),
			updates:       make(map[string]*accessTimeUpdates),
			remote:        remote,
			shutDownChan:  make(chan struct{}),
		},
	}
	// TODO(iain): halt this on server stop.
	go proxy.accessTimeUpdater.processAccessTimeUpdates()
	return &proxy, nil
}

func (s *CASServerProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	s.accessTimeUpdater.enqueueAccessTimeUpdates(ctx, req)
	resp, err := s.local.FindMissingBlobs(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(resp.MissingBlobDigests) == 0 {
		return resp, nil
	}
	remoteReq := repb.FindMissingBlobsRequest{
		InstanceName:   req.InstanceName,
		BlobDigests:    resp.MissingBlobDigests,
		DigestFunction: req.DigestFunction,
	}
	return s.remote.FindMissingBlobs(ctx, &remoteReq)
}

func (s *CASServerProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	_, err := s.local.BatchUpdateBlobs(context.Background(), req)
	if err != nil {
		log.Warningf("Local BatchUpdateBlobs error: %s", err)
	}
	return s.remote.BatchUpdateBlobs(ctx, req)
}

func (s *CASServerProxy) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	mergedResp := repb.BatchReadBlobsResponse{}
	mergedDigests := []*repb.Digest{}
	localResp, err := s.local.BatchReadBlobs(ctx, req)
	if err != nil {
		return s.batchReadBlobsRemote(ctx, req)
	}
	for _, resp := range localResp.Responses {
		if resp.Status.Code == int32(codes.OK) {
			mergedResp.Responses = append(mergedResp.Responses, resp)
			mergedDigests = append(mergedDigests, resp.Digest)
		}
	}
	if len(mergedResp.Responses) == len(req.Digests) {
		return &mergedResp, nil
	}
	_, missing := digest.Diff(req.Digests, mergedDigests)
	remoteReq := repb.BatchReadBlobsRequest{
		InstanceName:          req.InstanceName,
		Digests:               missing,
		AcceptableCompressors: req.AcceptableCompressors,
		DigestFunction:        req.DigestFunction,
	}
	remoteResp, err := s.batchReadBlobsRemote(ctx, &remoteReq)
	if err != nil {
		return nil, err
	}
	mergedResp.Responses = append(mergedResp.Responses, remoteResp.Responses...)
	return &mergedResp, nil
}

func (s *CASServerProxy) batchReadBlobsRemote(ctx context.Context, readReq *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	readResp, err := s.remote.BatchReadBlobs(ctx, readReq)
	if err != nil {
		return nil, err
	}
	updateReq := repb.BatchUpdateBlobsRequest{
		InstanceName:   readReq.InstanceName,
		DigestFunction: readReq.DigestFunction,
	}
	for _, response := range readResp.Responses {
		if response.Status.Code != int32(codes.OK) {
			continue
		}
		updateReq.Requests = append(updateReq.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     response.Digest,
			Data:       response.Data,
			Compressor: response.Compressor,
		})
	}
	if _, err := s.local.BatchUpdateBlobs(context.Background(), &updateReq); err != nil {
		log.Warningf("Error locally updating blobs: %s", err)
	}
	return readResp, nil
}

func (s *CASServerProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	// TODO(iain): cache these
	remoteStream, err := s.remote.GetTree(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}
