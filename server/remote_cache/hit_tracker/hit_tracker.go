package hit_tracker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/gorm"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

type counterType int

const (
	Hit counterType = iota
	Miss
	Upload

	DownloadSizeBytes
	UploadSizeBytes

	DownloadUsec
	UploadUsec

	CachedActionExecUsec

	// New counter types go here!
)

func cacheTypePrefix(actionCache bool, name string) string {
	if actionCache {
		return "action-cache-" + name
	} else {
		return "cas-" + name
	}
}

func rawCounterName(actionCache bool, ct counterType) string {
	switch ct {
	case Hit:
		return cacheTypePrefix(actionCache, "hits")
	case Miss:
		return cacheTypePrefix(actionCache, "misses")
	case Upload:
		return cacheTypePrefix(actionCache, "uploads")
	case DownloadSizeBytes:
		return "download-size-bytes"
	case UploadSizeBytes:
		return "upload-size-bytes"
	case DownloadUsec:
		return "download-usec"
	case UploadUsec:
		return "upload-usec"
	case CachedActionExecUsec:
		return "cached-action-exec-usec"
	default:
		return "UNKNOWN-COUNTER-TYPE"
	}
}

func counterName(actionCache bool, ct counterType, iid string) string {
	return iid + "-" + rawCounterName(actionCache, ct)
}

type HitTracker struct {
	iid                string
	c                  interfaces.MetricsCollector
	ctx                context.Context
	env                environment.Env
	remoteInstanceName string
	saveActions        bool
}

func NewHitTracker(ctx context.Context, env environment.Env, remoteInstanceName string) *HitTracker {
	return &HitTracker{
		c:                  env.GetMetricsCollector(),
		ctx:                ctx,
		env:                env,
		remoteInstanceName: remoteInstanceName,
		iid:                digest.GetInvocationIDFromMD(ctx),
		saveActions:        digest.IsCacheDebuggingEnabled(ctx),
	}
}

func (h *HitTracker) getCounter(actionCache bool, ct counterType) string {
	return counterName(actionCache, ct, h.iid)
}

func (h *HitTracker) casCounterName(ct counterType) string {
	return counterName(false, ct, h.iid)
}

func (h *HitTracker) acCounterName(ct counterType) string {
	return counterName(true, ct, h.iid)
}

// Example Usage:
//
// ht := NewHitTracker(ctx, env, instanceName)
// if err := ht.TrackMiss(); err != nil {
//   log.Printf("Error counting cache miss.")
// }
func (h *HitTracker) TrackCASMiss(d *repb.Digest) error {
	if h.c == nil || h.iid == "" {
		return nil
	}
	_, err := h.c.IncrementCount(h.ctx, h.casCounterName(Miss), 1)
	return err
}

func (h *HitTracker) TrackEmptyCASHit() error {
	if h.c == nil || h.iid == "" {
		return nil
	}
	_, err := h.c.IncrementCount(h.ctx, h.casCounterName(Hit), 1)
	return err
}

type closeFunction func() error
type transferTimer struct {
	closeFn closeFunction
}

func (t *transferTimer) Close() error {
	return t.closeFn()
}

func (h *HitTracker) makeCloseFunc(actionCache bool, d *repb.Digest, start time.Time, actionCounter, sizeCounter, timeCounter counterType) closeFunction {
	return func() error {
		dur := time.Since(start)
		if h.c == nil || h.iid == "" {
			return nil
		}
		if _, err := h.c.IncrementCount(h.ctx, h.getCounter(actionCache, actionCounter), 1); err != nil {
			return err
		}
		if _, err := h.c.IncrementCount(h.ctx, h.getCounter(actionCache, sizeCounter), d.GetSizeBytes()); err != nil {
			return err
		}
		if _, err := h.c.IncrementCount(h.ctx, h.getCounter(actionCache, timeCounter), dur.Microseconds()); err != nil {
			return err
		}
		return nil
	}
}

// Example Usage:
//
// ht := NewHitTracker(ctx, env, instanceName)
// dlt := ht.TrackCASDownload(d)
// defer dlt.Close()
// ... body of download logic ...
func (h *HitTracker) TrackCASDownload(d *repb.Digest) *transferTimer {
	start := time.Now()
	return &transferTimer{
		closeFn: h.makeCloseFunc(false, d, start, Hit, DownloadSizeBytes, DownloadUsec),
	}
}

// Example Usage:
//
// ht := NewHitTracker(ctx, env, instanceName)
// ult := ht.TrackCASUpload(d)
// defer ult.Close()
// ... body of download logic ...
func (h *HitTracker) TrackCASUpload(d *repb.Digest) *transferTimer {
	start := time.Now()
	return &transferTimer{
		closeFn: h.makeCloseFunc(false, d, start, Upload, UploadSizeBytes, UploadUsec),
	}
}

type actionCloseFn func(actionResult *repb.ActionResult) error
type actionTimer struct {
	closeFn actionCloseFn
}

func (t *actionTimer) Close(actionResult *repb.ActionResult) error {
	return t.closeFn(actionResult)
}

func hashString(raw string) string {
	h := sha256.New()
	h.Write([]byte(raw))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func actionResultKey(actionResult *repb.ActionResult) string {
	for _, of := range actionResult.GetOutputFiles() {
		return hashString(of.GetPath())
	}
	for _, ofs := range actionResult.GetOutputFileSymlinks() {
		return hashString(ofs.GetPath())
	}
	for _, od := range actionResult.GetOutputDirectories() {
		return hashString(od.GetPath())
	}
	for _, ods := range actionResult.GetOutputDirectorySymlinks() {
		return hashString(ods.GetPath())
	}
	return "unknown"
}

func diffTimeProtos(startPb, endPb *tspb.Timestamp) time.Duration {
	start, _ := ptypes.Timestamp(startPb)
	end, _ := ptypes.Timestamp(endPb)
	return end.Sub(start)
}

// Example Usage:
//
// ht := NewHitTracker(ctx, env, instanceName)
// dlt := ht.TrackACDownload(d)
// defer dlt.Close(actionResult)
// ... body of download logic ...
func (h *HitTracker) TrackACDownload(d *repb.Digest) *actionTimer {
	start := time.Now()
	closeFn := func(actionResult *repb.ActionResult) error {
		if md := actionResult.GetExecutionMetadata(); md != nil {
			if h.c != nil && h.iid != "" {
				actionExecDuration := diffTimeProtos(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
				if _, err := h.c.IncrementCount(h.ctx, h.getCounter(false, CachedActionExecUsec), actionExecDuration.Microseconds()); err != nil {
					return err
				}
			}
		}
		if h.saveActions {
			dbHandle := h.env.GetDBHandle()
			if dbHandle == nil {
				return status.FailedPreconditionError("No database configured")
			}
			if err := h.env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
				return tx.Create(&tables.CacheLog{
					JoinKey:            actionResultKey(actionResult),
					InvocationID:       h.iid,
					RemoteInstanceName: h.remoteInstanceName,
					DigestHash:         fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes()),
				}).Error
			}); err != nil {
				return err
			}
		}
		return h.makeCloseFunc(true, d, start, Hit, DownloadSizeBytes, DownloadUsec)()
	}

	return &actionTimer{
		closeFn: closeFn,
	}
}

// Example Usage:
//
// ht := NewHitTracker(ctx, env, instanceName)
// ult := ht.TrackACUpload(d)
// defer ult.Close(actionResult)
// ... body of download logic ...
func (h *HitTracker) TrackACUpload(d *repb.Digest) *actionTimer {
	start := time.Now()
	closeFn := func(actionResult *repb.ActionResult) error {
		if h.saveActions {
			dbHandle := h.env.GetDBHandle()
			if dbHandle == nil {
				return status.FailedPreconditionError("No database configured")
			}
			blob, err := proto.Marshal(actionResult)
			if err != nil {
				return err
			}
			if err := h.env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
				return tx.Create(&tables.CacheLog{
					JoinKey:            actionResultKey(actionResult),
					InvocationID:       h.iid,
					RemoteInstanceName: h.remoteInstanceName,
					DigestHash:         fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes()),
					SerializedProto:    blob,
				}).Error
			}); err != nil {
				return err
			}
		}
		return h.makeCloseFunc(true, d, start, Upload, UploadSizeBytes, UploadUsec)()
	}

	return &actionTimer{
		closeFn: closeFn,
	}
}

func CollectCacheStats(ctx context.Context, env environment.Env, iid string) *capb.CacheStats {
	c := env.GetMetricsCollector()
	if c == nil || iid == "" {
		return nil
	}
	cs := &capb.CacheStats{}

	cs.ActionCacheHits, _ = c.Read(ctx, counterName(true, Hit, iid))
	cs.ActionCacheMisses, _ = c.Read(ctx, counterName(true, Miss, iid))
	cs.ActionCacheUploads, _ = c.Read(ctx, counterName(true, Upload, iid))

	cs.CasCacheHits, _ = c.Read(ctx, counterName(false, Hit, iid))
	cs.CasCacheMisses, _ = c.Read(ctx, counterName(false, Miss, iid))
	cs.CasCacheUploads, _ = c.Read(ctx, counterName(false, Upload, iid))

	cs.TotalDownloadSizeBytes, _ = c.Read(ctx, counterName(false, DownloadSizeBytes, iid))
	cs.TotalUploadSizeBytes, _ = c.Read(ctx, counterName(false, UploadSizeBytes, iid))
	cs.TotalDownloadUsec, _ = c.Read(ctx, counterName(false, DownloadUsec, iid))
	cs.TotalUploadUsec, _ = c.Read(ctx, counterName(false, UploadUsec, iid))

	cs.TotalCachedActionExecUsec, _ = c.Read(ctx, counterName(false, CachedActionExecUsec, iid))

	return cs
}
