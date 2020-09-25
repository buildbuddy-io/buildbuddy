package hit_tracker

import (
	"context"
	"log"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type CacheMode int
type counterType int

const (
	CAS         CacheMode = iota // CAS cache
	ActionCache                  // Action cache

	Hit counterType = iota
	Miss
	Upload

	DownloadSizeBytes
	UploadSizeBytes

	DownloadUsec
	UploadUsec

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
	default:
		return "UNKNOWN-COUNTER-TYPE"
	}
}

func counterName(actionCache bool, ct counterType, iid string) string {
	return iid + "-" + rawCounterName(actionCache, ct)
}

type HitTracker struct {
	iid         string
	c           interfaces.Counter
	ctx         context.Context
	actionCache bool
}

func NewHitTracker(ctx context.Context, env environment.Env, actionCache bool) *HitTracker {
	ht := &HitTracker{
		c:           env.GetCounter(),
		ctx:         ctx,
		iid:         digest.GetInvocationIDFromMD(ctx),
		actionCache: actionCache,
	}
	if ht.iid == "" {
		log.Printf("HitTracker created with empty invocationID. Is header set?")
	}
	return ht
}

func (h *HitTracker) counterName(ct counterType) string {
	return counterName(h.actionCache, ct, h.iid)
}

// Example Usage:
//
// ht := NewHitTracker(env, invocationID, false /*=actionCache*/)
// if err := ht.TrackMiss(); err != nil {
//   log.Printf("Error counting cache miss.")
// }
func (h *HitTracker) TrackMiss(d *repb.Digest) error {
	if h.c == nil || h.iid == "" {
		return nil
	}
	_, err := h.c.Increment(h.ctx, h.counterName(Miss), 1)
	return err
}

func (h *HitTracker) TrackEmptyHit() error {
	if h.c == nil || h.iid == "" {
		return nil
	}
	_, err := h.c.Increment(h.ctx, h.counterName(Hit), 1)
	return err
}

type transferTimer struct {
	closeFunc func() error
}

func (t *transferTimer) Close() error {
	return t.closeFunc()
}

// Example Usage:
//
// ht := NewHitTracker(env, invocationID, false /*=actionCache*/)
// dlt := ht.TrackDownload(d)
// defer dlt.Close()
// ... body of download logic ...
func (h *HitTracker) TrackDownload(d *repb.Digest) *transferTimer {
	start := time.Now()
	return &transferTimer{
		closeFunc: func() error {
			if h.c == nil || h.iid == "" {
				return nil
			}
			end := time.Now()
			if _, err := h.c.Increment(h.ctx, h.counterName(Hit), 1); err != nil {
				return err
			}
			if _, err := h.c.Increment(h.ctx, h.counterName(DownloadSizeBytes), d.GetSizeBytes()); err != nil {
				return err
			}
			if _, err := h.c.Increment(h.ctx, h.counterName(DownloadUsec), end.Sub(start).Microseconds()); err != nil {
				return err
			}
			return nil
		},
	}
}

// Example Usage:
//
// ht := NewHitTracker(env, invocationID, false /*=actionCache*/)
// ult := ht.TrackUpload(d)
// defer ult.Close()
// ... body of download logic ...
func (h *HitTracker) TrackUpload(d *repb.Digest) *transferTimer {
	start := time.Now()
	return &transferTimer{
		closeFunc: func() error {
			if h.c == nil || h.iid == "" {
				return nil
			}
			end := time.Now()
			if _, err := h.c.Increment(h.ctx, h.counterName(Upload), 1); err != nil {
				return err
			}
			if _, err := h.c.Increment(h.ctx, h.counterName(UploadSizeBytes), d.GetSizeBytes()); err != nil {
				return err
			}
			if _, err := h.c.Increment(h.ctx, h.counterName(UploadUsec), end.Sub(start).Microseconds()); err != nil {
				return err
			}
			return nil
		},
	}
}

func CollectCacheStats(ctx context.Context, env environment.Env, iid string) *capb.CacheStats {
	c := env.GetCounter()
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

	return cs
}
