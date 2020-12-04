package hit_tracker

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/prometheus/client_golang/prometheus"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type CacheMode int
type counterType int

const (
	// Prometheus CacheEventTypeLabel values

	hitLabel    = "hit"
	missLabel   = "miss"
	uploadLabel = "upload"

	// Prometheus CacheTypeLabel values

	actionCacheLabel = "action_cache"
	casLabel         = "cas"

	CAS         CacheMode = iota // CAS cache
	ActionCache                  // Action cache

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
	iid         string
	c           interfaces.MetricsCollector
	ctx         context.Context
	actionCache bool
}

func NewHitTracker(ctx context.Context, env environment.Env, actionCache bool) *HitTracker {
	return &HitTracker{
		c:           env.GetMetricsCollector(),
		ctx:         ctx,
		iid:         digest.GetInvocationIDFromMD(ctx),
		actionCache: actionCache,
	}
}

func (h *HitTracker) counterName(ct counterType) string {
	return counterName(h.actionCache, ct, h.iid)
}

func (h *HitTracker) cacheTypeLabel() string {
	if h.actionCache {
		return actionCacheLabel
	}
	return casLabel
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
	metrics.CacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      h.cacheTypeLabel(),
		metrics.CacheEventTypeLabel: missLabel,
	}).Inc()
	_, err := h.c.IncrementCount(h.ctx, h.counterName(Miss), 1)
	return err
}

func (h *HitTracker) TrackEmptyHit() error {
	metrics.CacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      h.cacheTypeLabel(),
		metrics.CacheEventTypeLabel: hitLabel,
	}).Inc()
	if h.c == nil || h.iid == "" {
		return nil
	}
	_, err := h.c.IncrementCount(h.ctx, h.counterName(Hit), 1)
	return err
}

type closeFunction func() error

type transferTimer struct {
	closeFn closeFunction
}

func (t *transferTimer) Close() error {
	return t.closeFn()
}

func cacheEventTypeLabel(c counterType) string {
	if c == Hit {
		return hitLabel
	}
	if c == Miss {
		return missLabel
	}
	return uploadLabel
}

func sizeMetric(ct counterType) *prometheus.HistogramVec {
	if ct == UploadSizeBytes {
		return metrics.CacheUploadSizeBytes
	}
	return metrics.CacheDownloadSizeBytes
}

func durationMetric(ct counterType) *prometheus.HistogramVec {
	if ct == UploadUsec {
		return metrics.CacheUploadDurationUsec
	}
	return metrics.CacheDownloadDurationUsec
}

func (h *HitTracker) makeCloseFunc(actionCache bool, d *repb.Digest, start time.Time, actionCounter, sizeCounter, timeCounter counterType) closeFunction {
	return func() error {
		dur := time.Since(start)

		et := cacheEventTypeLabel(actionCounter)
		ct := h.cacheTypeLabel()
		metrics.CacheEvents.With(prometheus.Labels{
			metrics.CacheTypeLabel:      ct,
			metrics.CacheEventTypeLabel: et,
		}).Inc()
		sizeMetric(sizeCounter).With(prometheus.Labels{
			metrics.CacheTypeLabel: ct,
		}).Observe(float64(d.GetSizeBytes()))
		sizeMetric(timeCounter).With(prometheus.Labels{
			metrics.CacheTypeLabel: ct,
		}).Observe(float64(dur.Microseconds()))

		if h.c == nil || h.iid == "" {
			return nil
		}

		if _, err := h.c.IncrementCount(h.ctx, h.counterName(actionCounter), 1); err != nil {
			return err
		}
		if _, err := h.c.IncrementCount(h.ctx, h.counterName(sizeCounter), d.GetSizeBytes()); err != nil {
			return err
		}
		if _, err := h.c.IncrementCount(h.ctx, h.counterName(timeCounter), dur.Microseconds()); err != nil {
			return err
		}

		return nil
	}
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
		closeFn: h.makeCloseFunc(false, d, start, Hit, DownloadSizeBytes, DownloadUsec),
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
		closeFn: h.makeCloseFunc(false, d, start, Upload, UploadSizeBytes, UploadUsec),
	}
}

func CollectCacheStats(ctx context.Context, env environment.Env, iid string) *capb.CacheStats {
	c := env.GetMetricsCollector()
	if c == nil || iid == "" {
		return nil
	}
	cs := &capb.CacheStats{}

	cs.ActionCacheHits, _ = c.ReadCount(ctx, counterName(true, Hit, iid))
	cs.ActionCacheMisses, _ = c.ReadCount(ctx, counterName(true, Miss, iid))
	cs.ActionCacheUploads, _ = c.ReadCount(ctx, counterName(true, Upload, iid))

	cs.CasCacheHits, _ = c.ReadCount(ctx, counterName(false, Hit, iid))
	cs.CasCacheMisses, _ = c.ReadCount(ctx, counterName(false, Miss, iid))
	cs.CasCacheUploads, _ = c.ReadCount(ctx, counterName(false, Upload, iid))

	cs.TotalDownloadSizeBytes, _ = c.ReadCount(ctx, counterName(false, DownloadSizeBytes, iid))
	cs.TotalUploadSizeBytes, _ = c.ReadCount(ctx, counterName(false, UploadSizeBytes, iid))
	cs.TotalDownloadUsec, _ = c.ReadCount(ctx, counterName(false, DownloadUsec, iid))
	cs.TotalUploadUsec, _ = c.ReadCount(ctx, counterName(false, UploadUsec, iid))

	cs.TotalCachedActionExecUsec, _ = c.ReadCount(ctx, counterName(false, CachedActionExecUsec, iid))

	return cs
}
