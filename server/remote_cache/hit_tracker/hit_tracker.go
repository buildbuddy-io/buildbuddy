package hit_tracker

import (
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

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
)

type HitTracker struct {
	iid         string
	c           interfaces.Counter
	actionCache bool
}

func NewHitTracker(env environment.Env, invocationID string, actionCache bool) *HitTracker {
	return &HitTracker{
		iid:         invocationID,
		c:           env.GetCounter(),
		actionCache: actionCache,
	}
}

func (h *HitTracker) cacheTypePrefix(name string) string {
	if h.actionCache {
		return "action-cache-" + name
	} else {
		return "cas-" + name
	}
}

func (h *HitTracker) counterName(ct counterType) string {
	switch ct {
	case Hit:
		return h.cacheTypePrefix("hits")
	case Miss:
		return h.cacheTypePrefix("misses")
	case Upload:
		return h.cacheTypePrefix("uploads")
	case DownloadSizeBytes:
		return "download-size-bytes"
	case UploadSizeBytes:
		return "upload-size-bytes"
	case DownloadUsec:
		return "download-usec"
	case UploadUsec:
		return "upload-usec"
	default:
		return "UNKNWON-COUNTER-TYPE"
	}
}

func (h *HitTracker) TrackMiss(d *repb.Digest) error {
	if h.c == nil {
		return nil
	}
	_, err := h.c.Increment(h.counterName(Miss), 1)
	return err
}

type transferTimer struct {
	closeFunc func()
}

func (t *transferTimer) Finish() {
	t.closeFunc()
}

func (h *HitTracker) TrackDownload(d *repb.Digest) *transferTimer {
	start := time.Now()
	return &transferTimer{
		closeFunc: func() {
			end := time.Now()
			h.c.Increment(h.counterName(Hit), 1)
			h.c.Increment(h.counterName(DownloadSizeBytes), d.GetSizeBytes())
			h.c.Increment(h.counterName(DownloadUsec), end.Sub(start).Microseconds())
		},
	}
}

func (h *HitTracker) TrackUpload(d *repb.Digest) *transferTimer {
	start := time.Now()
	return &transferTimer{
		closeFunc: func() {
			end := time.Now()
			h.c.Increment(h.counterName(Upload), 1)
			h.c.Increment(h.counterName(UploadSizeBytes), d.GetSizeBytes())
			h.c.Increment(h.counterName(UploadUsec), end.Sub(start).Microseconds())
		},
	}
}
