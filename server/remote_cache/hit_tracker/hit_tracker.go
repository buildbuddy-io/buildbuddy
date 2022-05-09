package hit_tracker

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

var (
	detailedStatsEnabled = flag.Bool("cache.detailed_stats_enabled", false, "Whether to enable detailed stats recording for all cache requests.")

	// Example: "GoLink(//merger:merger_test)/16f1152b7b260f690ea06f8b938a1b60712b5ee41a1c125ecad8ed9416481fbb"
	actionRegexp = regexp.MustCompile("^(?P<action_mnemonic>[[:alnum:]]*)\\((?P<target_id>.+)\\)/(?P<action_id>[[:alnum:]]+)$")
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
	DownloadTransferredSizeBytes
	UploadTransferredSizeBytes

	DownloadUsec
	UploadUsec

	CachedActionExecUsec

	// New counter types go here!
)

func DetailedStatsEnabled() bool {
	return *detailedStatsEnabled
}

func cacheTypePrefix(actionCache bool, name string) string {
	if actionCache {
		return "action-cache-" + name
	} else {
		return "cas-" + name
	}
}

// counterKey returns a string key under which general invocation  metrics can
// be accounted.
func counterKey(iid string) string {
	return "hit_tracker/" + iid
}

// targetMissesKey returns a string key under which target level hit metrics can
// be accounted.
func targetMissesKey(iid string) string {
	return "hit_tracker/" + iid + "/misses"
}

// resultIDsKey returns a redis key that stores a set of IDs for all cache
// results associated with the invocation.
func resultIDsKey(iid string) string {
	return "hit_tracker/" + iid + "/result_ids"
}

func counterField(actionCache bool, ct counterType) string {
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
	case DownloadTransferredSizeBytes:
		return "compressed-download-size-bytes"
	case UploadTransferredSizeBytes:
		return "compressed-upload-size-bytes"
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

type HitTracker struct {
	c           interfaces.MetricsCollector
	usage       interfaces.UsageTracker
	ctx         context.Context
	iid         string
	actionCache bool

	// The request metadata, may be nil or incomplete.
	requestMetadata *repb.RequestMetadata
}

func NewHitTracker(ctx context.Context, env environment.Env, actionCache bool) *HitTracker {
	return &HitTracker{
		c:               env.GetMetricsCollector(),
		usage:           env.GetUsageTracker(),
		ctx:             ctx,
		iid:             bazel_request.GetInvocationID(ctx),
		actionCache:     actionCache,
		requestMetadata: bazel_request.GetRequestMetadata(ctx),
	}
}

func (h *HitTracker) counterKey() string {
	return counterKey(h.iid)
}

func (h *HitTracker) targetMissesKey() string {
	return targetMissesKey(h.iid)
}

func (h *HitTracker) targetField() string {
	if h.requestMetadata == nil {
		return ""
	}
	rmd := h.requestMetadata
	return makeTargetField(rmd.GetActionMnemonic(), rmd.GetTargetId(), rmd.GetActionId())
}

func (h *HitTracker) counterField(ct counterType) string {
	return counterField(h.actionCache, ct)
}

func (h *HitTracker) cacheTypeLabel() string {
	if h.actionCache {
		return actionCacheLabel
	}
	return casLabel
}

func makeTargetField(actionMnemonic, targetID, actionID string) string {
	return fmt.Sprintf("%s(%s)/%s", actionMnemonic, targetID, actionID)
}

// Example Usage:
//
// ht := NewHitTracker(env, invocationID, false /*=actionCache*/)
// if err := ht.TrackMiss(); err != nil {
//   log.Printf("Error counting cache miss.")
// }
func (h *HitTracker) TrackMiss(d *repb.Digest) error {
	start := time.Now()
	metrics.CacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      h.cacheTypeLabel(),
		metrics.CacheEventTypeLabel: missLabel,
	}).Inc()
	if h.c == nil || h.iid == "" {
		return nil
	}
	if err := h.c.IncrementCount(h.ctx, h.counterKey(), h.counterField(Miss), 1); err != nil {
		return err
	}
	if *detailedStatsEnabled {
		stats := &detailedStats{
			Status:    Miss,
			StartTime: start,
			Duration:  time.Since(start),
		}
		if err := h.recordDetailedStats(d, stats); err != nil {
			return err
		}
	} else if h.actionCache {
		if err := h.c.IncrementCount(h.ctx, h.targetMissesKey(), h.targetField(), 1); err != nil {
			return err
		}
	}
	return nil
}

func (h *HitTracker) TrackEmptyHit() error {
	start := time.Now()
	metrics.CacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      h.cacheTypeLabel(),
		metrics.CacheEventTypeLabel: hitLabel,
	}).Inc()
	if h.c == nil || h.iid == "" {
		return nil
	}
	if err := h.c.IncrementCount(h.ctx, h.counterKey(), h.counterField(Hit), 1); err != nil {
		return err
	}
	if *detailedStatsEnabled {
		emptyDigest := &repb.Digest{Hash: digest.EmptySha256}
		stats := &detailedStats{
			Status:    Miss,
			StartTime: start,
			Duration:  time.Since(start),
		}
		if err := h.recordDetailedStats(emptyDigest, stats); err != nil {
			return err
		}
	}
	return nil
}

func (h *HitTracker) recordDetailedStats(d *repb.Digest, stats *detailedStats) error {
	if h.requestMetadata.GetExecutorDetails().GetExecutorHostId() != "" {
		// Don't store executor requests in the scorecard for now.
		return nil
	}

	rid, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	if err := h.c.SetAdd(h.ctx, resultIDsKey(h.iid), rid.String()); err != nil {
		return err
	}

	// TODO(bduffany): Use protos instead of counterType so we can avoid this
	// translation
	cacheType := capb.CacheType_CAS
	if h.actionCache {
		cacheType = capb.CacheType_AC
	}
	requestType := capb.RequestType_READ
	if stats.Status == Upload {
		requestType = capb.RequestType_WRITE
	}
	// TODO(bduffany): Set response code explicitly
	statusCode := codes.OK
	if stats.Status == Miss {
		statusCode = codes.NotFound
	}
	startTimeProto := timestamppb.New(stats.StartTime)
	durationProto := durationpb.New(stats.Duration)

	result := &capb.ScoreCard_Result{
		ActionMnemonic:       h.requestMetadata.ActionMnemonic,
		TargetId:             h.requestMetadata.TargetId,
		ActionId:             h.requestMetadata.ActionId,
		CacheType:            cacheType,
		RequestType:          requestType,
		Digest:               d,
		Status:               &statuspb.Status{Code: int32(statusCode)},
		StartTime:            startTimeProto,
		Duration:             durationProto,
		Compressor:           stats.Compressor,
		TransferredSizeBytes: stats.TransferredSizeBytes,
		// TODO(bduffany): Committed
	}
	b, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	return h.c.Set(h.ctx, rid.String(), string(b), 0)
}

// detailedStats holds detailed cache stats for a transfer.
type detailedStats struct {
	Status               counterType
	StartTime            time.Time
	Duration             time.Duration
	Compressor           repb.Compressor_Value
	TransferredSizeBytes int64
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

type transferTimer struct {
	h *HitTracker

	d     *repb.Digest
	start time.Time
	actionCounter,
	sizeCounter,
	timeCounter counterType
	// TODO(bduffany): response code
}

func (t *transferTimer) Close() error {
	return t.CloseWithBytesTransferred(t.d.GetSizeBytes(), repb.Compressor_IDENTITY)
}

func (t *transferTimer) CloseWithBytesTransferred(transferredSizeBytes int64, compressor repb.Compressor_Value) error {
	dur := time.Since(t.start)

	h := t.h
	et := cacheEventTypeLabel(t.actionCounter)
	ct := h.cacheTypeLabel()
	metrics.CacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      ct,
		metrics.CacheEventTypeLabel: et,
	}).Inc()
	sizeMetric(t.sizeCounter).With(prometheus.Labels{
		metrics.CacheTypeLabel: ct,
	}).Observe(float64(t.d.GetSizeBytes()))
	durationMetric(t.timeCounter).With(prometheus.Labels{
		metrics.CacheTypeLabel: ct,
	}).Observe(float64(dur.Microseconds()))

	if err := h.recordCacheUsage(t.d, t.actionCounter); err != nil {
		return err
	}

	if h.c == nil || h.iid == "" {
		return nil
	}

	if err := h.c.IncrementCount(h.ctx, h.counterKey(), h.counterField(t.actionCounter), 1); err != nil {
		return err
	}
	if err := h.c.IncrementCount(h.ctx, h.counterKey(), h.counterField(t.sizeCounter), t.d.GetSizeBytes()); err != nil {
		return err
	}
	compressedSizeCounter := DownloadTransferredSizeBytes
	if t.sizeCounter == UploadSizeBytes {
		compressedSizeCounter = UploadTransferredSizeBytes
	}
	if err := h.c.IncrementCount(h.ctx, h.counterKey(), h.counterField(compressedSizeCounter), transferredSizeBytes); err != nil {
		return err
	}
	if err := h.c.IncrementCount(h.ctx, h.counterKey(), h.counterField(t.timeCounter), dur.Microseconds()); err != nil {
		return err
	}
	if *detailedStatsEnabled {
		stats := &detailedStats{
			Status:               t.actionCounter,
			StartTime:            t.start,
			Duration:             dur,
			Compressor:           compressor,
			TransferredSizeBytes: transferredSizeBytes,
		}
		if err := h.recordDetailedStats(t.d, stats); err != nil {
			return err
		}
	} else if h.actionCache && t.actionCounter == Miss {
		if err := h.c.IncrementCount(h.ctx, h.targetMissesKey(), h.targetField(), 1); err != nil {
			return err
		}
	}
	return nil
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
		h:             h,
		d:             d,
		start:         start,
		actionCounter: Hit,
		sizeCounter:   DownloadSizeBytes,
		timeCounter:   DownloadUsec,
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
		h:             h,
		d:             d,
		start:         start,
		actionCounter: Upload,
		sizeCounter:   UploadSizeBytes,
		timeCounter:   UploadUsec,
	}
}

func (h *HitTracker) recordCacheUsage(d *repb.Digest, actionCounter counterType) error {
	if h.usage == nil || actionCounter != Hit {
		return nil
	}
	c := &tables.UsageCounts{
		TotalDownloadSizeBytes: d.GetSizeBytes(),
	}
	if h.actionCache {
		c.ActionCacheHits = 1
	} else {
		c.CASCacheHits = 1
	}
	return h.usage.Increment(h.ctx, c)
}

func computeThroughputBytesPerSecond(sizeBytes, durationUsec int64) int64 {
	if durationUsec == 0 {
		return 0
	}
	dur := time.Duration(durationUsec * int64(time.Microsecond))
	return int64(float64(sizeBytes) / dur.Seconds())
}

func parseTargetField(f string) (string, string, string) {
	// parses a string like this:
	// "GoLink(//merger:merger_test)/16f1152b7b260f690ea06f8b938a1b60712b5ee41a1c125ecad8ed9416481fbb"
	match := actionRegexp.FindStringSubmatch(f)
	if len(match) != 4 {
		return "", "", ""
	}
	return match[1], match[2], match[3]
}

func ScoreCard(ctx context.Context, env environment.Env, iid string) *capb.ScoreCard {
	if *detailedStatsEnabled {
		return readResults(ctx, env, iid)
	}

	c := env.GetMetricsCollector()
	if c == nil || iid == "" {
		return nil
	}
	sc := &capb.ScoreCard{}

	misses, err := c.ReadCounts(ctx, targetMissesKey(iid))
	if err != nil {
		log.Warningf("Failed to collect score card: %s", err)
		return sc
	}
	if misses == nil {
		misses = make(map[string]int64)
	}
	sortedKeys := make([]string, 0, len(misses))
	for targetField, _ := range misses {
		sortedKeys = append(sortedKeys, targetField)
	}
	// TODO(tylerw): figure out pagination or something? For now, truncate
	// the number of cache misses to 1K if there are more than that. Too
	// many are not useful in the UI and we don't want to pay the storage
	// cost either.
	if len(sortedKeys) > 1000 {
		sortedKeys = sortedKeys[:1000]
	}
	sort.Strings(sortedKeys)

	for _, targetField := range sortedKeys {
		mnemonic, targetID, actionID := parseTargetField(targetField)
		if mnemonic == "" || targetID == "" || actionID == "" {
			continue
		}
		sc.Misses = append(sc.Misses, &capb.ScoreCard_Result{
			ActionMnemonic: mnemonic,
			TargetId:       targetID,
			ActionId:       actionID,
		})
	}
	return sc
}

func readResults(ctx context.Context, env environment.Env, iid string) *capb.ScoreCard {
	c := env.GetMetricsCollector()
	if c == nil || iid == "" {
		return nil
	}
	sc := &capb.ScoreCard{}

	resultIDs, err := c.SetGetMembers(ctx, resultIDsKey(iid))
	if err != nil {
		log.Warningf("Failed to read detailed scorecard for invocation %s: %s", iid, err)
		return sc
	}
	// This limit is a safeguard against buffering too many results in memory.
	// We expect to hit this rarely (if ever) in production usage.
	const limit = 500_000
	if len(resultIDs) > limit {
		log.Warningf("Truncating cache scorecard for invocation %s from %d to %d results", iid, len(resultIDs), limit)
		resultIDs = resultIDs[:limit]
	}

	serializedResults, err := c.GetAll(ctx, resultIDs...)
	if err != nil {
		log.Warningf("Failed to read cache scorecard for invocation %s: %s", iid, err)
		return sc
	}

	sc.Results = make([]*capb.ScoreCard_Result, 0, len(serializedResults))
	for _, serializedResult := range serializedResults {
		r := &capb.ScoreCard_Result{}
		if err := proto.Unmarshal([]byte(serializedResult), r); err != nil {
			log.Warning(err.Error())
			continue
		}
		sc.Results = append(sc.Results, r)
	}
	return sc
}

func CollectCacheStats(ctx context.Context, env environment.Env, iid string) *capb.CacheStats {
	c := env.GetMetricsCollector()
	if c == nil || iid == "" {
		return nil
	}
	cs := &capb.CacheStats{}

	counts, err := c.ReadCounts(ctx, counterKey(iid))
	if err != nil {
		log.Warningf("Failed to collect cache stats for invocation %s: %s", iid, err)
		return cs
	}
	if counts == nil {
		counts = make(map[string]int64)
	}

	cs.ActionCacheHits = counts[counterField(true, Hit)]
	cs.ActionCacheMisses = counts[counterField(true, Miss)]
	cs.ActionCacheUploads = counts[counterField(true, Upload)]

	cs.CasCacheHits = counts[counterField(false, Hit)]
	cs.CasCacheMisses = counts[counterField(false, Miss)]
	cs.CasCacheUploads = counts[counterField(false, Upload)]

	cs.TotalDownloadSizeBytes = counts[counterField(false, DownloadSizeBytes)]
	cs.TotalUploadSizeBytes = counts[counterField(false, UploadSizeBytes)]
	cs.TotalDownloadTransferredSizeBytes = counts[counterField(false, DownloadTransferredSizeBytes)]
	cs.TotalUploadTransferredSizeBytes = counts[counterField(false, UploadTransferredSizeBytes)]
	cs.TotalDownloadUsec = counts[counterField(false, DownloadUsec)]
	cs.TotalUploadUsec = counts[counterField(false, UploadUsec)]

	cs.DownloadThroughputBytesPerSecond = computeThroughputBytesPerSecond(cs.TotalDownloadSizeBytes, cs.TotalDownloadUsec)
	cs.UploadThroughputBytesPerSecond = computeThroughputBytesPerSecond(cs.TotalUploadSizeBytes, cs.TotalUploadUsec)

	cs.TotalCachedActionExecUsec = counts[counterField(false, CachedActionExecUsec)]

	return cs
}

func CleanupCacheStats(ctx context.Context, env environment.Env, iid string) {
	c := env.GetMetricsCollector()
	if c == nil || iid == "" {
		return
	}

	if err := c.Delete(ctx, counterKey(iid)); err != nil {
		log.Warningf("Failed to clean up cache stats for invocation %s: %s", iid, err)
	}
}
