package scorecard

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/codes"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
)

const (
	// Default page size for the scorecard when a page token is not included in
	// the request.
	defaultScoreCardPageSize = 100
)

var (
	bytestreamURIPattern = regexp.MustCompile(`^bytestream://.*/blobs/([a-z0-9]{64})/\d+$`)
)

// GetCacheScoreCard returns a list of detailed, per-request cache stats.
func GetCacheScoreCard(ctx context.Context, env environment.Env, req *capb.GetCacheScoreCardRequest) (*capb.GetCacheScoreCardResponse, error) {
	// Authorize access to the requested invocation
	invocation, err := env.GetInvocationDB().LookupInvocation(ctx, req.GetInvocationId())
	if err != nil {
		return nil, err
	}
	page := &pgpb.OffsetLimit{Offset: 0, Limit: defaultScoreCardPageSize}
	if req.PageToken != "" {
		reqPage, err := paging.DecodeOffsetLimit(req.PageToken)
		if err != nil {
			return nil, err
		}
		page = reqPage
	}

	if page.Offset < 0 || page.Limit < 0 {
		return nil, status.InvalidArgumentError("invalid page token")
	}

	scorecard := &capb.ScoreCard{}
	for attempt := uint64(0); attempt < invocation.Attempt; attempt++ {
		sc, err := Read(ctx, env, req.InvocationId, attempt)
		if err != nil {
			if status.IsNotFoundError(err) {
				// it's okay for scorecards to be missing for prior attempts
				continue
			}
			return nil, err
		}
		scorecard.Misses = append(scorecard.Misses, sc.Misses...)
		scorecard.Results = append(scorecard.Results, sc.Results...)
	}
	sc, err := Read(ctx, env, req.InvocationId, invocation.Attempt)
	if err != nil {
		return nil, err
	}
	scorecard.Misses = append(scorecard.Misses, sc.Misses...)
	scorecard.Results = append(scorecard.Results, sc.Results...)

	results, err := filterResults(scorecard.Results, req)
	if err != nil {
		return nil, err
	}

	start := page.Offset
	if start > int64(len(results)) {
		start = int64(len(results))
	}
	end := start + page.Limit
	if end > int64(len(results)) {
		end = int64(len(results))
	}

	nextPageToken := ""
	if end < int64(len(results)) {
		next, err := paging.EncodeOffsetLimit(&pgpb.OffsetLimit{
			Offset: int64(end),
			Limit:  defaultScoreCardPageSize,
		})
		if err != nil {
			return nil, err
		}
		nextPageToken = next
	}

	sortResults(results, &sortOpts{
		GroupBy:    req.GroupBy,
		OrderBy:    req.OrderBy,
		Descending: req.Descending,
	})
	return &capb.GetCacheScoreCardResponse{
		Results:       results[start:end],
		NextPageToken: nextPageToken,
	}, nil
}

func filterResults(results []*capb.ScoreCard_Result, req *capb.GetCacheScoreCardRequest) ([]*capb.ScoreCard_Result, error) {
	mask := req.GetFilter().GetMask()
	if len(mask.GetPaths()) == 0 {
		return results, nil
	}
	if !mask.IsValid(req.GetFilter()) {
		return nil, status.InvalidArgumentErrorf("invalid field mask: paths %s", mask.GetPaths())
	}
	predicates := make([]func(*capb.ScoreCard_Result) bool, 0, len(mask.GetPaths()))
	for _, path := range mask.GetPaths() {
		switch path {
		case "cache_type":
			predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
				filterCacheType := req.GetFilter().GetCacheType()
				resultCacheType := result.GetCacheType()
				return resultCacheType == filterCacheType
			})
		case "request_type":
			predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
				return result.GetRequestType() == req.GetFilter().GetRequestType()
			})
		case "response_type":
			switch req.GetFilter().GetResponseType() {
			case capb.ResponseType_OK:
				predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
					return result.GetStatus().GetCode() == int32(codes.OK)
				})
			case capb.ResponseType_NOT_FOUND:
				predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
					return result.GetStatus().GetCode() == int32(codes.NotFound)
				})
			case capb.ResponseType_ERROR:
				predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
					c := result.GetStatus().GetCode()
					return c != int32(codes.OK) && c != int32(codes.NotFound)
				})
			default:
				return nil, status.InvalidArgumentErrorf("invalid response type %d", req.GetFilter().GetResponseType())
			}
		case "search":
			s := strings.ToLower(req.GetFilter().GetSearch())
			predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
				filePath := filepath.Join(result.GetPathPrefix(), result.GetName())
				if req.GetFilter().GetExactMatch() {
					return strings.ToLower(result.GetActionId()) == s ||
						strings.ToLower(result.GetActionMnemonic()) == s ||
						strings.ToLower(result.GetTargetId()) == s ||
						strings.ToLower(result.GetDigest().GetHash()) == s ||
						strings.ToLower(filePath) == s
				}
				return strings.Contains(strings.ToLower(result.GetActionId()), s) ||
					strings.Contains(strings.ToLower(result.GetActionMnemonic()), s) ||
					strings.Contains(strings.ToLower(result.GetTargetId()), s) ||
					strings.Contains(strings.ToLower(result.GetDigest().GetHash()), s) ||
					strings.Contains(strings.ToLower(filePath), s)
			})
		default:
			return nil, status.InvalidArgumentErrorf("invalid field path %q", path)
		}
	}

	isMatched := func(result *capb.ScoreCard_Result) bool {
		for _, pred := range predicates {
			if !pred(result) {
				return false
			}
		}
		return true
	}

	out := make([]*capb.ScoreCard_Result, 0, len(results))
	for _, result := range results {
		if isMatched(result) {
			out = append(out, result)
		}
	}

	return out, nil
}

type sortOpts struct {
	GroupBy    capb.GetCacheScoreCardRequest_GroupBy
	OrderBy    capb.GetCacheScoreCardRequest_OrderBy
	Descending bool
}

// SortResults sorts scorecard results according to the provided SortOpts.
func sortResults(results []*capb.ScoreCard_Result, opts *sortOpts) {
	// orderFunc maps each result to a number, so that results can be sorted
	// as though they are just numbers.
	var orderFunc func(result *capb.ScoreCard_Result) int64
	switch opts.OrderBy {
	case capb.GetCacheScoreCardRequest_ORDER_BY_DURATION:
		orderFunc = func(result *capb.ScoreCard_Result) int64 {
			return result.Duration.AsDuration().Nanoseconds()
		}
	case capb.GetCacheScoreCardRequest_ORDER_BY_SIZE:
		orderFunc = func(result *capb.ScoreCard_Result) int64 {
			return result.Digest.GetSizeBytes()
		}
	case capb.GetCacheScoreCardRequest_ORDER_BY_START_TIME:
		fallthrough
	default:
		orderFunc = func(result *capb.ScoreCard_Result) int64 {
			return result.StartTime.AsTime().UnixNano()
		}
	}
	// When ordering in descending order, just negate the order.
	if opts.Descending {
		originalOrderFunc := orderFunc
		orderFunc = func(result *capb.ScoreCard_Result) int64 {
			return -originalOrderFunc(result)
		}
	}

	// groupKeyFunc maps each result to a unique ID for its containing group
	// (either action or target that the result was a part of).
	var groupKeyFunc func(res *capb.ScoreCard_Result) string
	switch opts.GroupBy {
	case capb.GetCacheScoreCardRequest_GROUP_BY_ACTION:
		groupKeyFunc = func(res *capb.ScoreCard_Result) string {
			return res.ActionId
		}
	case capb.GetCacheScoreCardRequest_GROUP_BY_TARGET:
		groupKeyFunc = func(res *capb.ScoreCard_Result) string {
			return res.TargetId
		}
	default:
		break
	}

	// If grouping results, compute each group's order as its min result order.
	// (Each result belongs to a group, identified by groupKeyFunc).
	groupOrder := make(map[string]int64)
	if groupKeyFunc != nil {
		for _, result := range results {
			groupKey := groupKeyFunc(result)
			order := orderFunc(result)
			existing, ok := groupOrder[groupKey]
			if !ok {
				groupOrder[groupKey] = order
				continue
			}
			if order < existing {
				groupOrder[groupKey] = order
			}
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if groupKeyFunc != nil {
			gi := groupKeyFunc(results[i])
			gj := groupKeyFunc(results[j])
			// For results with different parent groups, sort results earlier if
			// their parent group's min start time comes first.
			if gi != gj {
				oi := groupOrder[gi]
				oj := groupOrder[gj]
				if oi == oj {
					// If two groups happen to have exactly the same order, break the
					// tie by group key so that the sort is deterministic.
					return gi < gj
				}
				return oi < oj
			}
		}
		// Sort by result order.
		oi := orderFunc(results[i])
		oj := orderFunc(results[j])
		// If two results happen to have exactly the same order, break the tie
		// by result digest so that the sort is deterministic.
		if oi == oj {
			return results[i].GetDigest().GetHash() < results[j].GetDigest().GetHash()
		}
		return oi < oj
	})
}

func blobName(invocationID string, invocationAttempt uint64) string {
	// WARNING: Things will break if this is changed, because we use this name
	// to lookup data from historical invocations.
	blobFileName := "scorecard.pb"
	return filepath.Join(invocationID, fmt.Sprint(invocationAttempt), blobFileName)
}

// blobNameDeprecated returns a deprecated cache scorecard file name
// This may be needed to lookup data from historical invocations that used the deprecated naming
func blobNameDeprecated(invocationID string) string {
	blobFileName := invocationID + "-scorecard.pb"
	return filepath.Join(invocationID, blobFileName)
}

// ExtractFiles extracts any files from invocation BES events which may be
// associated with bes-upload cache requests, such as the timing profile or
// other uploads that weren't directly tied to an action execution. The returned
// mapping is keyed by digest hash.
func ExtractFiles(invocation *inpb.Invocation) map[string]*bespb.File {
	out := map[string]*bespb.File{}

	maybeAddToMap := func(files ...*bespb.File) {
		for _, file := range files {
			if file == nil {
				continue
			}
			if file.GetName() == "" {
				continue
			}
			if uri, ok := file.File.(*bespb.File_Uri); ok {
				m := bytestreamURIPattern.FindStringSubmatch(uri.Uri)
				if len(m) >= 1 {
					digestHash := m[1]
					out[digestHash] = file
				}
			}
		}
	}

	for _, event := range invocation.Event {
		switch p := event.BuildEvent.Payload.(type) {
		case *bespb.BuildEvent_NamedSetOfFiles:
			maybeAddToMap(p.NamedSetOfFiles.GetFiles()...)
		case *bespb.BuildEvent_BuildToolLogs:
			maybeAddToMap(p.BuildToolLogs.GetLog()...)
		case *bespb.BuildEvent_TestResult:
			maybeAddToMap(p.TestResult.GetTestActionOutput()...)
		case *bespb.BuildEvent_TestSummary:
			maybeAddToMap(p.TestSummary.GetPassed()...)
			maybeAddToMap(p.TestSummary.GetFailed()...)
		case *bespb.BuildEvent_RunTargetAnalyzed:
			maybeAddToMap(p.RunTargetAnalyzed.GetRunfiles()...)
		case *bespb.BuildEvent_Action:
			maybeAddToMap(p.Action.GetStdout())
			maybeAddToMap(p.Action.GetStderr())
			maybeAddToMap(p.Action.GetPrimaryOutput())
			maybeAddToMap(p.Action.GetActionMetadataLogs()...)
		case *bespb.BuildEvent_Completed:
			maybeAddToMap(p.Completed.GetImportantOutput()...)
			maybeAddToMap(p.Completed.GetDirectoryOutput()...)
		}
	}

	return out
}

// FillBESMetadata populates file metadata in the scorecard results for files
// uploaded via BEP.
func FillBESMetadata(sc *capb.ScoreCard, files map[string]*bespb.File) {
	for _, result := range sc.Results {
		if result.ActionId != "bes-upload" {
			continue
		}
		f := files[result.Digest.GetHash()]
		if f == nil {
			continue
		}
		result.Name = f.Name
		result.PathPrefix = filepath.Join(f.PathPrefix...)
	}
}

// Read reads the invocation cache scorecard from the configured blobstore.
func Read(ctx context.Context, env environment.Env, invocationID string, invocationAttempt uint64) (*capb.ScoreCard, error) {
	blobStore := env.GetBlobstore()
	buf, err := blobStore.ReadBlob(ctx, blobName(invocationID, invocationAttempt))
	if err != nil && status.IsNotFoundError(err) {
		// Try reading from deprecated file name for older files that were written before the naming changed
		buf, err = blobStore.ReadBlob(ctx, blobNameDeprecated(invocationID))
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	sc := &capb.ScoreCard{}
	if err := proto.Unmarshal(buf, sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// Write writes the invocation cache scorecard to the configured blobstore.
func Write(ctx context.Context, env environment.Env, invocationID string, invocationAttempt uint64, scoreCard *capb.ScoreCard) error {
	// Use MarshalOld b/c ScoreCard.MarshalVT is 50% slower than standard Marshal()
	// See https://github.com/buildbuddy-io/buildbuddy-internal/issues/3018
	scoreCardBuf, err := proto.MarshalOld(scoreCard)
	if err != nil {
		return err
	}
	blobStore := env.GetBlobstore()
	_, err = blobStore.WriteBlob(ctx, blobName(invocationID, invocationAttempt), scoreCardBuf)
	return err
}
