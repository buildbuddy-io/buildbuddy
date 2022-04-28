package scorecard

import (
	"context"
	"path/filepath"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
)

const (
	// Default page size for the scorecard when a page token is not included in
	// the request.
	defaultScoreCardPageSize = 100
)

// GetCacheScoreCard returns a list of detailed, per-request cache stats.
func GetCacheScoreCard(ctx context.Context, env environment.Env, req *capb.GetCacheScoreCardRequest) (*capb.GetCacheScoreCardResponse, error) {
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

	scorecard, err := Read(ctx, env, req.InvocationId)
	if err != nil {
		return nil, err
	}

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
		GroupByAction: req.GroupByAction,
		OrderBy:       req.OrderBy,
		Descending:    req.Descending,
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
				return result.GetCacheType() == req.GetFilter().GetCacheType()
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
			predicates = append(predicates, func(result *capb.ScoreCard_Result) bool {
				s := req.GetFilter().GetSearch()
				return strings.Contains(result.GetActionId(), s) ||
					strings.Contains(result.GetActionMnemonic(), s) ||
					strings.Contains(result.GetTargetId(), s) ||
					strings.Contains(result.GetDigest().GetHash(), s)
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
	GroupByAction bool
	OrderBy       capb.GetCacheScoreCardRequest_OrderBy
	Descending    bool
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

	// If grouping by action, compute each action's order as its min result order.
	// (Each result belongs to an action, identified by action ID).
	actionOrder := make(map[string]int64)
	if opts.GroupByAction {
		for _, result := range results {
			order := orderFunc(result)
			existing, ok := actionOrder[result.ActionId]
			if !ok {
				actionOrder[result.ActionId] = order
				continue
			}
			if order < existing {
				actionOrder[result.ActionId] = order
			}
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if opts.GroupByAction {
			// For results with different parent actions, sort results earlier if
			// their parent action's min start time comes first.
			if results[i].ActionId != results[j].ActionId {
				oi := actionOrder[results[i].ActionId]
				oj := actionOrder[results[j].ActionId]
				if oi == oj {
					// If two actions happen to have exactly the same order, break the
					// tie by action ID so that the sort is deterministic.
					return results[i].ActionId < results[j].ActionId
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

func blobName(invocationID string) string {
	// WARNING: Things will break if this is changed, because we use this name
	// to lookup data from historical invocations.
	blobFileName := invocationID + "-scorecard.pb"
	return filepath.Join(invocationID, blobFileName)
}

// Read reads the invocation cache scorecard from the configured blobstore.
func Read(ctx context.Context, env environment.Env, invocationID string) (*capb.ScoreCard, error) {
	blobStore := env.GetBlobstore()
	buf, err := blobStore.ReadBlob(ctx, blobName(invocationID))
	if err != nil {
		return nil, err
	}
	sc := &capb.ScoreCard{}
	if err := proto.Unmarshal(buf, sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// Write writes the invocation cache scorecard to the configured blobstore.
func Write(ctx context.Context, env environment.Env, invocationID string, scoreCard *capb.ScoreCard) error {
	scoreCardBuf, err := proto.Marshal(scoreCard)
	if err != nil {
		return err
	}
	blobStore := env.GetBlobstore()
	_, err = blobStore.WriteBlob(ctx, blobName(invocationID), scoreCardBuf)
	return err
}
