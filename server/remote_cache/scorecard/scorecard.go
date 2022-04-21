package scorecard

import (
	"context"
	"path/filepath"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
)

const (
	// Default page size for the scorecard when a page token is not included in
	// the request.
	defaultScoreCardPageSize = 250
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

	start := page.Offset
	if start > int64(len(scorecard.Results)) {
		start = int64(len(scorecard.Results))
	}
	end := start + page.Limit
	if end > int64(len(scorecard.Results)) {
		end = int64(len(scorecard.Results))
	}

	nextPageToken := ""
	if end < int64(len(scorecard.Results)) {
		next, err := paging.EncodeOffsetLimit(&pgpb.OffsetLimit{
			Offset: int64(end),
			Limit:  defaultScoreCardPageSize,
		})
		if err != nil {
			return nil, err
		}
		nextPageToken = next
	}

	return &capb.GetCacheScoreCardResponse{
		Results:       scorecard.Results[start:end],
		NextPageToken: nextPageToken,
	}, nil
}

func isTimestampLess(a, b *timestamppb.Timestamp) bool {
	if a.Seconds != b.Seconds {
		return a.Seconds < b.Seconds
	}
	return a.Nanos < b.Nanos
}

// SortResults sorts scorecard results, grouping by actions sorted by min
// request start timestamp, and sorting the requests within each action by start
// time.
// TODO(bduffany): More sorting options
func SortResults(results []*capb.ScoreCard_Result) {
	minStartTimeByActionID := map[string]*timestamppb.Timestamp{}
	for _, result := range results {
		existing, ok := minStartTimeByActionID[result.ActionId]
		if !ok {
			minStartTimeByActionID[result.ActionId] = result.StartTime
			continue
		}
		if isTimestampLess(result.StartTime, existing) {
			minStartTimeByActionID[result.ActionId] = result.StartTime
		}
	}
	sort.Slice(results, func(i, j int) bool {
		// Sort first by action min start time
		ti := minStartTimeByActionID[results[i].ActionId]
		tj := minStartTimeByActionID[results[j].ActionId]
		if isTimestampLess(ti, tj) {
			return true
		}
		// If two different actions have the same start time, break the tie using
		// their action ID, so that results stay grouped by action ID.
		if !isTimestampLess(tj, ti) && results[i].ActionId != results[j].ActionId {
			return results[i].ActionId < results[j].ActionId
		}
		// Within a single action, sort by start time.
		return isTimestampLess(results[i].StartTime, results[j].StartTime)
	})
}

func blobName(invocationID string) string {
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
