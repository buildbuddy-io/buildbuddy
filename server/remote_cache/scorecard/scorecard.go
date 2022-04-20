package scorecard

import (
	"context"
	"path/filepath"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"

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

// SortResults sorts scorecard results, grouping by actions sorted by min
// request start timestamp, and sorting the requests within each action by start
// time.
// TODO(bduffany): More sorting options
func SortResults(results []*capb.ScoreCard_Result) {
	// Compute the min result start for each action. (Each result belongs to
	// an action, identified by action ID).
	minStartTime := make(map[string]time.Time)
	for _, result := range results {
		t := result.StartTime.AsTime()
		existing, ok := minStartTime[result.ActionId]
		if !ok {
			minStartTime[result.ActionId] = t
			continue
		}
		if result.StartTime.AsTime().Before(existing) {
			minStartTime[result.ActionId] = t
		}
	}
	sort.Slice(results, func(i, j int) bool {
		// For results with different parent actions, sort results earlier if
		// their parent action's min start time comes first.
		if results[i].ActionId != results[j].ActionId {
			ti := minStartTime[results[i].ActionId]
			tj := minStartTime[results[j].ActionId]
			if ti.Equal(tj) {
				// If two actions happen to have exactly the same start time, break the
				// tie by action ID so that the sort is deterministic.
				return results[i].ActionId < results[j].ActionId
			}
			return ti.Before(tj)
		}
		// Within actions, sort by start time.
		return results[i].StartTime.AsTime().Before(results[j].StartTime.AsTime())
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
