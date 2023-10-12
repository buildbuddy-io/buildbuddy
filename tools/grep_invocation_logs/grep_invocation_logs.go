package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

var (
	// Required flags
	target  = flag.String("target", "grpcs://remote.buildbuddy.io", "BuildBuddy gRPC target")
	apiKey  = flag.String("api_key", "", "API key")
	groupID = flag.String("group_id", "", "Group ID")
	pattern = flag.String("pattern", "", "Pattern to search for (regex)")

	// Filtering options
	roles    = flag.Slice("role", []string{}, "Invocation role filter (e.g. CI_RUNNER)")
	statuses = flag.Slice("status", []string{}, "Invocation status")

	// Time window selection
	count = flag.Int("count", 100, "Max number of invocations to fetch")

	// Output options (optional)
	timestamps = flag.Bool("timestamps", false, "Show invocation updated_at_usec timestamps")
)

const (
	searchInvocationsPageSize = 500
	logFetchTimeout           = 60 * time.Second
)

type Match struct {
	Invocation *inpb.Invocation
	LineNumber int
	Line       string
}

type InvocationResult struct {
	Matches []Match
}

func main() {
	flag.Parse()
	if *apiKey == "" {
		log.Fatalf("-api_key is required")
	}
	if *groupID == "" {
		// TODO: infer group ID from API key.
		// There doesn't seem to be a way to do this currently.
		log.Fatalf("-group_id is required")
	}
	if *pattern == "" {
		log.Fatalf("-pattern is required")
	}
	var parsedStatuses []inspb.OverallStatus
	for _, s := range *statuses {
		value, ok := inspb.OverallStatus_value[s]
		if !ok {
			log.Fatalf(
				"Invalid invocation_status.OverallStatus enum value %q. Allowed values: %s",
				s, strings.Join(mapKeys(inspb.OverallStatus_value), ", "))
		}
		parsedStatuses = append(parsedStatuses, inspb.OverallStatus(value))
	}
	re, err := regexp.Compile(*pattern)
	if err != nil {
		log.Fatalf("-pattern is not a valid regex: %s", err)
	}
	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		log.Fatalf("Failed to dial target %q: %s", *target, err)
	}
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	bb := bbspb.NewBuildBuddyServiceClient(conn)

	pagesCh := make(chan *inpb.SearchInvocationResponse, 4)
	go func() {
		defer close(pagesCh)
		pageToken := ""
		for n := 0; n < *count; n += searchInvocationsPageSize {
			count := min(*count-n, searchInvocationsPageSize)
			res, err := bb.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
				Query: &inpb.InvocationQuery{
					GroupId: *groupID,
					Role:    *roles,
					Status:  parsedStatuses,
				},
				Count:     int32(count),
				PageToken: pageToken,
			})
			if err != nil {
				log.Fatalf("Failed to fetch invocation history: %s", err)
			}
			pagesCh <- res
			pageToken = res.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}()

	var printMu sync.Mutex

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(64)
	for res := range pagesCh {
		for _, invocation := range res.Invocation {
			invocation := invocation
			eg.Go(func() error {
				iid := invocation.GetInvocationId()
				// Fetch log
				ctx, cancel := context.WithTimeout(ctx, logFetchTimeout)
				defer cancel()
				text, err := NewInvocationLog(bb, iid).Get(ctx)
				if err != nil {
					log.Warningf("Failed to get logs for invocation %q: %s", iid, err)
					return nil
				}
				lines := strings.Split(text, "\n")
				var matches []Match
				for i, line := range lines {
					if re.MatchString(line) {
						lineNumber := i + 1
						matches = append(matches, Match{
							Invocation: invocation,
							LineNumber: lineNumber,
							Line:       line,
						})
					}
				}
				if len(matches) == 0 {
					return nil
				}
				// Lock to avoid interleaving matches for invocations.
				printMu.Lock()
				defer printMu.Unlock()
				for _, m := range matches {
					timestampPrefix := ""
					if *timestamps {
						timestampPrefix = fmt.Sprintf("%d:", m.Invocation.GetUpdatedAtUsec())
					}
					fmt.Printf("%s%s:%d:%s\n", timestampPrefix, m.Invocation.GetInvocationId(), m.LineNumber, m.Line)
				}
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		log.Fatal(err.Error())
	}
}

// TODO: this was copied from build_logs_test.go, move this to a shared util.

// invocationLog fetches and buffers logs for an invocation.
type invocationLog struct {
	bbClient     bbspb.BuildBuddyServiceClient
	invocationID string

	logs            []byte
	stableLogLength int
	chunkID         string
	eof             bool
}

func NewInvocationLog(bbClient bbspb.BuildBuddyServiceClient, invocationID string) *invocationLog {
	return &invocationLog{
		bbClient:     bbClient,
		invocationID: invocationID,
	}
}

// Get returns the invocation log that is available so far.
//
// For completed invocations, this returns the entire invocation log. For
// in-progress invocations, this returns the portion of the log that has been
// received and processed by the server so far.
func (c *invocationLog) Get(ctx context.Context) (string, error) {
	if c.eof {
		return string(c.logs), nil
	}

	for {
		req := &elpb.GetEventLogChunkRequest{
			InvocationId: c.invocationID,
			ChunkId:      c.chunkID,
			MinLines:     100_000,
		}
		res, err := c.bbClient.GetEventLogChunk(ctx, req)
		if err != nil {
			return "", err
		}

		c.logs = c.logs[:c.stableLogLength]
		c.logs = append(c.logs, res.Buffer...)
		if !res.Live {
			c.stableLogLength = len(c.logs)
		}

		// Empty next chunk ID means the invocation is complete and we've reached
		// the end of the log.
		if res.NextChunkId == "" {
			c.eof = true
			return string(c.logs), nil
		}

		// Unchanged next chunk ID means the invocation is still in progress and
		// we've fetched all available chunks; break and return the result.
		if res.NextChunkId == c.chunkID {
			break
		}

		// New next chunk ID means we successfully fetched the requested
		// chunk, and more may be available. Continue fetching the next chunk.
		c.chunkID = res.NextChunkId
	}

	return string(c.logs), nil
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	var keys []K
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
