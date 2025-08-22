package query_service

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	querypb "github.com/buildbuddy-io/buildbuddy/proto/query"
	bqpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_query"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

var (
	enableQueryService = flag.Bool("app.enable_query_service", false, "Whether to enable the query service")
)

type QueryService struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if *enableQueryService {
		env.SetQueryService(NewQueryService(env))
	}
	return nil
}

func NewQueryService(env environment.Env) interfaces.QueryService {
	return &QueryService{
		env: env,
	}
}

func (s *QueryService) Query(ctx context.Context, req *querypb.QueryRequest) (*querypb.QueryResponse, error) {
	log.Infof("Query request: %v", req)
	
	r, err := hostedrunner.New(s.env)
	if err != nil {
		return nil, err
	}

	// Build the bazel query command with --output=proto and save to the artifacts directory
	// The CI runner will automatically upload files from artifacts/command-0/ to CAS
	outputFile := "$BUILDBUDDY_ARTIFACTS_DIRECTORY/query_result.pb"
	
	// Build the bazel query command with user-provided flags
	// Default to --output=proto if not specified by user
	flags := req.GetBazelFlags()
	hasOutputFlag := false
	for _, flag := range flags {
		if strings.HasPrefix(flag, "--output=") || strings.HasPrefix(flag, "-output=") {
			hasOutputFlag = true
			break
		}
	}
	if !hasOutputFlag {
		flags = append([]string{"--output=proto"}, flags...)
	}
	
	// Join all flags with spaces
	flagsStr := strings.Join(flags, " ")
	if flagsStr != "" {
		flagsStr = " " + flagsStr
	}
	
	queryCmd := fmt.Sprintf("bazel query%s %q > %s", flagsStr, req.GetQuery(), outputFile)
	
	// Create steps to run the bazel query
	steps := []*rnpb.Step{
		{Run: queryCmd},
	}

	// Convert platform properties to exec properties
	execProps := make([]*repb.Platform_Property, 0, len(req.GetPlatformProperties()))
	for k, v := range req.GetPlatformProperties() {
		execProps = append(execProps, &repb.Platform_Property{
			Name:  k,
			Value: v,
		})
	}

	// Run the query command on the remote runner
	runReq := &rnpb.RunRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl:                 req.GetRepo(),
			UseSystemGitCredentials: req.GetUseSystemGitCredentials(),
		},
		RepoState: &gitpb.RepoState{
			CommitSha: req.GetCommitSha(),
			Branch:    req.GetBranch(),
			Patch:     req.GetPatches(),
		},
		Steps:          steps,
		Async:          false, // We need to wait for the result
		Env:            req.GetEnv(),
		Timeout:        req.GetTimeout(),
		ExecProperties: execProps,
		RemoteHeaders:  req.GetRemoteHeaders(),
		RunRemotely:    true,
		RunnerFlags:    []string{fmt.Sprintf("--skip_auto_checkout=%v", req.GetSkipAutoCheckout())},
	}

	rsp, err := r.Run(ctx, runReq)
	if err != nil {
		return nil, err
	}

	// The invocation ID for tracking
	invocationID := rsp.GetInvocationId()
	
	// Wait for the run to complete by polling the invocation status
	queryResult, err := s.waitAndFetchQueryResult(ctx, invocationID)
	if err != nil {
		return nil, status.InternalErrorf("Failed to fetch query results: %v", err)
	}
	
	return &querypb.QueryResponse{
		InvocationId: invocationID,
		Result:       queryResult,
	}, nil
}

// waitAndFetchQueryResult waits for the invocation to complete and fetches the query result from CAS
func (s *QueryService) waitAndFetchQueryResult(ctx context.Context, invocationID string) (*bqpb.QueryResult, error) {
	// Get authenticated user for permissions
	user, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	// Poll for invocation completion (with timeout)
	maxWaitTime := 5 * time.Minute
	pollInterval := 2 * time.Second
	deadline := time.Now().Add(maxWaitTime)
	
	var inv *tables.Invocation
	for time.Now().Before(deadline) {
		log.Infof("Polling for invocation %s", invocationID)
		// Query for the invocation
		q := query_builder.NewQuery(`SELECT * FROM "Invocations"`)
		q = q.AddWhereClause(`group_id = ?`, user.GetGroupID())
		q = q.AddWhereClause(`invocation_id = ?`, invocationID)
		
		qStr, qArgs := q.Build()
		rq := s.env.GetDBHandle().NewQuery(ctx, "api_get_invocation").Raw(qStr, qArgs...)
		
		inv = &tables.Invocation{}
		if err := rq.Take(inv); err != nil {
			if db.IsRecordNotFound(err) {
				// Invocation not found yet, wait and retry
				time.Sleep(pollInterval)
				continue
			}
			return nil, err
		}
		
		log.Infof("Invocation status: %v", inv.InvocationStatus)

		// Check if invocation is complete
		if inv.InvocationStatus == int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS) {
			break
		}

		
		// Still running, wait and retry
		time.Sleep(pollInterval)
	}
	
	if inv == nil || (inv.InvocationStatus != int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS) &&
	                  inv.InvocationStatus != int64(ispb.InvocationStatus_PARTIAL_INVOCATION_STATUS)) {
		return nil, status.DeadlineExceededError("Timed out waiting for query to complete")
	}
	
	// Fetch the invocation and its build events to find the query result artifact
	var queryResultURI string
	_, err = build_event_handler.LookupInvocationWithCallback(ctx, s.env, invocationID, func(event *inpb.InvocationEvent) error {
		log.Infof("Build event: %v", event)

		// Look for NamedSetOfFiles events that might contain our query result
		switch p := event.GetBuildEvent().GetPayload().(type) {
		case *bespb.BuildEvent_NamedSetOfFiles:
			for _, file := range p.NamedSetOfFiles.GetFiles() {
				// Check if this is our query result file
				if strings.HasSuffix(file.GetName(), "query_result.pb") {
					queryResultURI = file.GetUri()
					// Found it, no need to process more events
					return io.EOF // Use EOF as a signal to stop processing
				}
			}
		}
		return nil
	})
	
	// If we got EOF, it means we found the file and stopped early (which is fine)
	if err != nil && err != io.EOF {
		return nil, status.WrapError(err, "failed to fetch invocation events")
	}
	
	// If we found the query result URI, download it from CAS
	if queryResultURI != "" {
		queryResult, err := s.downloadQueryResultFromCAS(ctx, queryResultURI)
		if err != nil {
			log.Warningf("Failed to download query result from CAS: %v", err)
			return &bqpb.QueryResult{}, nil
		}
		return queryResult, nil
	}
	
	// If we couldn't find the query result in the artifacts, return empty result
	log.Warningf("Query completed but could not find query_result.pb in artifacts for invocation %s", invocationID)
	return &bqpb.QueryResult{}, nil
}

// downloadQueryResultFromCAS downloads a file from CAS using its bytestream URI
func (s *QueryService) downloadQueryResultFromCAS(ctx context.Context, uri string) (*bqpb.QueryResult, error) {
	// Parse the bytestream URI to get the resource name
	// Format: bytestream://remote.buildbuddy.io:443/blobs/blake3/abc123/456
	u, err := url.Parse(uri)
	if err != nil {
		return nil, status.WrapError(err, "failed to parse bytestream URI")
	}
	
	if u.Scheme != "bytestream" {
		return nil, status.InvalidArgumentErrorf("expected bytestream URI, got %s", u.Scheme)
	}

	log.Infof("Downloading query result from CAS: %s", uri)
	
	// Connect to the cache
	cacheTarget := fmt.Sprintf("grpc://%s", u.Host) // todo figure out whether to use grpc or grpcs
	conn, err := grpc_client.DialSimple(cacheTarget)
	if err != nil {
		return nil, status.WrapError(err, "failed to connect to cache")
	}
	defer conn.Close()
	
	bsClient := bspb.NewByteStreamClient(conn)
	
	// Build the resource name from the URI path
	resourceName := strings.TrimPrefix(u.Path, "/")
	
	// Read the file from ByteStream
	stream, err := bsClient.Read(ctx, &bspb.ReadRequest{
		ResourceName: resourceName,
		ReadOffset:   0,
		ReadLimit:    0, // Read entire file
	})
	if err != nil {
		return nil, status.WrapError(err, "failed to read from bytestream")
	}
	
	// Collect the data
	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, status.WrapError(err, "failed to receive bytestream data")
		}
		data = append(data, resp.Data...)
	}
	
	// Unmarshal the proto
	queryResult := &bqpb.QueryResult{}
	if err := proto.Unmarshal(data, queryResult); err != nil {
		return nil, status.WrapError(err, "failed to unmarshal query result proto")
	}
	
	return queryResult, nil
}