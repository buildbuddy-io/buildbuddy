package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"golang.org/x/sync/errgroup"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (d *daemon) exportInvocations(ctx context.Context) {
	outCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *bbAPIKey)
	searchResp, err := d.serviceClient.SearchInvocation(outCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{
			GroupId: *bbOrgID,
		},
		Query: &inpb.InvocationQuery{
			GroupId:      *bbOrgID,
			UpdatedAfter: timestamppb.New(d.lastFinishedAt),
			Status:       []inpb.OverallStatus{inpb.OverallStatus_FAILURE, inpb.OverallStatus_SUCCESS},
		},
		Sort: &inpb.InvocationSort{
			SortField: inpb.InvocationSort_CREATED_AT_USEC_SORT_FIELD,
			Ascending: true,
		},
		Count: 100_000,
	})
	if err != nil {
		log.Fatalf("error searching invocations: %v", err)
	}

	d.processSearchResult(ctx, searchResp)
}

func (d *daemon) processSearchResult(ctx context.Context, searchResp *inpb.SearchInvocationResponse) {
	cachedInvocationIDs := d.c.loadCache()
	defer d.c.fileStore.Close()

	for _, in := range searchResp.GetInvocation() {
		if _, ok := cachedInvocationIDs[in.GetInvocationId()]; ok {
			log.Printf("skipping cached invocations: %s\n", in.InvocationId)
			continue
		}

		if err := d.processInvocation(ctx, in.GetInvocationId()); err != nil {
			// TODO: handle error
			log.Println(err)
			continue
		}

		cachedInvocationIDs[in.GetInvocationId()] = struct{}{}
	}

	err := d.c.writeCache(cachedInvocationIDs)
	if err != nil {
		log.Printf("error caching invocations: %v\n", err)
	}
}

func (d *daemon) processInvocation(ctx context.Context, invocationID string) error {
	invocationURL := fmt.Sprintf("https://app.buildbuddy.io/invocation/%s", invocationID)
	log.Printf("processing Invocation: %s\n", invocationURL)

	outCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *bbAPIKey)
	invocationResp, err := d.serviceClient.GetInvocation(outCtx, &inpb.GetInvocationRequest{
		RequestContext: &ctxpb.RequestContext{
			GroupId: *bbOrgID,
		},
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationID,
		},
	})
	if err != nil {
		return fmt.Errorf("error getting invocation: %v", err)
	}
	in := invocationResp.GetInvocation()[0]

	var wfConfigured *bespb.WorkflowConfigured
	var childConfigured *bespb.ChildInvocationsConfigured
	for _, e := range in.GetEvent() {
		if e.GetBuildEvent() == nil {
			break
		}
		if wfConfigured != nil && childConfigured != nil {
			break
		}

		if wfc := e.GetBuildEvent().GetWorkflowConfigured(); wfc != nil {
			wfConfigured = wfc
			continue
		}
		if c := e.GetBuildEvent().GetChildInvocationsConfigured(); c != nil {
			childConfigured = c
			continue
		}
	}

	startTime := time.UnixMicro(in.GetCreatedAtUsec())
	endTime := time.UnixMicro(in.GetCreatedAtUsec() + in.GetDurationUsec())
	ctx, iSpan := d.tracer.Start(ctx, in.GetInvocationId(), trace.WithTimestamp(startTime))
	defer iSpan.End(trace.WithTimestamp(endTime))

	if in.GetSuccess() {
		iSpan.SetStatus(codes.Ok, "")
	} else {
		iSpan.SetStatus(codes.Error, "")
	}
	iSpan.SetAttributes(attribute.String("user", in.GetUser()))
	iSpan.SetAttributes(attribute.String("host", in.GetHost()))
	iSpan.SetAttributes(attribute.String("command", in.GetCommand()))
	iSpan.SetAttributes(attribute.String("pattern", strings.Join(in.GetPattern(), " ")))
	iSpan.SetAttributes(attribute.String("status", in.GetInvocationStatus().String()))
	iSpan.SetAttributes(attribute.String("repo_url", in.GetRepoUrl()))
	iSpan.SetAttributes(attribute.String("commit_sha", in.GetCommitSha()))
	iSpan.SetAttributes(attribute.String("branch_name", in.GetBranchName()))
	iSpan.SetAttributes(attribute.String("bazel_exit_code", in.GetBazelExitCode()))
	iSpan.SetAttributes(attribute.String("url", invocationURL))
	iSpan.SetAttributes(attribute.Int64("created_at", in.GetCreatedAtUsec()))
	iSpan.SetAttributes(attribute.Int64("updated_at", in.GetUpdatedAtUsec()))
	iSpan.SetAttributes(attribute.Int64("duration_usec", in.GetDurationUsec()))
	iSpan.SetAttributes(attribute.Int64("action_count", in.GetActionCount()))
	iSpan.SetAttributes(attribute.Int64("attempt", int64(in.GetAttempt())))

	if wfConfigured != nil && childConfigured != nil {
		eg, ctx := errgroup.WithContext(ctx)
		for _, i := range childConfigured.GetInvocation() {
			id := i.GetInvocationId()
			eg.Go(func() error {
				return d.processInvocation(ctx, id)
			})
		}

		iSpan.SetAttributes(attribute.String("invocation_type", "workflow"))

		return eg.Wait()
	} else {
		iSpan.SetAttributes(attribute.String("invocation_type", "bazel"))
	}

	if cs := in.GetCacheStats(); cs != nil {
		iSpan.SetAttributes(attribute.Int64("ac_hits", cs.GetActionCacheHits()))
		iSpan.SetAttributes(attribute.Int64("ac_misses", cs.GetActionCacheMisses()))
		iSpan.SetAttributes(attribute.Int64("ac_uploads", cs.GetActionCacheUploads()))
		iSpan.SetAttributes(attribute.Int64("cas_hits", cs.GetCasCacheHits()))
		iSpan.SetAttributes(attribute.Int64("cas_misses", cs.GetCasCacheMisses()))
		iSpan.SetAttributes(attribute.Int64("cas_uploads", cs.GetCasCacheUploads()))

		iSpan.SetAttributes(attribute.Int64("total_download_size_bytes", cs.GetTotalDownloadSizeBytes()))
		iSpan.SetAttributes(attribute.Int64("total_upload_size_bytes", cs.GetTotalUploadSizeBytes()))
		iSpan.SetAttributes(attribute.Int64("total_download_transferred_size_bytes", cs.GetTotalDownloadTransferredSizeBytes()))
		iSpan.SetAttributes(attribute.Int64("total_upload_transferred_size_bytes", cs.GetTotalUploadTransferredSizeBytes()))
		iSpan.SetAttributes(attribute.Int64("total_download_usec", cs.GetTotalDownloadUsec()))
		iSpan.SetAttributes(attribute.Int64("total_upload_usec", cs.GetTotalDownloadUsec()))
		iSpan.SetAttributes(attribute.Int64("download_throughput_bytes_per_sec", cs.GetDownloadThroughputBytesPerSecond()))
		iSpan.SetAttributes(attribute.Int64("upload_throughput_bytes_per_sec", cs.GetUploadThroughputBytesPerSecond()))
		iSpan.SetAttributes(attribute.Int64("total_cached_action_exec_usec", cs.GetTotalCachedActionExecUsec()))
	}

	if profile := getProfileFromInvocation(in); profile != nil {
		if err := d.processBazelProfile(ctx, profile, startTime); err != nil {
			// ignore if profile was cleaned up
			if gstatus.Code(err) != gcodes.NotFound {
				log.Printf("failed to process profile for invocation %s: %v\n", in.GetInvocationId(), err)
			}
		}
	}

	return nil
}

func getProfileFromInvocation(in *inpb.Invocation) *bespb.File {
	for _, e := range in.GetEvent() {
		if e.GetBuildEvent() != nil && e.GetBuildEvent().GetBuildToolLogs() != nil {
			for _, l := range e.GetBuildEvent().GetBuildToolLogs().GetLog() {
				if l.GetUri() != "" && !strings.HasPrefix(l.GetUri(), "file://") && strings.Contains(l.GetName(), "profile") {
					return l
				}
			}
		}
	}

	return nil
}
