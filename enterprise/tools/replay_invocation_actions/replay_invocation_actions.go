package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	bbpb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	src        = flag.String("source_executor", "remote.buildbuddy.io", "Source remote executor target")
	groupsFile = flag.String("groups_file", "", "File containing GROUP_ID API_KEY pairs (one per line)")

	dst        = flag.String("target_executor", "remote.buildbuddy.dev", "Destination remote executor target")
	dstAPIKey  = flag.String("target_api_key", "", "Destination BB API key")
	resultsDir = flag.String("results_dir", "", "Directory for storing results")

	replayJobs = flag.Int("replay_jobs", 64, "Max number of replay_action commands to run concurrently.")
	lookback   = flag.Duration("lookback", 24*time.Hour, "Invocation lookback window")
	limit      = flag.Int("executions_limit", 100_000, "Max number of executions to replay")
)

// Populated by x_defs in BUILD file
var replayActionRlocationpath string

var (
	notFoundRegexp = regexp.MustCompile(`Execute stream recv failed: rpc error: code = FailedPrecondition desc = Digest .* not found`)
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type group struct{ GroupID, APIKey string }

func run() error {
	ctx := context.Background()

	if *resultsDir == "" {
		return fmt.Errorf("missing -results_dir")
	}
	if *groupsFile == "" {
		return fmt.Errorf("missing -groups_file")
	}

	replayActionPath, err := runfiles.Rlocation(replayActionRlocationpath)
	if err != nil {
		return fmt.Errorf("rlocation: %w", err)
	}

	// Upload the replay_action binary to the target executor - the replay
	// binary itself will run remotely, so that when copying files, we aren't
	// limited to the bandwidth of a single machine.
	dstConn, err := grpc_client.DialSimple(*dst)
	if err != nil {
		return fmt.Errorf("dial target executor: %w", err)
	}
	defer dstConn.Close()
	dstCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *dstAPIKey)
	bs := bytestream.NewByteStreamClient(dstConn)
	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(bs)
	env.SetActionCacheClient(repb.NewActionCacheClient(dstConn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(dstConn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(dstConn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(dstConn))

	var replayActionInputRootDigest *repb.Digest
	log.Infof("Uploading replay_action binary")
	{
		f, err := os.Open(replayActionPath)
		if err != nil {
			return fmt.Errorf("open replay_action binary: %w", err)
		}
		defer f.Close()
		binDigest, err := cachetools.UploadBlob(dstCtx, bs, "", repb.DigestFunction_SHA256, f)
		if err != nil {
			return fmt.Errorf("upload replay_action binary: %w", err)
		}
		inputRoot := &repb.Directory{
			Files: []*repb.FileNode{
				{Name: "replay_action", Digest: binDigest, IsExecutable: true},
			},
		}
		inputRootDigest, err := cachetools.UploadProto(dstCtx, bs, "", repb.DigestFunction_SHA256, inputRoot)
		if err != nil {
			return fmt.Errorf("upload replay_action input_root Directory: %w", err)
		}
		replayActionInputRootDigest = inputRootDigest
	}
	remoteReplayAction := func(ctx context.Context, args []string) (*interfaces.CommandResult, error) {
		rn, err := rexec.Prepare(ctx, env, "", repb.DigestFunction_SHA256, &repb.Action{
			InputRootDigest: replayActionInputRootDigest,
			Timeout:         durationpb.New(1 * time.Hour),
			DoNotCache:      true,
		}, &repb.Command{
			Arguments: append([]string{"./replay_action"}, args...),
		}, "")
		if err != nil {
			return nil, fmt.Errorf("prepare remote replay: %w", err)
		}
		stream, err := rexec.Start(ctx, env, rn)
		if err != nil {
			return nil, fmt.Errorf("start remote replay: %w", err)
		}
		rsp, err := rexec.Wait(stream)
		if err != nil {
			return nil, fmt.Errorf("remote replay: %w", err)
		}
		ar, err := rexec.GetResult(ctx, env, "", repb.DigestFunction_SHA256, rsp.ExecuteResponse.GetResult())
		if err != nil {
			return nil, fmt.Errorf("get remote replay result: %w", err)
		}
		return ar, nil
	}

	// Make results dir
	if err := os.MkdirAll(*resultsDir, 0755); err != nil {
		return fmt.Errorf("make results dir: %w", err)
	}

	// Read groups file
	b, err := os.ReadFile(*groupsFile)
	if err != nil {
		return fmt.Errorf("read groups file %q: %w", *groupsFile, err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	var groups []*group
	for _, line := range lines {
		line, _, _ = strings.Cut(line, "#")
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		groups = append(groups, &group{GroupID: fields[0], APIKey: fields[1]})
	}

	conn, err := grpc_client.DialSimpleWithoutPooling(*src)
	if err != nil {
		return fmt.Errorf("dial %q: %w", *src, err)
	}
	defer conn.Close()
	bb := bbpb.NewBuildBuddyServiceClient(conn)

	histories := map[string]chan string{}
	for _, g := range groups {
		histories[g.GroupID] = InvocationHistory(ctx, bb, g.GroupID, g.APIKey)
	}

	type executions struct {
		*espb.GetExecutionResponse
		Group *group
	}

	var replayGroup errgroup.Group
	replayGroup.SetLimit(*replayJobs)

	executeResponseCh := make(chan *executions, 128)
	doneExecuting := make(chan struct{})
	var count atomic.Int64
	go func() {
		defer func() {
			_ = replayGroup.Wait()
			close(doneExecuting)
		}()
		// Keep track of unique execution IDs seen, and print each only once
		executionIDs := map[string]bool{}
		for rsp := range executeResponseCh {
			for _, execution := range rsp.GetExecution() {
				if executionIDs[execution.GetExecutionId()] {
					continue
				}
				executionIDs[execution.GetExecutionId()] = true
				if execution.GetExitCode() != 0 || execution.GetStatus().GetCode() != 0 {
					continue
				}
				rn, err := digest.ParseUploadResourceName(execution.GetExecutionId())
				if err != nil {
					log.Errorf("Failed to parse execution ID: %s", err)
					continue
				}
				dl, err := rn.DownloadString()
				if err != nil {
					log.Errorf("Failed to create resource name string: %s", err)
					continue
				}
				ad := dl[len(rn.GetInstanceName()):]
				args := []string{
					"--source_executor=" + *src,
					"--source_api_key=" + rsp.Group.APIKey,
					"--source_remote_instance_name=" + rn.GetInstanceName(),
					"--action_digest=" + ad,
					"--target_executor=" + *dst,
					"--target_api_key=" + *dstAPIKey,
				}
				n := count.Add(1)
				outPath := filepath.Join(*resultsDir, fmt.Sprintf("%d", n))
				if err := os.MkdirAll(outPath, 0755); err != nil {
					log.Fatalf("Failed to create %s: %s", outPath, err)
				}
				replayGroup.Go(func() error {
					width := fmt.Sprintf("%d", len(fmt.Sprintf("%d", *limit)))
					cmdName := fmt.Sprintf("Command %"+width+"d, %s", n, rsp.Group.GroupID)

					// cmd := exec.Command(replayActionPath, args...)
					var stdoutW, stderrW io.Writer
					if err := os.WriteFile(outPath+"/args", []byte(strings.Join(args, " ")), 0644); err != nil {
						log.Errorf("Failed to write replay args: %s", err)
					}
					stdout, err := os.OpenFile(outPath+"/stdout", os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0644)
					if err != nil {
						log.Errorf("Failed to create stdout file: %s", err)
					}
					defer stdout.Close()
					// cmd.Stdout = stdout
					stdoutW = stdout
					stderr, err := os.OpenFile(outPath+"/stderr", os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0644)
					if err != nil {
						log.Errorf("Failed to create stderr file: %s", err)
					}
					defer stderr.Close()
					stderrBuf := &bytes.Buffer{}
					// cmd.Stderr = io.MultiWriter(stderr, stderrBuf)
					stderrW = io.MultiWriter(stderr, stderrBuf)
					// if err := cmd.Run(); err != nil {

					res, err := remoteReplayAction(dstCtx, args)
					if err != nil {
						log.Fatalf("Remote replay failed: %s", err)
						return nil
					}
					stdoutW.Write(res.Stdout)
					stderrW.Write(res.Stderr)
					err = res.Error
					if err == nil && res.ExitCode != 0 {
						err = fmt.Errorf("exit code %d", res.ExitCode)
					}

					if err != nil {
						// Ignore "digest not found" errors - these happen if
						// one of the files needed to replay the action just
						// expired from cache.
						if notFoundRegexp.MatchString(stderrBuf.String()) {
							log.Infof("%s: expired from cache", cmdName)
							return nil
						}

						log.Warningf("%s: error: %s. Outputs written to %s/", cmdName, err, outPath)
						_ = os.WriteFile(outPath+"/error", []byte(err.Error()+"\n"), 0644)
						os.Stderr.Write(stderrBuf.Bytes())
					} else {
						log.Infof("%s: OK. Outputs written to %s/", cmdName, outPath)
					}
					return nil
				})
			}
		}
	}()

	var executionFetchGroup errgroup.Group
	executionFetchGroup.SetLimit(16)

	i := -1
	for int(count.Load()) < *limit {
		if len(groups) == 0 {
			log.Infof("No more groups")
			break
		}
		i = (i + 1) % len(groups)
		g := groups[i]
		iid, ok := <-histories[g.GroupID]
		if !ok {
			log.Infof("Removing group %s: %s", g.GroupID, err)
			groups = append(groups[:i], groups[i+1:]...)
			continue
		}
		executionFetchGroup.Go(func() error {
			ctx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", g.APIKey)
			log.Infof("Fetching executions for %s", iid)
			rsp, err := bb.GetExecution(ctx, &espb.GetExecutionRequest{
				RequestContext:  &ctxpb.RequestContext{GroupId: g.GroupID},
				ExecutionLookup: &espb.ExecutionLookup{InvocationId: iid},
			})
			if err != nil {
				log.Warningf("Failed to fetch executions for invocation %s: %s", iid, err)
				return nil
			}
			executeResponseCh <- &executions{GetExecutionResponse: rsp, Group: g}
			return nil
		})
	}

	if err := executionFetchGroup.Wait(); err != nil {
		return err
	}
	close(executeResponseCh)
	<-doneExecuting
	return nil
}

// Returns a channel with a stream of invocation IDs for the given group.
func InvocationHistory(ctx context.Context, client bbpb.BuildBuddyServiceClient, groupID, groupAPIKey string) chan string {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", groupAPIKey)
	iids := make(chan string, 128)
	go func() {
		defer close(iids)
		pageToken := ""
		for {
			log.Infof("Fetching invocations for %s", groupID)
			res, err := client.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
				RequestContext: &ctxpb.RequestContext{GroupId: groupID},
				PageToken:      pageToken,
				Query: &inpb.InvocationQuery{
					GroupId: groupID,
					Role:    []string{"CI"},
					Status:  []inspb.OverallStatus{inspb.OverallStatus_SUCCESS},
				},
			})
			if err != nil {
				log.Errorf("Failed to fetch invocation history for %s: %s", groupID, err)
				return
			}
			if len(res.GetInvocation()) == 0 {
				log.Infof("No more invocations for %s", groupID)
				return
			}
			for _, inv := range res.GetInvocation() {
				iids <- inv.GetInvocationId()
			}
			pageToken = res.GetNextPageToken()
		}
	}()
	return iids
}
