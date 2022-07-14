package build_logs

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

// invocationLog fetches and buffers logs for an invocation.
//
// Only anonymous invocations are supported currently.
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

func newUUID(t *testing.T) string {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	return id.String()
}

func publishStarted(t *testing.T, bep *build_event_publisher.Publisher) {
	startTime := time.Now()
	err := bep.Publish(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{
				StartTime:          timestamppb.New(startTime),
				OptionsDescription: "--bes_backend=foo",
			},
		},
	})
	require.NoError(t, err)
}

func publishProgress(t *testing.T, bep *build_event_publisher.Publisher, stdout, stderr string) {
	err := bep.Publish(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Progress{
			Progress: &bespb.Progress{
				Stdout: stdout,
				Stderr: stderr,
			},
		},
	})
	require.NoError(t, err)
}

func publishFinished(t *testing.T, bep *build_event_publisher.Publisher) {
	finishTime := time.Now()
	err := bep.Publish(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{
				ExitCode:   &bespb.BuildFinished_ExitCode{Name: "OK", Code: 0},
				FinishTime: timestamppb.New(finishTime),
			},
		},
	})
	require.NoError(t, err)
}

func waitForInvocation(t *testing.T, ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID string) {
	for delay := 1 * time.Microsecond; delay < time.Second; delay *= 2 {
		_, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
			Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
		})
		if err == nil {
			return
		}
		if !status.IsNotFoundError(err) && !strings.Contains(err.Error(), "not found") {
			require.FailNowf(t, "wait for invocation", "Error while waiting for invocation: %s", err)
		}
		time.Sleep(delay)
	}
	require.FailNowf(t, "timed out", "Timed out waiting for invocation %q to be created", invocationID)
}

func getLogs(t *testing.T, ctx context.Context, log *invocationLog) string {
	logs, err := log.Get(ctx)
	require.NoError(t, err)
	return logs
}

func waitForLogsToEqual(t *testing.T, ctx context.Context, log *invocationLog, expected string) {
	for delay := 1 * time.Microsecond; delay < time.Second; delay *= 2 {
		logs := getLogs(t, ctx, log)
		if logs == expected {
			return
		}
		time.Sleep(delay)
	}

	// One last attempt, showing a nice diff if it fails.
	logs := getLogs(t, ctx, log)
	require.Equal(t, expected, logs)
}

func TestBuildLogs_CompletedInvocation(t *testing.T) {
	bb := buildbuddy_enterprise.Run(t, "--storage.enable_chunked_event_logs=true")
	bbClient := bb.BuildBuddyServiceClient(t)
	iid := newUUID(t)
	bep, err := build_event_publisher.New(bb.GRPCAddress(), "" /*=apiKey*/, iid)
	require.NoError(t, err)
	ctx := context.Background()
	bep.Start(ctx)
	bepClosed := false
	defer func() {
		// Close the build event publisher in case the test failed before we made it
		// to the part where the stream is closed.
		if !bepClosed {
			err := bep.Finish()
			require.NoError(t, err)
		}
	}()
	log := NewInvocationLog(bbClient, iid)

	publishStarted(t, bep)

	expected := &bytes.Buffer{}
	for p := 0; p < 100; p++ {
		stderr := ""
		stdout := ""
		for line := 0; line < 100; line++ {
			stderr += fmt.Sprintf("stderr event %d, line %d\n", p, line)
			stdout += fmt.Sprintf("stdout event %d, line %d\n", p, line)
		}
		// Note: The server should write the logs as stderr first, followed by
		// stdout. The build event protocol doesn't have a way of representing the
		// timing of each byte written to stdout or stderr, so we just make the
		// arbitrary assumption that all stdout bytes were written after all stderr
		// bytes.
		expected.WriteString(stderr)
		expected.WriteString(stdout)

		publishProgress(t, bep, stdout, stderr)
	}

	publishFinished(t, bep)
	// Close the BEP stream -- this should not return until all events are ACK'd
	// by the server. The server sends ACKs only after all events are processed,
	// so the logs should be available when this returns.
	err = bep.Finish()
	bepClosed = true
	require.NoError(t, err)

	logContents := getLogs(t, ctx, log)
	// TODO(tempoz): avoid needing to strip the trailing newline for the assertion
	// to pass
	require.Equal(t, strings.TrimSuffix(expected.String(), "\n"), logContents)
}

func TestBuildLogs_InProgressInvocation(t *testing.T) {
	bb := buildbuddy_enterprise.Run(t, "--storage.enable_chunked_event_logs=true")
	bbClient := bb.BuildBuddyServiceClient(t)
	iid := newUUID(t)
	bep, err := build_event_publisher.New(bb.GRPCAddress(), "" /*=apiKey*/, iid)
	require.NoError(t, err)
	ctx := context.Background()
	bep.Start(ctx)
	defer func() {
		// Close the BEP stream during cleanup since we keep it open for the
		// duration of the test (since we are testing "in progress" invocations)
		err := bep.Finish()
		require.NoError(t, err)
	}()
	log := NewInvocationLog(bbClient, iid)

	publishStarted(t, bep)

	expected := &bytes.Buffer{}
	for p := 0; p < 100; p++ {
		stderr := ""
		stdout := ""
		for line := 0; line < 100; line++ {
			stderr += fmt.Sprintf("stderr event %d, line %d\n", p, line)
			stdout += fmt.Sprintf("stdout event %d, line %d\n", p, line)
		}
		// Note: The server should write the logs as stderr first, followed by
		// stdout. The build event protocol doesn't have a way of representing the
		// timing of each byte written to stdout or stderr, so we just make the
		// arbitrary assumption that all stdout bytes were written after all stderr
		// bytes.
		expected.WriteString(stderr)
		expected.WriteString(stdout)

		publishProgress(t, bep, stdout, stderr)
	}

	// Since our stream is still open (to test the "in progress" state), there are
	// no guarantees that the server has processed any build events, so we need to
	// poll.

	// Wait for the invocation to be created, otherwise the EventLog lookup may
	// fail with a "record not found" error when it looks up the invocation.
	waitForInvocation(t, ctx, bbClient, iid)
	// TODO(tempoz): avoid needing to strip the trailing newline for the assertion
	// to pass
	waitForLogsToEqual(t, ctx, log, strings.TrimSuffix(expected.String(), "\n"))
}
