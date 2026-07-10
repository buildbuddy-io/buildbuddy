package view_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/util/download/downloadtest"
	"github.com/buildbuddy-io/buildbuddy/cli/view"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
)

const (
	invocationID = "abc12345-1234-1234-1234-123456789012"
	targetLabel  = "//foo:bar_test"
	testXMLURI   = "bytestream://localhost/blobs/deadbeef/123"
)

// testXML is a JUnit-style test.xml containing a failing case (TestFoo), a
// passing case (TestBar), and an errored case (TestBaz).
const testXML = `<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="foo" tests="3" failures="1" errors="1">
    <testcase name="TestFoo" classname="foo">
      <failure message="expected 1 got 2" type="">assertion failed
  at foo_test.go:10</failure>
    </testcase>
    <testcase name="TestBar" classname="foo"></testcase>
    <testcase name="TestBaz" classname="foo">
      <error message="panic: boom" type="">goroutine 1 [running]</error>
    </testcase>
  </testsuite>
</testsuites>`

type fakeBBClient struct {
	bbspb.BuildBuddyServiceClient
	getTarget func(req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error)
}

func (f *fakeBBClient) GetTarget(ctx context.Context, req *trpb.GetTargetRequest, opts ...grpc.CallOption) (*trpb.GetTargetResponse, error) {
	return f.getTarget(req)
}

// getTargetClient returns a client that serves a single target's events when
// fetched by label, and errors on any invocation-wide listing.
func getTargetClient(label string, events ...*bespb.BuildEvent) *fakeBBClient {
	return &fakeBBClient{getTarget: func(req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
		if req.GetTargetLabel() == "" {
			return nil, status.InternalError("unexpected invocation-wide listing")
		}
		if req.GetTargetLabel() != label {
			return &trpb.GetTargetResponse{}, nil
		}
		return targetResponse(label, events...), nil
	}}
}

func targetResponse(label string, events ...*bespb.BuildEvent) *trpb.GetTargetResponse {
	return &trpb.GetTargetResponse{
		TargetGroups: []*trpb.TargetGroup{{
			Targets: []*trpb.Target{{
				Metadata:         &trpb.TargetMetadata{Label: label},
				TestResultEvents: events,
			}},
		}},
	}
}

// listingResponse builds the metadata-only response returned for an
// invocation-wide listing of a given status.
func listingResponse(status cmpb.Status, labels ...string) *trpb.GetTargetResponse {
	g := &trpb.TargetGroup{Status: status}
	for _, l := range labels {
		g.Targets = append(g.Targets, &trpb.Target{Metadata: &trpb.TargetMetadata{Label: l}})
	}
	return &trpb.GetTargetResponse{TargetGroups: []*trpb.TargetGroup{g}}
}

func failedTestEvent(label, uri string, statusVal bespb.TestStatus, run, attempt int32) *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_TestResult{
			TestResult: &bespb.BuildEventId_TestResultId{Label: label, Run: run, Attempt: attempt},
		}},
		Payload: &bespb.BuildEvent_TestResult{TestResult: &bespb.TestResult{
			Status: statusVal,
			TestActionOutput: []*bespb.File{{
				Name: "test.xml",
				File: &bespb.File_Uri{Uri: uri},
			}},
		}},
	}
}

func TestViewFilteredTestOutput_ExplicitTargetWithFilter(t *testing.T) {
	event := failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 2 /*run*/, 1 /*attempt*/)
	bb := getTargetClient(targetLabel, event)
	dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, []string{targetLabel}, "TestFoo")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	out := buf.String()
	require.Contains(t, out, targetLabel)
	// Output should include the failing test case.
	require.Contains(t, out, "expected 1 got 2")
	require.Contains(t, out, "assertion failed")
	// The non-matching errored case must not be printed.
	require.NotContains(t, out, "panic: boom")
}

func TestViewFilteredTestOutput_ExplicitTargetNoFilterPrintsAllFailures(t *testing.T) {
	event := failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 0, 0)
	bb := getTargetClient(targetLabel, event)
	dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, []string{targetLabel}, "")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	out := buf.String()
	// An empty filter prints every failed case, but not the passing case.
	require.Contains(t, out, "expected 1 got 2")
	require.Contains(t, out, "panic: boom")
}

func TestViewFilteredTestOutput_MultipleTargets(t *testing.T) {
	const otherLabel = "//baz:qux_test"
	const otherURI = "bytestream://localhost/blobs/cafef00d/45"
	const otherXML = `<testsuites><testsuite name="baz">` +
		`<testcase name="TestQux"><failure message="qux failed"></failure></testcase>` +
		`</testsuite></testsuites>`

	bb := &fakeBBClient{getTarget: func(req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
		switch req.GetTargetLabel() {
		case targetLabel:
			return targetResponse(targetLabel, failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 0, 0)), nil
		case otherLabel:
			return targetResponse(otherLabel, failedTestEvent(otherLabel, otherURI, bespb.TestStatus_FAILED, 0, 0)), nil
		default:
			return nil, status.InternalErrorf("unexpected label %q", req.GetTargetLabel())
		}
	}}
	dl := downloadtest.New().
		Add(testXMLURI, []byte(testXML)).
		Add(otherURI, []byte(otherXML))

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, []string{targetLabel, otherLabel}, "")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	out := buf.String()
	require.Contains(t, out, targetLabel)
	require.Contains(t, out, otherLabel)
	require.Contains(t, out, "expected 1 got 2")
	require.Contains(t, out, "qux failed")
}

func TestViewFilteredTestOutput_NoMatchingFailedCases(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pattern string
	}{
		{name: "pattern matches nothing", pattern: "TestNope"},
		{name: "pattern matches only passing test", pattern: "TestBar"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 0, 0)
			bb := getTargetClient(targetLabel, event)
			dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

			var buf bytes.Buffer
			code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, []string{targetLabel}, tc.pattern)

			require.NoError(t, err)
			require.Equal(t, 0, code)
			require.Empty(t, buf.String())
		})
	}
}

func TestViewFilteredTestOutput_NoTargetSpecifiedWithTestFilter(t *testing.T) {
	bb := &fakeBBClient{getTarget: func(req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
		// First pass: metadata-only listing keyed by status.
		if req.GetTargetLabel() == "" {
			if req.GetStatus() == cmpb.Status_FAILED {
				return listingResponse(cmpb.Status_FAILED, targetLabel), nil
			}
			return listingResponse(req.GetStatus()), nil // no FLAKY/TIMED_OUT targets
		}
		// Second pass: hydrate the discovered target.
		require.Equal(t, targetLabel, req.GetTargetLabel())
		return targetResponse(targetLabel, failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 0, 0)), nil
	}}
	dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, nil, "TestFoo")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Contains(t, buf.String(), "expected 1 got 2")
}

func TestViewFilteredTestOutput_TargetWithoutTestResults(t *testing.T) {
	// A non-test (or untested) target resolves via GetTarget but carries no test
	// result events; this exits cleanly without printing or downloading anything.
	bb := getTargetClient(targetLabel /* no events */)
	dl := downloadtest.New()

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, []string{targetLabel}, "")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Empty(t, buf.String())
}
