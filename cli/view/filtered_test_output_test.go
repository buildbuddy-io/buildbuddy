package view_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/util/download/downloadtest"
	"github.com/buildbuddy-io/buildbuddy/cli/view"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

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

func bbClientReturning(resp *trpb.GetTargetResponse, err error) *fakeBBClient {
	return &fakeBBClient{getTarget: func(*trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
		return resp, err
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

func TestViewFilteredTestOutput_InvalidFilter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter string
	}{
		{name: "no target label", filter: "//foo/bar"},
		{name: "missing test name", filter: "//foo:bar_test"},
		{name: "invalid regex", filter: "//foo:bar_test.("},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// GetTarget should never be reached for a malformed filter.
			bb := &fakeBBClient{getTarget: func(*trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
				t.Fatalf("GetTarget should not be called for filter %q", tc.filter)
				return nil, nil
			}}

			code, err := view.ViewFilteredTestOutput(context.Background(), bb, downloadtest.New(), &bytes.Buffer{}, invocationID, tc.filter)
			require.Error(t, err)
			require.Equal(t, 1, code)
		})
	}
}

func TestViewFilteredTestOutput(t *testing.T) {
	event := failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 2 /*run*/, 1 /*attempt*/)
	bb := bbClientReturning(targetResponse(targetLabel, event), nil)
	dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, targetLabel+".TestFoo")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	out := buf.String()
	require.Contains(t, out, targetLabel)
	require.Contains(t, out, "expected 1 got 2")
	require.Contains(t, out, "assertion failed")
	// Non-matching and passing cases must not be printed.
	require.NotContains(t, out, "TestBaz")
	require.NotContains(t, out, "TestBar")
}

func TestViewFilteredTestOutput_RegexMatchesMultipleCases(t *testing.T) {
	event := failedTestEvent(targetLabel, testXMLURI, bespb.TestStatus_FAILED, 0, 0)
	bb := bbClientReturning(targetResponse(targetLabel, event), nil)
	dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

	var buf bytes.Buffer
	code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, targetLabel+".Test(Foo|Baz)")

	require.NoError(t, err)
	require.Equal(t, 0, code)
	out := buf.String()
	require.Contains(t, out, "expected 1 got 2")
	require.Contains(t, out, "panic: boom")
	// The passing case is never printed.
	require.NotContains(t, out, "TestBar")
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
			bb := bbClientReturning(targetResponse(targetLabel, event), nil)
			dl := downloadtest.New().Add(testXMLURI, []byte(testXML))

			var buf bytes.Buffer
			code, err := view.ViewFilteredTestOutput(context.Background(), bb, dl, &buf, invocationID, targetLabel+"."+tc.pattern)

			require.NoError(t, err)
			require.Equal(t, 0, code)
			require.Empty(t, buf.String())
		})
	}
}
