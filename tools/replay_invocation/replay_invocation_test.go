package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	espb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/junit"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
)

func uriFile(name string) *espb.File {
	return &espb.File{Name: name, File: &espb.File_Uri{Uri: "bytestream://remote.buildbuddy.io/blobs/hash/1"}}
}

func localFile(name string) *espb.File {
	return &espb.File{Name: name, File: &espb.File_Uri{Uri: "file:///tmp/" + name}}
}

func TestErrorTrackingArtifactCandidatesFailedAction(t *testing.T) {
	event := &espb.BuildEvent{Payload: &espb.BuildEvent_Action{Action: &espb.ActionExecuted{
		Success: false,
		Stderr:  uriFile("stderr"),
		Stdout:  uriFile("stdout"),
	}}}

	candidates := errorTrackingArtifactCandidates(event)
	if len(candidates) != 2 {
		t.Fatalf("got %d candidates, want 2", len(candidates))
	}
	for _, candidate := range candidates {
		if candidate.maxBytes != maxImportedActionOutputBytes || !candidate.mayTruncate {
			t.Fatalf("unexpected action candidate limits: %+v", candidate)
		}
	}
}

func TestErrorTrackingArtifactCandidatesOnlyFailedTestDiagnostics(t *testing.T) {
	event := &espb.BuildEvent{Payload: &espb.BuildEvent_TestResult{TestResult: &espb.TestResult{
		Status: espb.TestStatus_FAILED,
		TestActionOutput: []*espb.File{
			uriFile("test.xml"),
			uriFile("test.log"),
			uriFile("coverage.dat"),
			localFile("test.xml"),
			{Name: "inline-test.xml", File: &espb.File_Contents{Contents: []byte("already in BES")}},
		},
	}}}

	candidates := errorTrackingArtifactCandidates(event)
	if len(candidates) != 2 {
		t.Fatalf("got %d candidates, want 2", len(candidates))
	}
	if got := candidates[0]; got.file.GetName() != "test.xml" || got.maxBytes != maxImportedTestXMLBytes || got.mayTruncate {
		t.Fatalf("unexpected XML candidate: %+v", got)
	}
	if got := candidates[1]; got.file.GetName() != "test.log" || got.maxBytes != maxImportedTestLogBytes || !got.mayTruncate {
		t.Fatalf("unexpected log candidate: %+v", got)
	}
}

func TestErrorTrackingArtifactCandidatesIgnoreSuccessfulEvents(t *testing.T) {
	tests := []*espb.BuildEvent{
		{Payload: &espb.BuildEvent_Action{Action: &espb.ActionExecuted{Success: true, Stderr: uriFile("stderr")}}},
		{Payload: &espb.BuildEvent_TestResult{TestResult: &espb.TestResult{Status: espb.TestStatus_PASSED, TestActionOutput: []*espb.File{uriFile("test.xml")}}}},
		{Payload: &espb.BuildEvent_TestResult{TestResult: &espb.TestResult{Status: espb.TestStatus_FLAKY, TestActionOutput: []*espb.File{uriFile("test.xml")}}}},
	}
	for _, event := range tests {
		if got := errorTrackingArtifactCandidates(event); len(got) != 0 {
			t.Fatalf("got candidates for successful event: %+v", got)
		}
	}
}

func TestScrubStartedAPIKey(t *testing.T) {
	options := "--remote_header=x-buildbuddy-api-key=SOURCE_SECRET --bes_header 'x-buildbuddy-api-key OTHER_SECRET'"
	if got, want := scrubStartedAPIKey(options, ""), "--remote_header=x-buildbuddy-api-key=<REDACTED> --bes_header 'x-buildbuddy-api-key=<REDACTED>'"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := scrubStartedAPIKey(options, "DESTINATION_KEY"); got != "--remote_header=x-buildbuddy-api-key=DESTINATION_KEY --bes_header 'x-buildbuddy-api-key=DESTINATION_KEY'" {
		t.Fatalf("unexpected destination substitution: %q", got)
	}
}

func TestParseSourceBaseURLRequiresHTTPSExceptLoopback(t *testing.T) {
	for _, rawURL := range []string{"https://buildbuddy.example", "http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"} {
		if _, err := parseSourceBaseURL(rawURL); err != nil {
			t.Errorf("parseSourceBaseURL(%q): %s", rawURL, err)
		}
	}
	for _, rawURL := range []string{"http://buildbuddy.example", "https://user:password@buildbuddy.example", "https://buildbuddy.example?key=value", "file:///tmp/events"} {
		if _, err := parseSourceBaseURL(rawURL); err == nil {
			t.Errorf("parseSourceBaseURL(%q) unexpectedly succeeded", rawURL)
		}
	}
}

func TestParseSourceArtifactHosts(t *testing.T) {
	hosts, err := parseSourceArtifactHosts([]string{"buildbuddy.example", "cache.example:443"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 2 {
		t.Fatalf("got %d hosts, want 2", len(hosts))
	}
	for _, invalid := range [][]string{nil, {"https://cache.example"}, {"cache.example/path"}, {"user@cache.example"}} {
		if _, err := parseSourceArtifactHosts(invalid); err == nil {
			t.Errorf("parseSourceArtifactHosts(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestRedactRemoteReplayEvent(t *testing.T) {
	const sourceKey = "SOURCE_SECRET_VALUE"
	r := redact.NewStreamingRedactor()
	started := &espb.BuildEvent{Payload: &espb.BuildEvent_Started{Started: &espb.BuildStarted{
		OptionsDescription: "--bes_header=x-buildbuddy-api-key=" + sourceKey,
	}}}
	if err := redactRemoteReplayEvent(context.Background(), r, sourceKey, started); err != nil {
		t.Fatal(err)
	}
	if got := started.GetStarted().GetOptionsDescription(); got != "<REDACTED>" {
		t.Fatalf("started options = %q, want fully redacted", got)
	}

	commandLine := &espb.BuildEvent{Payload: &espb.BuildEvent_UnstructuredCommandLine{
		UnstructuredCommandLine: &espb.UnstructuredCommandLine{Args: []string{"bazel", "test", "--token=" + sourceKey}},
	}}
	if err := redactRemoteReplayEvent(context.Background(), r, sourceKey, commandLine); err != nil {
		t.Fatal(err)
	}
	if got := commandLine.GetUnstructuredCommandLine().GetArgs(); len(got) != 0 {
		t.Fatalf("unstructured command line was retained: %q", got)
	}

	progress := &espb.BuildEvent{Payload: &espb.BuildEvent_Progress{Progress: &espb.Progress{Stderr: "token=" + sourceKey}}}
	if err := redactRemoteReplayEvent(context.Background(), r, sourceKey, progress); err != nil {
		t.Fatal(err)
	}
	if got := progress.GetProgress().GetStderr(); strings.Contains(got, sourceKey) || !strings.Contains(got, "<REDACTED>") {
		t.Fatalf("source key was not redacted from progress: %q", got)
	}
}

func TestRedactInlineErrorTrackingArtifacts(t *testing.T) {
	const sourceKey = "source-api-key"
	const embeddedAPIKey = "apikeyexactly20chars"
	secretText := "x-buildbuddy-api-key=" + embeddedAPIKey + " https://user:password@example.com/path " + sourceKey
	event := &espb.BuildEvent{Payload: &espb.BuildEvent_Action{Action: &espb.ActionExecuted{
		Success: false,
		Stderr:  &espb.File{Name: "stderr", File: &espb.File_Contents{Contents: []byte(secretText)}},
	}}}

	redactInlineErrorTrackingArtifacts(event, sourceKey)
	got := string(event.GetAction().GetStderr().GetContents())
	for _, secret := range []string{embeddedAPIKey, "user:password", sourceKey} {
		if strings.Contains(got, secret) {
			t.Errorf("inline diagnostic retained %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "<REDACTED>") {
		t.Fatalf("inline diagnostic did not contain a redaction marker: %q", got)
	}

	xmlText := `<testsuite name="suite"><testcase name="fails"><failure message="https://user:password@example.com/ ` + sourceKey + `">` + sourceKey + `</failure></testcase></testsuite>`
	xmlEvent := &espb.BuildEvent{Payload: &espb.BuildEvent_TestResult{TestResult: &espb.TestResult{
		Status:           espb.TestStatus_FAILED,
		TestActionOutput: []*espb.File{{Name: "test.xml", File: &espb.File_Contents{Contents: []byte(xmlText)}}},
	}}}
	redactInlineErrorTrackingArtifacts(xmlEvent, sourceKey)
	redactedXML := xmlEvent.GetTestResult().GetTestActionOutput()[0].GetContents()
	cases, err := junit.Parse(bytes.NewReader(redactedXML), junit.DefaultLimits())
	if err != nil {
		t.Fatalf("parse redacted inline JUnit XML: %s\n%s", err, redactedXML)
	}
	if len(cases) != 1 || cases[0].Name != "fails" || !strings.Contains(cases[0].Failures[0].Message, "[REDACTED]") {
		t.Fatalf("unexpected redacted inline JUnit result: %+v", cases)
	}
}

func bytestreamURI(t *testing.T, host string, b []byte, compressor repb.Compressor_Value) string {
	t.Helper()
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	r := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	r.SetCompressor(compressor)
	return (&url.URL{Scheme: "bytestream", Host: host, Path: "/" + r.DownloadString()}).String()
}

func testArtifactImporter(t *testing.T, sourceURL string, allowedHosts ...string) *errorTrackingArtifactImporter {
	t.Helper()
	baseURL, err := parseSourceBaseURL(sourceURL)
	if err != nil {
		t.Fatal(err)
	}
	hosts, err := parseSourceArtifactHosts(allowedHosts)
	if err != nil {
		t.Fatal(err)
	}
	return &errorTrackingArtifactImporter{
		client:              newSourceHTTPClient(),
		sourceBaseURL:       baseURL,
		sourceArtifactHosts: hosts,
		sourceInvocationID:  "source-invocation",
		sourceAPIKey:        "source-api-key",
		destinationHost:     "localhost:1985",
	}
}

func TestErrorTrackingArtifactImporterDownloadsTruncatesAndRewrites(t *testing.T) {
	const sourceHost = "cache.example"
	sourceBytes := []byte("source-api-key:0123456789")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-buildbuddy-api-key"); got != "source-api-key" {
			t.Errorf("API key header = %q", got)
		}
		if got := r.URL.Query().Get("invocation_id"); got != "source-invocation" {
			t.Errorf("invocation_id = %q", got)
		}
		_, _ = w.Write(sourceBytes)
	}))
	defer server.Close()

	importer := testArtifactImporter(t, server.URL, sourceHost)
	var uploaded []byte
	importer.upload = func(_ context.Context, _ string, digestFunction repb.DigestFunction_Value, b []byte) error {
		if digestFunction != repb.DigestFunction_SHA256 {
			t.Errorf("digest function = %s", digestFunction)
		}
		uploaded = append([]byte(nil), b...)
		return nil
	}
	file := &espb.File{Name: "test.log", File: &espb.File_Uri{Uri: bytestreamURI(t, sourceHost, sourceBytes, repb.Compressor_IDENTITY)}}
	if err := importer.importCandidate(context.Background(), errorTrackingArtifactCandidate{file: file, maxBytes: 18, mayTruncate: true}); err != nil {
		t.Fatal(err)
	}
	if got, want := string(uploaded), "<REDACTED>:0123456"; got != want {
		t.Fatalf("uploaded bytes = %q, want %q", got, want)
	}
	parsed, err := url.Parse(file.GetUri())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Host != "localhost:1985" {
		t.Fatalf("rewritten host = %q", parsed.Host)
	}
	rewritten, err := digest.ParseDownloadResourceName(parsed.Path)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := digest.Compute(strings.NewReader("<REDACTED>:0123456"), repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if rewritten.GetDigest().GetHash() != wantDigest.GetHash() || rewritten.GetDigest().GetSizeBytes() != int64(len("<REDACTED>:0123456")) {
		t.Fatalf("rewritten digest = %v, want %v", rewritten.GetDigest(), wantDigest)
	}
	if importer.importedArtifacts != 1 || importer.truncatedArtifacts != 1 || importer.importedBytes != int64(len("<REDACTED>:0123456")) {
		t.Fatalf("unexpected import counters: %+v", importer)
	}
	if importer.requestedArtifacts != 1 {
		t.Fatalf("source requests = %d, want 1", importer.requestedArtifacts)
	}
}

func TestErrorTrackingArtifactImporterPreservesRedactedJUnitStructure(t *testing.T) {
	const sourceHost = "cache.example"
	xmlBytes := []byte(`<testsuite name="suite"><testcase name="fails"><failure message="https://user:password@example.com/ source-api-key">source-api-key</failure></testcase></testsuite>`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(xmlBytes)
	}))
	defer server.Close()

	importer := testArtifactImporter(t, server.URL, sourceHost)
	var uploaded []byte
	importer.upload = func(_ context.Context, _ string, _ repb.DigestFunction_Value, b []byte) error {
		uploaded = append([]byte(nil), b...)
		return nil
	}
	file := &espb.File{Name: "test.xml", File: &espb.File_Uri{Uri: bytestreamURI(t, sourceHost, xmlBytes, repb.Compressor_IDENTITY)}}
	if err := importer.importCandidate(context.Background(), errorTrackingArtifactCandidate{file: file, maxBytes: maxImportedTestXMLBytes}); err != nil {
		t.Fatal(err)
	}
	cases, err := junit.Parse(bytes.NewReader(uploaded), junit.DefaultLimits())
	if err != nil {
		t.Fatalf("parse redacted downloaded JUnit XML: %s\n%s", err, uploaded)
	}
	if len(cases) != 1 || cases[0].Name != "fails" || !strings.Contains(cases[0].Failures[0].Message, "[REDACTED]") {
		t.Fatalf("unexpected redacted downloaded JUnit result: %+v", cases)
	}
}

func TestErrorTrackingArtifactImporterRejectsUnsafeCandidatesBeforeRequest(t *testing.T) {
	serverRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { serverRequests++ }))
	defer server.Close()
	validBytes := []byte("diagnostic")

	tests := []struct {
		name      string
		uri       string
		configure func(*errorTrackingArtifactImporter)
	}{
		{name: "unexpected host", uri: bytestreamURI(t, "other.example", validBytes, repb.Compressor_IDENTITY)},
		{name: "compressed", uri: bytestreamURI(t, "cache.example", validBytes, repb.Compressor_ZSTD)},
		{name: "request budget", uri: bytestreamURI(t, "cache.example", validBytes, repb.Compressor_IDENTITY), configure: func(i *errorTrackingArtifactImporter) { i.requestedArtifacts = maxImportedArtifactRequests }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			importer := testArtifactImporter(t, server.URL, "cache.example")
			importer.upload = func(context.Context, string, repb.DigestFunction_Value, []byte) error {
				return fmt.Errorf("unexpected upload")
			}
			if test.configure != nil {
				test.configure(importer)
			}
			requestsBefore := importer.requestedArtifacts
			file := &espb.File{Name: "test.log", File: &espb.File_Uri{Uri: test.uri}}
			if err := importer.importCandidate(context.Background(), errorTrackingArtifactCandidate{file: file, maxBytes: 1024, mayTruncate: true}); err == nil {
				t.Fatal("import unexpectedly succeeded")
			}
			if importer.requestedArtifacts != requestsBefore {
				t.Fatalf("rejected candidate consumed a request slot: before=%d after=%d", requestsBefore, importer.requestedArtifacts)
			}
		})
	}
	if serverRequests != 0 {
		t.Fatalf("unsafe candidates made %d source requests", serverRequests)
	}
}

func TestErrorTrackingArtifactImporterDoesNotFollowRedirects(t *testing.T) {
	redirectedRequests := 0
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirectedRequests++ }))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer source.Close()

	importer := testArtifactImporter(t, source.URL, "cache.example")
	importer.upload = func(context.Context, string, repb.DigestFunction_Value, []byte) error { return nil }
	file := &espb.File{Name: "test.log", File: &espb.File_Uri{Uri: bytestreamURI(t, "cache.example", []byte("diagnostic"), repb.Compressor_IDENTITY)}}
	if err := importer.importCandidate(context.Background(), errorTrackingArtifactCandidate{file: file, maxBytes: 1024, mayTruncate: true}); err == nil {
		t.Fatal("redirecting import unexpectedly succeeded")
	}
	if redirectedRequests != 0 {
		t.Fatalf("source API key was forwarded through %d redirect requests", redirectedRequests)
	}
}

func TestErrorTrackingArtifactImporterRejectsDigestMismatch(t *testing.T) {
	declaredBytes := []byte("declared")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("modified"))
	}))
	defer server.Close()

	importer := testArtifactImporter(t, server.URL, "cache.example")
	uploaded := false
	importer.upload = func(context.Context, string, repb.DigestFunction_Value, []byte) error {
		uploaded = true
		return nil
	}
	file := &espb.File{Name: "test.xml", File: &espb.File_Uri{Uri: bytestreamURI(t, "cache.example", declaredBytes, repb.Compressor_IDENTITY)}}
	if err := importer.importCandidate(context.Background(), errorTrackingArtifactCandidate{file: file, maxBytes: 1024}); err == nil {
		t.Fatal("digest-mismatched import unexpectedly succeeded")
	}
	if uploaded {
		t.Fatal("digest-mismatched artifact was uploaded")
	}
}
