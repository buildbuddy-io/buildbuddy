package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

const (
	invocationsEnvName = "BUILDGREP_INVOCATIONS"
	apiKeyEnvName      = "BUILDBUDDY_API_KEY"
)

var (
	target           = flag.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target.")
	configFile       = flag.String("config_file", "config.json", "JSON file containing buildgrep worker config.")
	fetchConcurrency = flag.Int("fetch_concurrency", 16, "Maximum number of concurrent GetEventLog fetches.")

	ansiEscapePattern = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\)|[@-Z\\-_])`)
)

type workerConfig struct {
	Pattern  string `json:"pattern"`
	Regex    bool   `json:"regex"`
	Template string `json:"template"`
}

type batchInvocation struct {
	InvocationID  string `json:"invocation_id"`
	UpdatedAtUsec int64  `json:"updated_at_usec"`
}

type lineMatcher struct {
	pattern string
	regex   *regexp.Regexp
}

type matchTemplateData struct {
	InvocationID  string
	UpdatedAt     string
	UpdatedAtUsec int64
	LineNumber    int
	Match         string
	Line          string
	MatchedText   string
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	invocations, err := readInvocations()
	if err != nil {
		return err
	}
	key := os.Getenv(apiKeyEnvName)
	if key == "" {
		return fmt.Errorf("%s is empty", apiKeyEnvName)
	}
	if *fetchConcurrency <= 0 {
		return fmt.Errorf("--fetch_concurrency must be positive")
	}
	config, err := readConfig(*configFile)
	if err != nil {
		return err
	}
	matcher, err := newLineMatcher(config)
	if err != nil {
		return err
	}
	tmpl, err := template.New("match").Parse(config.Template)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *target, err)
	}
	defer conn.Close()
	ctx := metadata.AppendToOutgoingContext(context.Background(), authutil.APIKeyHeader, key)
	client := bbspb.NewBuildBuddyServiceClient(conn)

	return fetchAndSearchAllLogs(ctx, client, invocations, matcher, tmpl)
}

func readInvocations() ([]batchInvocation, error) {
	env := os.Getenv(invocationsEnvName)
	if env == "" {
		return nil, fmt.Errorf("%s is empty", invocationsEnvName)
	}
	var invocations []batchInvocation
	if err := json.Unmarshal([]byte(env), &invocations); err != nil {
		return nil, fmt.Errorf("unmarshal invocations: %w", err)
	}
	if len(invocations) == 0 {
		return nil, fmt.Errorf("%s is empty", invocationsEnvName)
	}
	return invocations, nil
}

func readConfig(path string) (*workerConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var config workerConfig
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if config.Pattern == "" {
		return nil, fmt.Errorf("search pattern must not be empty")
	}
	return &config, nil
}

func newLineMatcher(config *workerConfig) (*lineMatcher, error) {
	m := &lineMatcher{pattern: config.Pattern}
	if !config.Regex {
		return m, nil
	}
	r, err := regexp.Compile(config.Pattern)
	if err != nil {
		return nil, fmt.Errorf("compile regex: %w", err)
	}
	m.regex = r
	return m, nil
}

func (m *lineMatcher) match(line string) (bool, string) {
	if m.regex == nil {
		if !strings.Contains(line, m.pattern) {
			return false, ""
		}
		return true, m.pattern
	}
	loc := m.regex.FindStringIndex(line)
	if loc == nil {
		return false, ""
	}
	return true, line[loc[0]:loc[1]]
}

func fetchAndSearchAllLogs(ctx context.Context, client bbspb.BuildBuddyServiceClient, invocations []batchInvocation, matcher *lineMatcher, tmpl *template.Template) error {
	var succeeded atomic.Int64
	var outputMu sync.Mutex
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(*fetchConcurrency)
	for _, inv := range invocations {
		eg.Go(func() error {
			var out bytes.Buffer
			if err := searchLog(ctx, client, inv, matcher, tmpl, &out); err != nil {
				fmt.Fprintf(os.Stderr, "buildgrep: %s: fetch log: %s\n", inv.InvocationID, err)
				return nil
			}
			succeeded.Add(1)
			if out.Len() > 0 {
				outputMu.Lock()
				_, _ = os.Stdout.Write(out.Bytes())
				outputMu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if succeeded.Load() == 0 {
		return fmt.Errorf("no invocation logs fetched")
	}
	return nil
}

func searchLog(ctx context.Context, client bbspb.BuildBuddyServiceClient, inv batchInvocation, matcher *lineMatcher, tmpl *template.Template, out io.Writer) error {
	stream, err := client.GetEventLog(ctx, &elpb.GetEventLogChunkRequest{
		InvocationId: inv.InvocationID,
		ChunkId:      "0",
		Type:         elpb.LogType_BUILD_LOG,
	})
	if err != nil {
		return fmt.Errorf("GetEventLog: %w", err)
	}
	searcher := newLogSearcher(inv, matcher, tmpl, out)
	for {
		rsp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return searcher.flush()
		}
		if err != nil {
			return fmt.Errorf("recv event log: %w", err)
		}
		if err := searcher.write(rsp.GetBuffer()); err != nil {
			return err
		}
	}
}

type logSearcher struct {
	inv        batchInvocation
	matcher    *lineMatcher
	tmpl       *template.Template
	out        io.Writer
	lineNumber int
	pending    []byte
}

func newLogSearcher(inv batchInvocation, matcher *lineMatcher, tmpl *template.Template, out io.Writer) *logSearcher {
	return &logSearcher{
		inv:        inv,
		matcher:    matcher,
		tmpl:       tmpl,
		out:        out,
		lineNumber: 1,
	}
}

func (s *logSearcher) write(chunk []byte) error {
	s.pending = append(s.pending, chunk...)
	for {
		idx := bytes.IndexByte(s.pending, '\n')
		if idx < 0 {
			return nil
		}
		if err := s.processLine(trimCarriageReturn(s.pending[:idx])); err != nil {
			return err
		}
		s.lineNumber++
		s.pending = s.pending[idx+1:]
	}
}

func (s *logSearcher) flush() error {
	if len(s.pending) == 0 {
		return nil
	}
	if err := s.processLine(trimCarriageReturn(s.pending)); err != nil {
		return err
	}
	s.pending = nil
	return nil
}

func (s *logSearcher) processLine(rawLine []byte) error {
	line := string(stripANSI(rawLine))
	matched, matchedText := s.matcher.match(line)
	if !matched {
		return nil
	}
	data := matchTemplateData{
		InvocationID:  s.inv.InvocationID,
		UpdatedAt:     formatUsec(s.inv.UpdatedAtUsec),
		UpdatedAtUsec: s.inv.UpdatedAtUsec,
		LineNumber:    s.lineNumber,
		Match:         line,
		Line:          line,
		MatchedText:   matchedText,
	}
	var rendered bytes.Buffer
	if err := s.tmpl.Execute(&rendered, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}
	if rendered.Len() == 0 || rendered.Bytes()[rendered.Len()-1] != '\n' {
		rendered.WriteByte('\n')
	}
	if _, err := s.out.Write(rendered.Bytes()); err != nil {
		return fmt.Errorf("write match: %w", err)
	}
	return nil
}

func stripANSI(b []byte) []byte {
	return ansiEscapePattern.ReplaceAll(b, nil)
}

func trimCarriageReturn(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\r' {
		return b[:len(b)-1]
	}
	return b
}

func formatUsec(usec int64) string {
	if usec <= 0 {
		return ""
	}
	sec := usec / 1_000_000
	nsec := (usec % 1_000_000) * 1_000
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano)
}
