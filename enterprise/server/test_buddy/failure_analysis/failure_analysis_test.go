package failure_analysis_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test_buddy/failure_analysis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	testflags "github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

type classifier struct {
	mu       sync.Mutex
	calls    int
	entered  chan struct{}
	release  chan struct{}
	analysis *failure_analysis.Analysis
	err      error
}

func (c *classifier) Classify(ctx context.Context, failureMessage string) (*failure_analysis.Analysis, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	if c.entered != nil {
		c.entered <- struct{}{}
	}
	if c.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.release:
		}
	}
	return c.analysis, c.err
}

func (c *classifier) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func insertCluster(t *testing.T, env environment.Env, fingerprint string) {
	t.Helper()
	require.NoError(t, env.GetDBHandle().GORM(context.Background(), "test_buddy_insert_failure_cluster").Create(&tables.TestFailureCluster{
		GroupID: "GR1", Repository: "https://github.com/acme/repo", Fingerprint: fingerprint,
		FailureMessage: []byte("expected true, got false"), AnalysisSummary: []byte{}, SuggestedFix: []byte{},
	}).Error)
}

func readCluster(t *testing.T, env environment.Env, fingerprint string) *tables.TestFailureCluster {
	t.Helper()
	cluster := &tables.TestFailureCluster{}
	require.NoError(t, env.GetDBHandle().GORM(context.Background(), "test_buddy_read_failure_cluster").
		Where("group_id = ? AND repository = ? AND fingerprint = ?", "GR1", "https://github.com/acme/repo", fingerprint).
		Take(cluster).Error)
	return cluster
}

func TestWorkersLeaseEachClusterOnce(t *testing.T) {
	env := testenv.GetTestEnv(t)
	insertCluster(t, env, "fingerprint")

	firstClassifier := &classifier{
		entered: make(chan struct{}, 1), release: make(chan struct{}),
		analysis: &failure_analysis.Analysis{
			Category: "assertion", Summary: "The assertion failed.", SuggestedFix: "Correct the expected value.",
			Confidence: "high", Model: "gpt-5.4-nano",
		},
	}
	firstResult := make(chan error, 1)
	go func() {
		worked, err := failure_analysis.New(env.GetDBHandle(), firstClassifier).RunOnce(context.Background())
		if !worked && err == nil {
			err = errors.New("first worker did not claim cluster")
		}
		firstResult <- err
	}()
	<-firstClassifier.entered

	secondClassifier := &classifier{err: errors.New("unexpected classification")}
	worked, err := failure_analysis.New(env.GetDBHandle(), secondClassifier).RunOnce(context.Background())
	require.NoError(t, err)
	require.False(t, worked)
	require.Zero(t, secondClassifier.callCount())

	close(firstClassifier.release)
	require.NoError(t, <-firstResult)
	cluster := readCluster(t, env, "fingerprint")
	require.Equal(t, int64(1), cluster.AnalysisPromptVersion)
	require.Equal(t, "gpt-5.4-nano", cluster.AnalysisModel)
	require.Equal(t, "assertion", cluster.AnalysisCategory)
	require.Equal(t, "The assertion failed.", string(cluster.AnalysisSummary))
	require.Equal(t, "Correct the expected value.", string(cluster.SuggestedFix))
	require.Equal(t, "high", cluster.AnalysisConfidence)
	require.Empty(t, cluster.AnalysisLeaseToken)
}

func TestFailedAnalysisIsReleasedForRetry(t *testing.T) {
	env := testenv.GetTestEnv(t)
	insertCluster(t, env, "retry")

	worked, err := failure_analysis.New(env.GetDBHandle(), &classifier{err: errors.New("model unavailable")}).RunOnce(context.Background())
	require.ErrorContains(t, err, "model unavailable")
	require.True(t, worked)
	cluster := readCluster(t, env, "retry")
	require.Zero(t, cluster.AnalysisPromptVersion)
	require.Empty(t, cluster.AnalysisLeaseToken)
	require.Greater(t, cluster.NextAnalysisAttemptUsec, env.GetDBHandle().NowFunc().UnixMicro())

	worked, err = failure_analysis.New(env.GetDBHandle(), &classifier{}).RunOnce(context.Background())
	require.NoError(t, err)
	require.False(t, worked)
}

func TestConfiguredWorkerUsesNanoResponsesAPI(t *testing.T) {
	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requests <- request
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"{\"category\":\"assertion\",\"summary\":\"The assertion failed.\",\"suggested_fix\":\"Correct the expected value.\",\"confidence\":\"high\"}"}]}]}`))
	}))
	defer server.Close()
	testflags.Set(t, "test_buddy.failure_analysis_enabled", true)
	testflags.Set(t, "test_buddy.failure_analysis_model", "gpt-5.4-nano")
	testflags.Set(t, "openai.api_key", "test-key")
	testflags.Set(t, "openai.responses_endpoint", server.URL)

	env := testenv.GetTestEnv(t)
	insertCluster(t, env, "openai")
	worker, err := failure_analysis.NewConfigured(env)
	require.NoError(t, err)
	require.NotNil(t, worker)
	worked, err := worker.RunOnce(context.Background())
	require.NoError(t, err)
	require.True(t, worked)

	request := <-requests
	require.Equal(t, "gpt-5.4-nano", request["model"])
	require.Equal(t, false, request["store"])
	inputs := request["input"].([]any)
	require.Len(t, inputs, 2)
	require.Equal(t, "developer", inputs[0].(map[string]any)["role"])
	require.Equal(t, "user", inputs[1].(map[string]any)["role"])
	require.Equal(t, "expected true, got false", inputs[1].(map[string]any)["content"])
	format := request["text"].(map[string]any)["format"].(map[string]any)
	require.Equal(t, "json_schema", format["type"])
	require.Equal(t, true, format["strict"])
}
