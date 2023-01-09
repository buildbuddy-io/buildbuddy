package invocation_webhook_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhealthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestInvocationUploadWebhook(t *testing.T) {
	tempDir := testfs.MakeTempDir(t)
	dataSource := "sqlite3://" + filepath.Join(tempDir, "buildbuddy-test.db")
	app := buildbuddy_enterprise.Run(t,
		"--database.data_source="+dataSource,
		"--integrations.invocation_upload.enabled=true",
		"--cache.detailed_stats_enabled=true",
		"--app.no_default_user_group=false",
	)

	// Start our fake webhook server.
	ws := NewWebhookServer(t)

	// Connect directly to the DB and configure webhooks to point at the server.
	// (For now, manual SQL is the only way to set up invocation webhooks.)
	flags.Set(t, "database.data_source", dataSource)
	hc := testhealthcheck.NewTestingHealthChecker()
	env := real_environment.NewRealEnv(hc)
	dbh, err := db.GetConfiguredDatabase(env)
	require.NoError(t, err)
	ctx := context.Background()
	db := dbh.DB(ctx)
	err = db.Exec("UPDATE `Groups` SET invocation_webhook_url = ?", ws.URL.String()).Error
	require.NoError(t, err)
	apiKey := tables.APIKey{}
	dbResult := db.Where("group_id = ?", userdb.DefaultGroupID).First(&apiKey)
	require.NoError(t, dbResult.Error)

	// Now run an invocation (with BES and remote cache) as the default group,
	// expecting a cache miss.
	wsDir := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": "",
		"BUILD":     `genrule(name = "gen", outs = ["out"], cmd_bash = "touch $@")`,
	})
	buildFlags := []string{":gen", "--remote_header=x-buildbuddy-api-key=" + apiKey.Value}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, app.RemoteCacheBazelFlags()...)

	result := testbazel.Invoke(ctx, t, wsDir, "build", buildFlags...)

	require.NoError(t, result.Error)

	// Wait up to 1s for the invocation to be published to the webhook.
	end := time.Now().Add(1 * time.Second)
	var invocations []*inpb.Invocation
	for time.Now().Before(end) {
		time.Sleep(25 * time.Millisecond)
		invocations = ws.Invocations()
		if len(invocations) != 0 {
			break
		}
	}
	if len(invocations) == 0 {
		assert.FailNow(t, "no invocations were published to the test webhook server")
	}
	in := invocations[0]
	require.Len(t, in.GetScoreCard().GetMisses(), 1, "should have 1 action cache miss")
	require.Equal(t, "Genrule", in.GetScoreCard().GetMisses()[0].GetActionMnemonic())
}

// webhookServer collects invocations that are sent to it via HTTP and stores
// them in a list.
type webhookServer struct {
	t   *testing.T
	URL *url.URL

	mu          sync.Mutex
	invocations []*inpb.Invocation
}

func NewWebhookServer(t *testing.T) *webhookServer {
	ws := &webhookServer{t: t}
	handler := ws
	url := testhttp.StartServer(t, handler)
	ws.URL = url
	return ws
}

func (ws *webhookServer) Invocations() []*inpb.Invocation {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return append(make([]*inpb.Invocation, 0, len(ws.invocations)), ws.invocations...)
}

func (ws *webhookServer) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	in, err := ws.readInvocation(req)
	if err != nil {
		require.FailNow(ws.t, "failed to read invocation", "%s", err)
	}
	_, err = res.Write([]byte("OK"))
	require.NoError(ws.t, err)

	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.invocations = append(ws.invocations, in)
}

func (ws *webhookServer) readInvocation(req *http.Request) (*inpb.Invocation, error) {
	gzb, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	gzr, err := gzip.NewReader(bytes.NewReader(gzb))
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(gzr)
	if err != nil {
		return nil, err
	}
	in := &inpb.Invocation{}
	if err := protojson.Unmarshal(b, in); err != nil {
		return nil, err
	}
	return in, nil
}
