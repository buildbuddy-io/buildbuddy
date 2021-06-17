package remote_cache_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

const (
	// configuredDefaultAPIKey is the API key value explicitly configured for the default group.
	configuredDefaultAPIKey = "configuredDefaultAPIKey"
)

var (
	testGroupID       = userdb.DefaultGroupID
	workspaceContents = map[string]string{
		"WORKSPACE": `workspace(name = "integration_test")`,
		"BUILD":     `genrule(name = "hello_txt", outs = ["hello.txt"], cmd_bash = "echo 'Hello world' > $@")`,
	}
)

func TestBuild_RemoteCacheFlags_Anonymous_SecondBuildIsCached(t *testing.T) {
	app := buildbuddy_enterprise.Run(t)
	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := []string{"//:hello.txt"}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, app.RemoteCacheBazelFlags()...)

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	require.NotContains(
		t, result.Stderr, "1 remote cache hit",
		"sanity check: initial build shouldn't be cached",
	)

	// Clear the local cache so we can try for a remote cache hit.
	testbazel.Clean(ctx, t, ws)

	result = testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.Contains(
		t, result.Stderr, "1 remote cache hit",
		"second build should be cached since anonymous users have cache write capabilities by default",
	)
}

func TestBuild_RemoteCacheFlags_ReadWriteApiKey_SecondBuildIsCached(t *testing.T) {
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	// Run the app with an API key we control so that we can authorize using it.
	app := buildbuddy_enterprise.Run(
		t,
		fmt.Sprintf("--app.no_default_user_group=false"),
		fmt.Sprintf("--api.api_key=%s", configuredDefaultAPIKey),
	)
	bbService := app.BuildBuddyServiceClient(t)
	// Create a new read-write key
	ctx := metadata.AppendToOutgoingContext(context.Background(), auth.APIKeyHeader, configuredDefaultAPIKey)
	rsp, err := bbService.CreateApiKey(ctx, &akpb.CreateApiKeyRequest{
		RequestContext: testauth.RequestContext(testGroupID, testGroupID),
		GroupId:        testGroupID,
		Capability:     []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY},
	})
	if err != nil {
		t.Fatal(err)
	}
	readWriteKey := rsp.ApiKey.Value
	buildFlags := []string{"//:hello.txt", fmt.Sprintf("--remote_header=%s=%s", auth.APIKeyHeader, readWriteKey)}
	buildFlags = append(buildFlags, app.RemoteCacheBazelFlags()...)

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.NotContains(
		t, result.Stderr, "1 remote cache hit",
		"sanity check: initial build shouldn't be cached",
	)

	// Clear the local cache so we can try for a remote cache hit.
	testbazel.Clean(ctx, t, ws)

	result = testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.Contains(
		t, result.Stderr, "1 remote cache hit",
		"second build should be cached since the API key used in the first build has cache write capabilities",
	)
}

func TestBuild_RemoteCacheFlags_ReadOnlyApiKey_SecondBuildIsNotCached(t *testing.T) {
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	// Run the app with an API key we control so that we can authorize using it.
	app := buildbuddy_enterprise.Run(
		t,
		fmt.Sprintf("--app.no_default_user_group=false"),
		fmt.Sprintf("--api.api_key=%s", configuredDefaultAPIKey),
	)
	bbService := app.BuildBuddyServiceClient(t)
	// Create a new read-only key
	ctx := metadata.AppendToOutgoingContext(context.Background(), auth.APIKeyHeader, configuredDefaultAPIKey)
	rsp, err := bbService.CreateApiKey(ctx, &akpb.CreateApiKeyRequest{
		RequestContext: testauth.RequestContext(testGroupID, testGroupID),
		GroupId:        testGroupID,
		Capability:     []akpb.ApiKey_Capability{},
	})
	if err != nil {
		t.Fatal(err)
	}
	readOnlyKey := rsp.ApiKey.Value
	buildFlags := []string{"//:hello.txt", fmt.Sprintf("--remote_header=%s=%s", auth.APIKeyHeader, readOnlyKey)}
	buildFlags = append(buildFlags, app.RemoteCacheBazelFlags()...)

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.NotContains(
		t, result.Stderr, "1 remote cache hit",
		"sanity check: initial build shouldn't be cached",
	)

	// Clear the local cache so the remote cache will be queried.
	testbazel.Clean(ctx, t, ws)

	result = testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.NotContains(
		t, result.Stderr, "1 remote cache hit",
		"second build should not be cached since the first build was done with a read-only key",
	)
}

func TestBuild_RemoteCacheFlags_NoAuthConfigured_SecondBuildIsCached(t *testing.T) {
	app := buildbuddy_enterprise.RunWithConfig(t, buildbuddy_enterprise.NoAuthConfig)
	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := []string{"//:hello.txt"}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, app.RemoteCacheBazelFlags()...)

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	require.NotContains(
		t, result.Stderr, "1 remote cache hit",
		"sanity check: initial build shouldn't be cached",
	)

	// Clear the local cache so we can try for a remote cache hit.
	testbazel.Clean(ctx, t, ws)

	result = testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	assert.Contains(
		t, result.Stderr, "1 remote cache hit",
		"second build should be cached since anonymous users have cache write capabilities by default",
	)
}
