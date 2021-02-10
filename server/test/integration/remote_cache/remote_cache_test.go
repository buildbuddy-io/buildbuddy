package remote_cache_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/bazel"
	"google.golang.org/grpc"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	testauth "github.com/buildbuddy-io/buildbuddy/server/testutil/auth"
	testenv "github.com/buildbuddy-io/buildbuddy/server/testutil/environment"
)

var (
	workspaceContents = map[string]string{
		"WORKSPACE": `workspace(name = "integration_test")`,
		"BUILD": `genrule(
			name = "hello_txt",
			outs = ["hello.txt"],
			cmd_bash = "echo 'Hello world!' > $@",
		)`,
	}
)

func runBBServer(ctx context.Context, t *testing.T) *grpc.ClientConn {
	env := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	env.SetAuthenticator(authenticator)

	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env /*sslService=*/, nil)
	if err != nil {
		t.Fatal(err)
	}
	grpcServer, runFunc := env.LocalGRPCServer()
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return clientConn
}

func TestBazelBuild_RemoteCacheHit(t *testing.T) {
	ctx := context.Background()
	_ = runBBServer(ctx, t)
	ws := bazel.MakeTempWorkspace(t, workspaceContents)

	result := bazel.Invoke(ctx, t, ws, "build", "//:hello_txt")

	assert.Nil(t, result.Error)
	assert.Regexp(t, "Build completed successfully", result.Output)

	bazel.Clean(ctx, t, ws)

	result = bazel.Invoke(ctx, t, ws, "build", "//:hello.txt")

	assert.Nil(t, result.Error)
	assert.Regexp(t, "1 remote cache hit", result.Output)
}
