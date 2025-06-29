package main

import (
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/experimental/mcp/resources"
	"github.com/buildbuddy-io/buildbuddy/experimental/mcp/templates"
	"github.com/buildbuddy-io/buildbuddy/experimental/mcp/tools"
	"github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/mcp"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"google.golang.org/grpc/metadata"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	serverType = flag.String("server_type", "mcp-server", "The server type to match on health checks")

	listen = flag.String("mcp.listen", ":6270", "Address to listen on")

	remoteAPI      = flag.String("mcp.remote_api", "", "gRPC Address of buildbuddy api")
	remoteCache    = flag.String("mcp.remote_cache", "", "gRPC Address of buildbuddy cache")
	monitoringAddr = flag.String("mcp.monitoring.listen", ":9090", "Address to listen for monitoring traffic on")

	headersToPropagate = []string{authutil.APIKeyHeader, authutil.ContextTokenStringKey}
)

func main() {
	flag.Parse()

	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}

	if err := config.Load(); err == nil {
		config.ReloadOnSIGHUP()
	}

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err.Error())
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)

	authenticator, err := remoteauth.NewRemoteAuthenticator()
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetAuthenticator(authenticator)

	monitoring.StartMonitoringHandler(env, *monitoringAddr)

	cacheConn, err := grpc_client.DialInternal(env, *remoteCache)
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(cacheConn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(cacheConn))
	env.SetActionCacheClient(repb.NewActionCacheClient(cacheConn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(cacheConn))

	apiConn, err := grpc_client.DialInternal(env, *remoteAPI)
	if err != nil {
		log.Fatal(err.Error())
	}
	env.SetApiClient(apipb.NewApiServiceClient(apiConn))

	// NewServer creates a new MCP server. The resulting server has no features:
	// add features using [Server.AddTools], [Server.AddPrompts] and [Server.AddResources].
	sseServer := mcp.NewServer("sse", "v0.0.1", &mcp.ServerOptions{})

	toolHandler, err := tools.NewHandler(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	sseServer.AddTools(toolHandler.GetAllTools()...)

	templateHandler, err := templates.NewHandler(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	sseServer.AddResourceTemplates(templateHandler.GetAllResourceTemplates()...)

	resourceHandler, err := resources.NewHandler(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	sseServer.AddResources(resourceHandler.GetAllResources()...)

	handler := mcp.NewSSEHandler(func(request *http.Request) *mcp.Server {
		url := request.URL.Path
		log.Infof("Handling request for URL %q", url)

		// If the client has attached an auth header, add it to the
		// context so that if we make outgoing requests they'll be
		// authed.
		if apiKey := request.Header.Get(authutil.APIKeyHeader); apiKey != "" {
			ctx := metadata.AppendToOutgoingContext(request.Context(), authutil.APIKeyHeader, apiKey)
			newRequest := request.WithContext(ctx)
			*request = *newRequest
		}
		switch url {
		case "/sse":
			return sseServer
		default:
			log.Errorf("Unhandled server URL: %q", url)
			return nil
		}
	})
	go func() {
		_ = http.Serve(lis, handler)
	}()

	log.Printf("BuildBuddy MCP server listening at %v", lis.Addr())
	env.GetHealthChecker().WaitForGracefulShutdown()
}
