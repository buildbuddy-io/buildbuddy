package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remoteauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test_buddy/failure_analysis"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	testbuddy "github.com/buildbuddy-io/buildbuddy/server/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on.")
	httpPort       = flag.Int("port", 8080, "The port to listen for HTTP traffic on.")
	monitoringPort = flag.Int("monitoring_port", 9090, "The port to listen for monitoring traffic on.")
	serverType     = flag.String("server_type", "test-buddy", "The server type to match on health checks.")
)

func main() {
	version.Print("BuildBuddy TestBuddy")
	flag.Parse()
	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config: %s", err)
	}
	config.ReloadOnSIGHUP()
	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	env.SetListenAddr(*listen)
	env.SetAuthenticator(nullauth.NewNullAuthenticator(true))
	if err := tracing.Configure(env); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}
	if remoteauth.Configured() {
		if err := remoteauth.Register(env); err != nil {
			log.Fatal(err.Error())
		}
	}
	if err := clientidentity.Register(env); err != nil {
		log.Fatal(err.Error())
	}
	if err := ssl.Register(env); err != nil {
		log.Fatal(err.Error())
	}
	database, err := db.GetConfiguredDatabase(context.Background(), env)
	if err != nil {
		log.Fatalf("Error configuring database: %s", err)
	}
	env.SetDBHandle(database)
	failureAnalysis, err := failure_analysis.NewConfigured(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	if failureAnalysis != nil {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			failureAnalysis.Run(ctx)
		}()
		env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
			cancel()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	for _, port := range []int{grpc_server.GRPCPort(), grpc_server.InternalGRPCPort()} {
		server, err := grpc_server.New(env, port, false, grpc_server.GRPCServerConfig{})
		if err != nil {
			log.Fatal(err.Error())
		}
		testbuddy.RegisterLocal(env, server.GetServer())
		if err := server.Start(); err != nil {
			log.Fatal(err.Error())
		}
	}

	monitoring.StartMonitoringHandler(env, fmt.Sprintf("%s:%d", *listen, *monitoringPort))
	env.GetMux().Handle("/healthz", env.GetHealthChecker().LivenessHandler())
	env.GetMux().Handle("/readyz", env.GetHealthChecker().ReadinessHandler())
	httpServer := &http.Server{Addr: fmt.Sprintf("%s:%d", *listen, *httpPort), Handler: env.GetMux()}
	env.GetHTTPServerWaitGroup().Add(1)
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		defer env.GetHTTPServerWaitGroup().Done()
		return httpServer.Shutdown(ctx)
	})
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("HTTP server failed: %s", err)
		}
	}()
	env.GetHealthChecker().WaitForGracefulShutdown()
}
