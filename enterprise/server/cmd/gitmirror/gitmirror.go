package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"github.com/jonboulle/clockwork"
)

var (
	listen         = flag.String("listen", "0.0.0.0", "The interface to listen on (default: 0.0.0.0)")
	port           = flag.Int("port", 8180, "The port to listen for HTTP traffic on")
	monitoringAddr = flag.String("monitoring.listen", ":9191", "Address to listen for monitoring traffic on")
	serverType     = flag.String("server_type", "gitmirror", "The server type to match on health checks")
)

func main() {
	version.Print("BuildBuddy Git mirror")

	// Flags must be parsed before config secrets integration is enabled since
	// that feature itself depends on flag values.
	flag.Parse()
	if err := configsecrets.Configure(); err != nil {
		log.Fatalf("Could not prepare config secrets provider: %s", err)
	}
	if err := config.Load(); err != nil {
		log.Fatalf("Could not load config: %s", err)
	}
	config.ReloadOnSIGHUP()

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s\n", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)
	if err := tracing.Configure(env); err != nil {
		log.Fatalf("Could not configure tracing: %s", err)
	}
	env.SetListenAddr(*listen)
	env.SetMux(tracing.NewHttpServeMux(http.NewServeMux()))
	gitMirror, err := gitmirror.New(clockwork.NewRealClock())
	if err != nil {
		log.Fatalf("Could not initialize Git mirror: %s", err)
	}
	env.GetMux().Handle("/", gitMirror)
	env.GetMux().Handle("/healthz", healthChecker.LivenessHandler())
	env.GetMux().Handle("/readyz", healthChecker.ReadinessHandler())

	monitoring.StartMonitoringHandler(env, *monitoringAddr)

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", *listen, *port),
		Handler: env.GetMux(),
	}
	healthChecker.RegisterShutdownFunction(func(ctx context.Context) error {
		shutdownErr := server.Shutdown(ctx)
		closeErr := gitMirror.Close()
		return errors.Join(shutdownErr, closeErr)
	})

	go func() {
		log.Infof("Listening for HTTP traffic on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Could not serve HTTP traffic: %s", err)
		}
	}()

	healthChecker.WaitForGracefulShutdown()
}
