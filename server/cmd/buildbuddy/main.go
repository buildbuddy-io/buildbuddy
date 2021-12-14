package main

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/janitor"
	"github.com/buildbuddy-io/buildbuddy/server/libmain"
	"github.com/buildbuddy-io/buildbuddy/server/telemetry"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/version"
)

var (
	configFile = flag.String("config_file", "/config.yaml", "The path to a buildbuddy config file")
	serverType = flag.String("server_type", "buildbuddy-server", "The server type to match on health checks")
)

// NB: Most of the logic you'd typically find in a main.go file is in
// libmain.go. We put it there to reduce the duplicated code between the open
// source main() entry point and the enterprise main() entry point, both of
// which import from libmain.go.

func main() {
	flag.Parse()
	ctx := context.Background()
	version.Print(ctx)
	configurator, err := config.NewConfigurator(ctx, *configFile)
	if err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}
	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := libmain.GetConfiguredEnvironmentOrDie(ctx, configurator, healthChecker)

	telemetryClient := telemetry.NewTelemetryClient(env)
	telemetryClient.Start()
	defer telemetryClient.Stop()

	cleanupService := janitor.NewJanitor(env)
	cleanupService.Start()
	defer cleanupService.Stop()

	libmain.StartAndRunServices(ctx, env) // Does not return
}
