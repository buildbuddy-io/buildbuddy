package main

import (
	"context"
	"flag"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	image = flag.String("image", "docker.io/library/busybox", "The default container to run.")
)

func getToolEnv() *real_environment.RealEnv {
	configurator, err := config.NewConfigurator("")
	if err != nil {
		log.Fatalf("This should never happen.")
	}
	healthChecker := healthcheck.NewHealthChecker("tool")
	return real_environment.NewRealEnv(configurator, healthChecker)
}

func main() {
	flag.Parse()
	env := getToolEnv()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	emptyActionDir, err := os.MkdirTemp("", "fc-container-*")
	if err != nil {
		log.Fatalf("unable to make temp dir: %s", err)
	}

	c, err := firecracker.NewContainer(ctx, env, *image, "/tmp")
	if err != nil {
		log.Fatalf("Error creating container: %s", err)
	}
	if err := c.PullImageIfNecessary(ctx); err != nil {
		log.Fatalf("unable to PullImageIfNecessary: %s", err)
	}
	if err := c.Create(ctx, emptyActionDir); err != nil {
		log.Fatalf("unable to Create container: %s", err)
	}
	log.Printf("Started firecracker container!")
	// If you're testing snapshot-specifics, use the following:
	/*	if err := c.Pause(ctx); err != nil {
			log.Fatalf("unable to Pause container: %s", err)
		}
		log.Printf("Paused firecracker container!")
		if err := c.Unpause(ctx); err != nil {
			log.Fatalf("unable to Resume container: %s", err)
		}
		log.Printf("Resumed firecracker container!")
	*/
	if err := c.Wait(ctx); err != nil {
		log.Printf("Wait err: %s", err)
	}
	if err := c.Remove(ctx); err != nil {
		log.Errorf("Error removing container: %s", err)
	}
}
