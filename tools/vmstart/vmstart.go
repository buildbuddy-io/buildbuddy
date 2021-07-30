package main

import (
	"context"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	image              = flag.String("image", "docker.io/library/busybox", "The default container to run.")
	cacheTarget        = flag.String("cache_target", "grpcs://remote.buildbuddy.dev", "The remote cache target")
	remoteInstanceName = flag.String("remote_instance_name", "", "The remote_instance_name for caching snapshots")
	forceVMIdx         = flag.Int("force_vm_idx", -1, "VM index to force to avoid network conflicts -- random by default")
	snapshotID         = flag.String("snapshot_id", "", "The snapshot ID to load")
)

func getToolEnv() *real_environment.RealEnv {
	configurator, err := config.NewConfigurator("")
	if err != nil {
		log.Fatalf("This should never happen.")
	}
	healthChecker := healthcheck.NewHealthChecker("tool")
	re := real_environment.NewRealEnv(configurator, healthChecker)

	conn, err := grpc_client.DialTarget(*cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", *cacheTarget, err)
	}
	re.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	re.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	re.SetActionCacheClient(repb.NewActionCacheClient(conn))
	return re
}

func main() {
	flag.Parse()
	env := getToolEnv()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Configure(log.Opts{Level: "debug", EnableShortFileName: true})

	emptyActionDir, err := os.MkdirTemp("", "fc-container-*")
	if err != nil {
		log.Fatalf("unable to make temp dir: %s", err)
	}

	vmIdx := 100 + rand.Intn(100)
	if *forceVMIdx != -1 {
		vmIdx = *forceVMIdx
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         *image,
		ActionWorkingDirectory: emptyActionDir,
		NumCPUs:                1,
		MemSizeMB:              100,
		EnableNetworking:       true,
		DebugMode:              true,
		ForceVMIdx:             vmIdx,
	}

	if *snapshotID != "" {
		c, err := firecracker.NewContainer(env, opts)
		if err != nil {
			log.Fatalf("Error creating container: %s", err)
		}
		if err := c.LoadSnapshot(ctx, *remoteInstanceName, *snapshotID); err != nil {
			log.Fatalf("Error loading snapshot: %s", err)
		}
		if err := c.Wait(ctx); err != nil {
			log.Printf("Wait err: %s", err)
		}
		if err := c.Remove(ctx); err != nil {
			log.Errorf("Error removing container: %s", err)
		}
		return
	}

	c, err := firecracker.NewContainer(env, opts)
	if err != nil {
		log.Fatalf("Error creating container: %s", err)
	}
	if err := c.PullImageIfNecessary(ctx); err != nil {
		log.Fatalf("Unable to PullImageIfNecessary: %s", err)
	}
	if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
		log.Fatalf("Unable to Create container: %s", err)
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM)
	go func() {
		for {
			<-sigc
			log.Errorf("Capturing snapshot...")
			snapshotID, err := c.SaveSnapshot(ctx, *remoteInstanceName)
			if err != nil {
				log.Fatalf("Error dumping snapshot: %s", err)
			}
			log.Printf("Created snapshot with ID %q", snapshotID)
		}
	}()

	log.Printf("Started firecracker container!")
	log.Printf("To capture a snapshot at any time, send SIGTERM (killall vmstart)")
	if err := c.Wait(ctx); err != nil {
		log.Printf("Wait err: %s", err)
	}

	if err := c.Remove(ctx); err != nil {
		log.Errorf("Error removing container: %s", err)
	}
}
