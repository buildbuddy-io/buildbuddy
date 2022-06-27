package main

import (
	"context"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/prototext"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	image               = flag.String("image", "docker.io/library/busybox", "The default container to run.")
	registryUser        = flag.String("container_registry_user", "", "User to use when pulling the image")
	registryPassword    = flag.String("container_registry_password", "", "Password to use when pulling the image")
	cacheTarget         = flag.String("cache_target", "grpcs://remote.buildbuddy.dev", "The remote cache target")
	snapshotDir         = flag.String("snapshot_dir", "/tmp/filecache", "The directory to store snapshots in")
	snapshotDirMaxBytes = flag.Int64("snapshot_dir_max_bytes", 10000000000, "The max number of bytes the snapshot_dir can be")
	remoteInstanceName  = flag.String("remote_instance_name", "", "The remote_instance_name for caching snapshots and interacting with the CAS if an action digest is specified")
	forceVMIdx          = flag.Int("force_vm_idx", -1, "VM index to force to avoid network conflicts -- random by default")
	snapshotID          = flag.String("snapshot_id", "", "The snapshot ID to load")
	apiKey              = flag.String("api_key", "", "The API key to use to interact with the remote cache.")
	actionDigest        = flag.String("action_digest", "", "The optional digest of the action you want to mount.")
)

func getToolEnv() *real_environment.RealEnv {
	healthChecker := healthcheck.NewHealthChecker("tool")
	re := real_environment.NewRealEnv(healthChecker)

	fc, err := filecache.NewFileCache(*snapshotDir, *snapshotDirMaxBytes)
	if err != nil {
		log.Fatalf("Unable to setup filecache %s", err)
	}
	fc.WaitForDirectoryScanToComplete()
	re.SetFileCache(fc)

	conn, err := grpc_client.DialTarget(*cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", *cacheTarget, err)
	}
	re.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	re.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	re.SetActionCacheClient(repb.NewActionCacheClient(conn))
	return re
}

func parseSnapshotID(in string) *repb.Digest {
	parts := strings.SplitN(in, "/", 2)
	if len(parts) != 2 || len(parts[0]) != 64 {
		log.Fatalf("Error parsing snapshotID %q (not in hash/size form)", in)
	}
	i, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		log.Fatalf("Error parsing snapshotID %q: %s", in, err)
	}
	return &repb.Digest{
		Hash:      parts[0],
		SizeBytes: i,
	}
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().Unix())

	env := getToolEnv()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	*log.LogLevel = "debug"
	*log.IncludeShortFileName = true
	log.Configure()

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
		MemSizeMB:              2500,
		ScratchDiskSizeMB:      100,
		EnableNetworking:       true,
		DebugMode:              true,
		ForceVMIdx:             vmIdx,
		JailerRoot:             "/tmp/remote_build/",
	}

	var c *firecracker.FirecrackerContainer
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	if *snapshotID != "" {
		c, err = firecracker.NewContainer(env, auth, opts)
		if err != nil {
			log.Fatalf("Error creating container: %s", err)
		}
		if err := c.LoadSnapshot(ctx, "" /*workspaceFS*/, *remoteInstanceName, parseSnapshotID(*snapshotID)); err != nil {
			log.Fatalf("Error loading snapshot: %s", err)
		}
	} else {
		c, err = firecracker.NewContainer(env, auth, opts)
		if err != nil {
			log.Fatalf("Error creating container: %s", err)
		}
		creds := container.PullCredentials{Username: *registryUser, Password: *registryPassword}
		if err := container.PullImageIfNecessary(ctx, env, auth, c, creds, opts.ContainerImage); err != nil {
			log.Fatalf("Unable to PullImageIfNecessary: %s", err)
		}
		if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
			log.Fatalf("Unable to Create container: %s", err)
		}
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM)
	go func() {
		for {
			<-sigc
			log.Errorf("Capturing snapshot...")
			snapshotDigest, err := c.SaveSnapshot(ctx, *remoteInstanceName, nil, nil /*=baseSnapshotDigest*/)
			if err != nil {
				log.Fatalf("Error dumping snapshot: %s", err)
			}
			log.Printf("Created snapshot with ID %s/%d", snapshotDigest.GetHash(), snapshotDigest.GetSizeBytes())
		}
	}()

	if *actionDigest != "" {
		d, err := digest.Parse(*actionDigest)
		if err != nil {
			log.Fatalf("Error parsing action digest %q: %s", *actionDigest, err)
		}

		actionInstanceDigest := digest.NewResourceName(d, *remoteInstanceName)

		action, cmd, err := cachetools.GetActionAndCommand(ctx, env.GetByteStreamClient(), actionInstanceDigest)
		if err != nil {
			log.Fatal(err.Error())
		}
		out, _ := prototext.Marshal(action)
		log.Infof("Action:\n%s", string(out))
		out, _ = prototext.Marshal(cmd)
		log.Infof("Command:\n%s", string(out))

		tree, err := cachetools.GetTreeFromRootDirectoryDigest(ctx, env.GetContentAddressableStorageClient(), digest.NewResourceName(action.GetInputRootDigest(), *remoteInstanceName))
		if err != nil {
			log.Fatalf("Could not fetch input root structure: %s", err)
		}

		c.SetTaskFileSystemLayout(&container.FileSystemLayout{
			RemoteInstanceName: *remoteInstanceName,
			Inputs:             tree,
			OutputDirs:         cmd.GetOutputDirectories(),
			OutputFiles:        cmd.GetOutputFiles(),
		})

		_, err = c.SendPrepareFileSystemRequestToGuest(ctx, &vmfspb.PrepareRequest{})
		if err != nil {
			log.Fatalf("Error preparing VFS: %s", err)
		}
	}

	log.Printf("Started firecracker container!")
	log.Printf("To capture a snapshot at any time, send SIGTERM (killall vmstart)")
	if err := c.Wait(ctx); err != nil {
		log.Printf("Wait err: %s", err)
	}

	if err := c.Remove(ctx); err != nil {
		log.Errorf("Error removing container: %s", err)
	}
}
