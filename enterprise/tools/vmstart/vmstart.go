package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	image               = flag.String("image", "docker.io/library/busybox", "The default container to run.")
	registryUser        = flag.String("container_registry_user", "", "User to use when pulling the image")
	registryPassword    = flag.String("container_registry_password", "", "Password to use when pulling the image")
	cacheTarget         = flag.String("cache_target", "grpcs://remote.buildbuddy.dev", "The remote cache target")
	snapshotDir         = flag.String("snapshot_dir", "/tmp/filecache", "The directory to store snapshots in")
	snapshotDirMaxBytes = flag.Int64("snapshot_dir_max_bytes", 20_000_000_000, "The max number of bytes the snapshot_dir can be")
	remoteInstanceName  = flag.String("remote_instance_name", "", "The remote_instance_name for caching snapshots and interacting with the CAS if an action digest is specified")
	forceVMIdx          = flag.Int("force_vm_idx", -1, "VM index to force to avoid network conflicts -- random by default")
	apiKey              = flag.String("api_key", "", "The API key to use to interact with the remote cache.")
	actionDigest        = flag.String("action_digest", "", "The optional digest of the action you want to mount.")

	// Note: this key can be copied and pasted from logs:
	//
	//  "INFO: Fetched remote snapshot manifest {...}" // copy this JSON
	//
	// At the shell, paste it in single quotes: -remote_snapshot_key='<paste>'
	// Make sure to also set -api_key for proper auth. The API key must match
	// the group ID in the logs.
	remoteSnapshotKeyJSON = flag.String("remote_snapshot_key", "", "JSON struct containing a remote snapshot key that the VM should be resumed from.")

	tty  = flag.Bool("tty", false, "Enable debug terminal. This doesn't always work when resuming from snapshot.")
	repl = flag.Bool("repl", false, "Start a basic REPL for running bash commands with Exec().")
)

type RemoteSnapshotKey struct {
	GroupID      string `json:"group_id"`
	InstanceName string `json:"instance_name"`
	KeyDigest    string `json:"key_digest"`
	Key          any    `json:"key"`
}

func getToolEnv() *real_environment.RealEnv {
	healthChecker := healthcheck.NewHealthChecker("tool")
	re := real_environment.NewRealEnv(healthChecker)

	if err := tracing.Configure(re); err != nil {
		log.Fatalf("Failed to configure tracing: %s", err)
	}

	fc, err := filecache.NewFileCache(*snapshotDir, *snapshotDirMaxBytes, false)
	if err != nil {
		log.Fatalf("Unable to setup filecache %s", err)
	}
	fc.WaitForDirectoryScanToComplete()
	re.SetFileCache(fc)

	conn, err := grpc_client.DialSimple(*cacheTarget)
	if err != nil {
		log.Fatalf("Unable to connect to cache '%s': %s", *cacheTarget, err)
	}
	re.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	re.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	re.SetActionCacheClient(repb.NewActionCacheClient(conn))
	re.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	return re
}

func parseSnapshotKeyJSON(in string) (*RemoteSnapshotKey, *fcpb.SnapshotKey) {
	k := &RemoteSnapshotKey{}
	if err := json.Unmarshal([]byte(in), k); err != nil {
		log.Fatalf("Failed to parse remote snapshot key: %s", err)
	}
	b, err := json.Marshal(k.Key)
	if err != nil {
		log.Fatalf("Failed to marshal SnapshotKey: %s", err)
	}
	pk := &fcpb.SnapshotKey{}
	if err := protojson.Unmarshal(b, pk); err != nil {
		log.Fatalf("Failed to unmashal SnapshotKey: %s", err)
	}
	return k, pk
}

func parseDigest(in string) *repb.Digest {
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

func getActionAndCommand(ctx context.Context, bsClient bspb.ByteStreamClient, actionDigest *digest.ResourceName) (*repb.Action, *repb.Command, error) {
	action := &repb.Action{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, actionDigest, action); err != nil {
		return nil, nil, status.WrapErrorf(err, "could not fetch action")
	}
	cmd := &repb.Command{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, digest.NewResourceName(action.GetCommandDigest(), actionDigest.GetInstanceName(), rspb.CacheType_CAS, repb.DigestFunction_SHA256), cmd); err != nil {
		return nil, nil, status.WrapErrorf(err, "could not fetch command")
	}
	return action, cmd, nil
}

func main() {
	flag.Parse()

	flagutil.SetValueForFlagName("app.trace_fraction", 1, nil, false)
	flagutil.SetValueForFlagName("executor.firecracker_debug_stream_vm_logs", true, nil, false)
	if *tty {
		flagutil.SetValueForFlagName("executor.firecracker_debug_terminal", true, nil, false)
	}
	if *repl && *tty {
		log.Fatalf("Only one of -tty or -repl may be set.")
	}

	// Enable copy-on-write + remote snapshot sharing, but don't re-upload
	// back to the remote:
	if os.Getuid() != 0 {
		log.Fatalf("Must be run as root: 'bazel run --run_under=sudo'")
	}
	flagutil.SetValueForFlagName("executor.firecracker_enable_uffd", true, nil, false)
	flagutil.SetValueForFlagName("executor.firecracker_enable_vbd", true, nil, false)
	flagutil.SetValueForFlagName("executor.firecracker_enable_merged_rootfs", true, nil, false)
	flagutil.SetValueForFlagName("executor.enable_local_snapshot_sharing", true, nil, false)
	flagutil.SetValueForFlagName("executor.enable_remote_snapshot_sharing", true, nil, false)
	flagutil.SetValueForFlagName("executor.remote_snapshot_readonly", true, nil, false)

	rand.Seed(time.Now().Unix())

	env := getToolEnv()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	run(ctx, env)

	// Shut down and wait for traces to be flushed
	env.GetHealthChecker().Shutdown()
	env.GetHealthChecker().WaitForGracefulShutdown()
}

func run(ctx context.Context, env environment.Env) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if err := networking.DeleteNetNamespaces(ctx); err != nil {
		log.Warningf("Failed to clean up network namespaces: %s", err)
	}
	if err := vbd.CleanStaleMounts(); err != nil {
		log.Warningf("Failed to clean stale VBD mounts: %s", err)
	}

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
	cfg, err := firecracker.GetExecutorConfig(ctx, "/tmp/remote_build/")
	if err != nil {
		log.Fatalf("Failed to get executor configuration: %s", err)
	}
	opts := firecracker.ContainerOpts{
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			ScratchDiskSizeMb: 100,
			EnableNetworking:  true,
		},
		ContainerImage:         *image,
		ActionWorkingDirectory: emptyActionDir,
		ForceVMIdx:             vmIdx,
		ExecutorConfig:         cfg,
	}

	var c *firecracker.FirecrackerContainer
	if *remoteSnapshotKeyJSON != "" {
		// Runner recycling is needed to allow starting from snapshot.
		p, err := rexec.MakePlatform("recycle-runner=true")
		if err != nil {
			log.Fatal(err.Error())
		}
		task := &repb.ExecutionTask{Command: &repb.Command{Platform: p}}
		_, key := parseSnapshotKeyJSON(*remoteSnapshotKeyJSON)
		opts.SavedState = &rnpb.FirecrackerState{SnapshotKey: key}
		c, err = firecracker.NewContainer(ctx, env, task, opts)
		if err != nil {
			log.Fatalf("Error creating container: %s", err)
		}
		if err := c.LoadSnapshot(ctx); err != nil {
			log.Fatalf("Error loading snapshot: %s", err)
		}
	} else {
		c, err = firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
		if err != nil {
			log.Fatalf("Error creating container: %s", err)
		}
		creds := oci.Credentials{Username: *registryUser, Password: *registryPassword}
		if err := container.PullImageIfNecessary(ctx, env, c, creds, opts.ContainerImage); err != nil {
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
			if err := c.Pause(ctx); err != nil {
				log.Fatalf("Error dumping snapshot: %s", err)
			}
			log.Printf("Saved snapshot")
		}
	}()

	go func() {
		env.GetHealthChecker().WaitForGracefulShutdown()
		log.Infof("Shutdown: removing VM")
		if err := c.Remove(ctx); err != nil {
			log.Errorf("Failed to remove VM: %s", err)
		}
		os.Exit(2)
	}()

	if *actionDigest != "" {
		actionInstanceDigest, err := digest.ParseDownloadResourceName(*actionDigest)
		if err != nil {
			log.Fatalf("Error parsing action digest %q: %s", *actionDigest, err)
		}

		// For backwards compatibility with the existing behavior of this code:
		// If the parsed remote_instance_name is empty, and the flag instance
		// name is set; override the instance name of `rn`.
		if actionInstanceDigest.GetInstanceName() == "" && *remoteInstanceName != "" {
			actionInstanceDigest = digest.NewResourceName(actionInstanceDigest.GetDigest(), *remoteInstanceName, actionInstanceDigest.GetCacheType(), actionInstanceDigest.GetDigestFunction())
		}

		action, cmd, err := getActionAndCommand(ctx, env.GetByteStreamClient(), actionInstanceDigest)
		if err != nil {
			log.Fatal(err.Error())
		}
		out, _ := prototext.Marshal(action)
		log.Infof("Action:\n%s", string(out))
		out, _ = prototext.Marshal(cmd)
		log.Infof("Command:\n%s", string(out))

		tree, err := cachetools.GetTreeFromRootDirectoryDigest(ctx, env.GetContentAddressableStorageClient(), digest.NewResourceName(action.GetInputRootDigest(), *remoteInstanceName, rspb.CacheType_CAS, actionInstanceDigest.GetDigestFunction()))
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

	if *repl {
		log.Infof("Starting Exec() repl (enter bash commands; quit with Ctrl+D)")
		s := bufio.NewScanner(os.Stdin)
		for {
			fmt.Fprintf(os.Stderr, "bash> ")
			if !s.Scan() {
				break
			}
			res := c.Exec(ctx, &repb.Command{
				Arguments: []string{"bash", "-c", s.Text()},
			}, &commandutil.Stdio{
				Stdout: os.Stdout,
				Stderr: os.Stderr,
			})
			if res.Error != nil {
				log.Error(res.Error.Error())
			}
		}
	}

	if *tty {
		if err := c.Wait(ctx); err != nil {
			log.Errorf("Wait err: %s", err)
		}
	}

	if err := c.Remove(ctx); err != nil {
		log.Errorf("Error removing container: %s", err)
	}
}
