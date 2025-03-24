package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	server             = flag.String("server", "", "CAS endpoint")
	scratchDir         = flag.String("scratch_dir", "", "Backing directory for the VFS. If not set, a new temp directory will be created.")
	dir                = flag.String("dir", "", "Where the filesystem will be mounted")
	apiKey             = flag.String("api_key", "", "The API key to use to fetch action & contents")
	actionDigest       = flag.String("action_digest", "", "The digest of the action you want to mount.")
	remoteInstanceName = flag.String("remote_instance_name", "", "The remote instance name, if one should be used.")
	cpuProfile         = flag.String("cpuprofile", "", "If set, CPU profile will be written to given file. Use 'go tool pprof' to view it.")
)

func getActionAndCommand(ctx context.Context, bsClient bspb.ByteStreamClient, actionDigest *digest.ResourceName) (*repb.Action, *repb.Command, error) {
	action := &repb.Action{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, actionDigest, action); err != nil {
		return nil, nil, status.WrapErrorf(err, "could not fetch action")
	}
	cmd := &repb.Command{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, digest.NewResourceName(action.GetCommandDigest(), actionDigest.GetInstanceName(), rspb.CacheType_CAS, repb.DigestFunction_BLAKE3), cmd); err != nil {
		return nil, nil, status.WrapErrorf(err, "could not fetch command")
	}
	return action, cmd, nil
}

func main() {
	flag.Parse()
	*log.LogLevel = "debug"
	*log.IncludeShortFileName = true
	log.Configure()

	if *server == "" {
		log.Fatalf("--server is required")
	}
	if *dir == "" {
		log.Fatalf("--dir is required")
	}
	if *apiKey == "" {
		log.Fatalf("--api_key is required")
	}

	conn, err := grpc_client.DialSimple(*server)
	if err != nil {
		log.Fatalf("Could not connect to server %q: %s", *server, err)
	}
	bsClient := bspb.NewByteStreamClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)

	healthChecker := healthcheck.NewHealthChecker("vmexec")
	env := real_environment.NewRealEnv(healthChecker)
	env.SetByteStreamClient(bsClient)
	env.SetContentAddressableStorageClient(casClient)

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)

	vfsScratchDir := *scratchDir
	if vfsScratchDir == "" {
		vfsScratchDir, err = os.MkdirTemp("", "vfs-scratch-")
		if err != nil {
			log.Fatalf("Could not create temp scratch dir: %s", err)
		}
	}

	log.Infof("Using scratch dir %q", vfsScratchDir)

	layout := &container.FileSystemLayout{
		Inputs: &repb.Tree{},
	}

	if *actionDigest != "" {
		// For backwards compatibility, attempt to fixup old style digest
		// strings that don't start with a '/blobs/' prefix.
		digestString := *actionDigest
		if !strings.HasPrefix(digestString, "/blobs") {
			digestString = "/blobs/" + digestString
		}

		actionRN, err := digest.ParseDownloadResourceName(digestString)
		if err != nil {
			log.Fatalf("Error parsing action digest %q: %s", *actionDigest, err)
		}

		action, command, err := getActionAndCommand(ctx, bsClient, actionRN)
		if err != nil {
			log.Fatalf("Error fetching action: %s", err)
		}

		tree, err := cachetools.GetTreeFromRootDirectoryDigest(ctx, casClient, digest.NewResourceName(action.GetInputRootDigest(), *remoteInstanceName, rspb.CacheType_CAS, actionRN.GetDigestFunction()))
		if err != nil {
			log.Fatalf("Could not fetch input root structure: %s", err)
		}

		layout = &container.FileSystemLayout{
			RemoteInstanceName: *remoteInstanceName,
			DigestFunction:     actionRN.GetDigestFunction(),
			Inputs:             tree,
		}

		actionScript := filepath.Join(vfsScratchDir, "buildbuddy_run_action.sh")
		f, err := os.OpenFile(actionScript, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0555)
		if err != nil {
			log.Fatalf("Could not create action script: %s", err)
		}

		script := "#!/bin/bash\n"
		script += "cd " + filepath.Join(*dir, command.GetWorkingDirectory()) + "\n"
		script += "env -i \\\n"
		for _, e := range command.GetEnvironmentVariables() {
			script += "  " + e.GetName() + "=" + e.GetValue() + " \\\n"
		}
		for _, a := range command.GetArguments() {
			script += "  '" + a + "' \\\n"
		}
		script += "\n"

		_, err = f.WriteString(script)
		if err != nil {
			log.Fatalf("Could not write action script: %s", err)
			return
		}
		if err := f.Close(); err != nil {
			log.Fatalf("Could not close action script; %s", err)
			return
		}

		log.Infof("Action script written to %q", actionScript)
	}

	vfsServer, err := vfs_server.New(env, vfsScratchDir)
	if err != nil {
		log.Fatalf("Error creating vfs server: %s", err)
	}
	if err := vfsServer.Prepare(ctx, layout); err != nil {
		log.Fatalf("Could not prepare VFS server: %s", err)
	}
	vfsClient := vfs_server.NewDirectClient(vfsServer)

	fs := vfs.New(vfsClient, *dir, &vfs.Options{})
	err = fs.Mount()
	if err != nil {
		log.Fatalf("Could not mount filesystem at %q: %s", *dir, err)
	}

	err = fs.PrepareForTask(ctx, *actionDigest)
	if err != nil {
		log.Fatalf("Could not prepare layout: %s", err)
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Could not create CPU profile file: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Could not start CPU profiling: %s", err)
		}
	}

	interrupt := make(chan os.Signal, 1)
	go func() {
		<-interrupt
		log.CtxInfof(ctx, "shutting down...")
		_ = fs.FinishTask()
		if *scratchDir == "" {
			log.Infof("Cleaning up scratch dir %q", vfsScratchDir)
			if err := os.RemoveAll(*scratchDir); err != nil {
				log.Warningf("Could not clean up scratch dir: %s", err)
			}
		}
		log.CtxInfof(ctx, "unmounting...")
		err = fs.Unmount()
		if err != nil {
			log.Warningf("Could not umount vfs: %s", err)
		}

		if *cpuProfile != "" {
			pprof.StopCPUProfile()
		}

		os.Exit(0)
	}()
	signal.Notify(interrupt, os.Interrupt)

	select {}
}
