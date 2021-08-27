package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/casfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
)

var (
	server             = flag.String("server", "", "")
	dir                = flag.String("dir", "", "")
	apiKey             = flag.String("api_key", "", "The API key of the account that owns the action.")
	actionID           = flag.String("action_id", "", "The ID of the action you want to mount.")
	remoteInstanceName = flag.String("remote_instance_name", "", "The remote instance name, if one should be used.")
	cpuProfile         = flag.String("cpuprofile", "", "If set, CPU profile will be written to given file.")
)

// XXX: don't use fatal, it messes up the signal handler
func main() {
	flag.Parse()
	log.Configure(log.Opts{Level: "debug", EnableShortFileName: true})

	if *server == "" {
		log.Fatalf("--server is required")
	}
	if *dir == "" {
		log.Fatalf("--dir is required")
	}
	if *actionID == "" {
		log.Fatalf("--action_id is required")
	}
	if *apiKey == "" {
		log.Fatalf("--api_key is required")
	}

	conn, err := grpc_client.DialTarget(*server)
	if err != nil {
		log.Fatalf("Could not connect to server %q: %s", *server, err)
	}
	bsClient := bspb.NewByteStreamClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)

	fileFetcher := dirtools.NewBatchFileFetcher(ctx, *remoteInstanceName, nil, bsClient, casClient)

	aidParts := strings.SplitN(*actionID, "/", 2)
	if len(aidParts) != 2 || len(aidParts[0]) != 64 {
		log.Fatalf("Error parsing actionID %q: should be of form 'f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142'", *actionID)
	}
	i, err := strconv.ParseInt(aidParts[1], 10, 64)
	if err != nil {
		log.Fatalf("Error parsing actionID %q: %s", *actionID, err.Error())
	}
	d := &repb.Digest{
		Hash:      aidParts[0],
		SizeBytes: i,
	}
	// Fetch the action to ensure it exists.
	actionInstanceDigest := digest.NewInstanceNameDigest(d, *remoteInstanceName)
	action := &repb.Action{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, actionInstanceDigest, action); err != nil {
		log.Fatalf("Error fetching action: %s", err)
	}
	log.Infof("Fetched action: %s", proto.MarshalTextString(action))

	action.GetCommandDigest()
	command := &repb.Command{}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, digest.NewInstanceNameDigest(action.GetCommandDigest(), *remoteInstanceName), command); err != nil {
		log.Fatalf("Error fetching command: %s", err)
	}
	log.Infof("Fetched command: %s", proto.MarshalTextString(action))

	scratchDir, err := os.MkdirTemp("", "casfs-scratch-")
	if err != nil {
		log.Fatalf("Could not create temp scratch dir: %s", err)
	}

	dirs, err := dirtools.GetTreeFromRootDirectoryDigest(ctx, casClient, digest.NewInstanceNameDigest(action.GetInputRootDigest(), *remoteInstanceName))
	if err != nil {
		log.Fatalf("Could not fetch input root structure: %s", err)
	}

	layout := &container.FileSystemLayout{
		Inputs:      dirs,
		OutputDirs:  command.GetOutputDirectories(),
		OutputFiles: command.GetOutputFiles(),
	}

	fs := casfs.New(scratchDir, &casfs.Options{Verbose: true, LogFUSEOps: true})
	err = fs.Mount(*dir)
	if err != nil {
		log.Fatalf("Could not mount filesystem at %q: %s", *dir, err)
	}

	err = fs.PrepareForTask(context.Background(), fileFetcher, *actionID, layout)
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
		log.Infof("Cleaning up scratch dir %q", scratchDir)
		if err := os.RemoveAll(scratchDir); err != nil {
			log.Warningf("Could not clean up scratch dir: %s", err)
		}
		err = fs.Unmount()
		if err != nil {
			log.Warningf("Could not umount casfs: %s", err)
		}

		if *cpuProfile != "" {
			pprof.StopCPUProfile()
		}

		os.Exit(0)
	}()
	signal.Notify(interrupt, os.Interrupt)

	actionScript := filepath.Join(*dir, "buildbuddy_run_action.sh")

	f, err := os.OpenFile(actionScript, os.O_WRONLY|os.O_CREATE, 0555)
	if err != nil {
		log.Errorf("Could not create action script: %s", err)
		return
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
		log.Errorf("Could not write action script: %s", err)
		return
	}
	if err := f.Close(); err != nil {
		log.Errorf("Could not close action script; %s", err)
		return
	}

	log.Infof("Action script written to %q", actionScript)

	select {}
}
