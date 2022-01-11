package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

var (
	server             = flag.String("server", "grpcs://remote.buildbuddy.dev", "")
	apiKey             = flag.String("api_key", "", "The API key of the account that owns the action.")
	remoteInstanceName = flag.String("remote_instance_name", "", "The remote instance name, if one should be used.")
	repo               = flag.String("repo", "", "git repo URL")
	patch              = flag.String("patch", "", "Optional patch to apply after checking out the repo")
	affinityKey        = flag.String("affinity_key", "", "Preference for which Bazel instance the request is routed to")
)

const (
	escapeSeq = "\u001B["
)

func consoleCursorMoveUp(y int) {
	fmt.Print(escapeSeq + strconv.Itoa(y) + "A")
}

func consoleCursorMoveBeginningLine() {
	fmt.Print(escapeSeq + "1G")
}

func consoleDeleteLines(n int) {
	fmt.Print(escapeSeq + strconv.Itoa(n) + "M")
}

func main() {
	flag.Parse()

	if *server == "" {
		log.Fatalf("--server is required")
	}
	if *apiKey == "" {
		log.Fatalf("--api_key is required")
	}
	if *repo == "" {
		log.Fatalf("--repo is required")
	}

	if len(flag.Args()) == 0 {
		log.Fatalf("Usage: runremote <bazel subcommand> [args]")
	}

	conn, err := grpc_client.DialTarget(*server)
	if err != nil {
		log.Fatalf("Could not connect to server %q: %s", *server, err)
	}
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", *apiKey)

	// --isatty=1 makes Bazel think it's running in an interactive terminal which enables nicer UI output
	// TODO(vadim): propagate terminal width
	cmd := flag.Args()[0] + " --isatty=1 " + strings.Join(flag.Args()[1:], " ")

	log.Infof("Sending request...")

	req := &rnpb.RunRequest{
		GitRepo: &rnpb.RunRequest_GitRepo{
			RepoUrl: *repo,
		},
		RepoState:          &rnpb.RunRequest_RepoState{},
		BazelCommand:       cmd,
		InstanceName:       *remoteInstanceName,
		SessionAffinityKey: *affinityKey,
	}
	if *patch != "" {
		patchContents, err := os.ReadFile(*patch)
		if err != nil {
			log.Fatalf("Could not read patch file: %s", err)
		}
		req.GetRepoState().Patch = append(req.GetRepoState().Patch, string(patchContents))
	}

	rsp, err := bbClient.Run(ctx, req)
	if err != nil {
		log.Fatalf("Error running command: %s", err)
	}

	iid := rsp.GetInvocationId()

	log.Infof("Invocation ID: %s", iid)

	chunkID := ""
	moveBack := 0
	for {
		l, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
			InvocationId: iid,
			ChunkId:      chunkID,
			MinLines:     100,
		})
		if err != nil {
			log.Fatalf("Error looking up logs: %s", err)
		}

		if l.GetNextChunkId() == "" {
			break
		}

		// Are we redrawing the current chunk?
		if moveBack > 0 {
			consoleCursorMoveUp(moveBack)
			consoleCursorMoveBeginningLine()
			consoleDeleteLines(moveBack)
		}
		if !l.GetLive() {
			moveBack = 0
		} else {
			moveBack = len(strings.Split(string(l.GetBuffer()), "\n"))
		}

		_, _ = os.Stdout.Write(l.GetBuffer())
		_, _ = os.Stdout.Write([]byte("\n"))

		if l.GetNextChunkId() == chunkID {
			time.Sleep(1 * time.Second)
		}
		chunkID = l.GetNextChunkId()
	}
}
