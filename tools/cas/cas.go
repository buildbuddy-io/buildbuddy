package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	// Required flags:

	target     = flag.String("target", "", "Cache grpc target, such as grpcs://remote.buildbuddy.io")
	blobDigest = flag.String("digest", "", "Digest of the blob to fetch, in HASH/SIZE format.")
	blobType   = flag.String("type", "", "Type of blob to inspect: Action, ActionResult, Command, file, stdout, stderr")

	// Optional flags:

	instanceName = flag.String("remote_instance_name", "", "Remote instance name")
	apiKey       = flag.String("api_key", "", "API key to attach to the outgoing context")
	invocationID = flag.String("invocation_id", "", "Invocation ID. This is required when fetching the result of a failed action. Otherwise, it's optional.")
)

// Examples:
//
// Show an action result proto:
//     bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=ActionResult
//
// Show a command proto:
//     bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=Command
//
// Show stderr contents:
//     bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=stderr
//
// Show a failed action result proto (requires invocation ID):
//     bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=ActionResult -invocation_id=IID
func main() {
	flag.Parse()
	if *target == "" {
		log.Fatalf("Missing --target")
	}
	if *blobDigest == "" {
		log.Fatalf("Missing --digest")
	}
	d, err := digest.Parse(*blobDigest)
	if err != nil {
		log.Fatalf(status.Message(err))
	}

	ind := digest.NewResourceName(d, *instanceName)
	conn, err := grpc_client.DialTarget(*target)
	if err != nil {
		log.Fatalf("Error dialing CAS target: %s", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)
	acClient := repb.NewActionCacheClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	// Handle raw string types
	if *blobType == "stdout" || *blobType == "stderr" || *blobType == "file" {
		var out bytes.Buffer
		if err := cachetools.GetBlob(ctx, bsClient, ind, &out); err != nil {
			log.Fatal(err.Error())
		}
		fmt.Println(out.String())
		return
	}

	// Handle ActionResults (these are stored in the action cache)
	if *blobType == "ActionResult" {
		ar, err := cachetools.GetActionResult(ctx, acClient, ind)
		if err != nil {
			log.Infof("Could not fetch ActionResult; maybe the action failed. Attempting to fetch failed action using invocation ID = %q", *invocationID)
			failedDigest, err := digest.AddInvocationIDToDigest(ind.GetDigest(), *invocationID)
			if err != nil {
				log.Fatalf(err.Error())
			}
			ind := digest.NewResourceName(failedDigest, ind.GetInstanceName())
			ar, err = cachetools.GetActionResult(ctx, acClient, ind)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
		printMessage(ar)
		return
	}

	// Handle Trees (these are stored in the CAS)
	if *blobType == "Tree" {
		inputTree, err := cachetools.GetTreeFromRootDirectoryDigest(ctx, casClient, ind)
		if err != nil {
			log.Fatal(err.Error())
		}
		printMessage(inputTree)
		return
	}

	// Handle well-known protos
	var msg proto.Message
	switch *blobType {
	case "Action":
		msg = &repb.Action{}
	case "Command":
		msg = &repb.Command{}
	default:
		log.Fatalf(`Invalid --type: %q (allowed values: Action, ActionResult, Command, file, stderr, stdout)`, *blobType)
	}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, ind, msg); err != nil {
		log.Fatal(err.Error())
	}
	printMessage(msg)
}

func printMessage(msg proto.Message) {
	out, _ := prototext.MarshalOptions{Multiline: true}.Marshal(msg)
	fmt.Println(string(out))
}
