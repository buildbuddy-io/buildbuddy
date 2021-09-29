package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	blobDigest   = flag.String("digest", "", "CAS digest")
	instanceName = flag.String("remote_instance_name", "", "Remote instance name")
	target       = flag.String("target", "", "CAS grpc target")
	apiKey       = flag.String("api_key", "", "API key to attach to the outgoing context")

	invocationID = flag.String("invocation_id", "", "Invocation ID. This is required when fetching the result of a failed action.")

	blobType = flag.String("type", "", "Type of proto to inspect.")
)

func main() {
	flag.Parse()
	parts := strings.Split(*blobDigest, "/")
	if len(parts) != 2 {
		log.Fatalf("Invalid digest format: expecting {hash}/{size}, got %q", *blobDigest)
	}
	sizeBytes, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("Error parsing digest: %s", err)
	}
	ind := digest.NewResourceName(&repb.Digest{
		Hash:      parts[0],
		SizeBytes: int64(sizeBytes),
	}, *instanceName)
	conn, err := grpc_client.DialTarget(*target)
	if err != nil {
		log.Fatalf("Error dialing CAS target: %s", err.Error())
	}
	bsClient := bspb.NewByteStreamClient(conn)
	acClient := repb.NewActionCacheClient(conn)

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}
	var msg proto.Message
	switch *blobType {
	case "Action":
		msg = &repb.Action{}
	case "ActionResult":
		msg = &repb.ActionResult{}
	case "Command":
		msg = &repb.Command{}
	case "stdout": // NOP
	case "stderr": // NOP
	default:
		log.Fatalf(`Invalid --type: %q (must be a proto message name like "Action", "ActionResult", "Command", ...)`, *blobType)
	}
	if *blobType == "stdout" || *blobType == "stderr" {
		var out bytes.Buffer
		if err := cachetools.GetBlob(ctx, bsClient, ind, &out); err != nil {
			log.Fatal(err.Error())
		}
		fmt.Println(out.String())
	} else if *blobType == "ActionResult" {
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
		msg = ar
		fmt.Println(proto.MarshalTextString(msg))
	} else {
		if err := cachetools.GetBlobAsProto(ctx, bsClient, ind, msg); err != nil {
			log.Fatal(err.Error())
		}
		fmt.Println(proto.MarshalTextString(msg))
	}
}
