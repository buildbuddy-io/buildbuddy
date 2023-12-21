package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mattn/go-isatty"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	// Required flags:

	target     = flag.String("target", "", "Cache grpc target, such as grpcs://remote.buildbuddy.io")
	blobDigest = flag.String("digest", "", "Digest of the blob to fetch, in HASH/SIZE format.")
	blobType   = flag.String("type", "", "Type of blob to inspect: Action, ActionResult, Command, Tree, file, stdout, stderr")

	// Optional flags:

	instanceName = flag.String("remote_instance_name", "", "Remote instance name")
	apiKey       = flag.String("api_key", "", "API key to attach to the outgoing context")
	invocationID = flag.String("invocation_id", "", "Invocation ID. This is required when fetching the result of a failed action. Otherwise, it's optional.")

	showMetadata     = flag.Bool("metadata", false, "Whether to fetch and log metadata for the digest (printed to stderr).")
	showMetadataOnly = flag.Bool("metadata_only", false, "Whether to *only* fetch metadata, not the contents. This will print the metadata to stdout instead of stderr.")
)

// Examples:
//
// Show an action result proto:
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=ActionResult
//
// Show a command proto:
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=Command
//
// Show stderr contents:
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=stderr
//
// Show a failed action result proto (requires invocation ID):
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=HASH/SIZE -type=ActionResult -invocation_id=IID
func main() {
	flag.Parse()
	if *target == "" {
		log.Fatalf("Missing --target")
	}
	if *blobDigest == "" {
		log.Fatalf("Missing --digest")
	}

	// For backwards compatibility, attempt to fixup old style digest
	// strings that don't start with a '/blobs/' prefix.
	digestString := *blobDigest
	if !strings.HasPrefix(digestString, "/blobs") {
		digestString = "/blobs/" + digestString
	}

	ind, err := digest.ParseDownloadResourceName(digestString)
	if err != nil {
		log.Fatalf(status.Message(err))
	}
	if *blobType == "ActionResult" {
		ind = digest.NewResourceName(ind.GetDigest(), ind.GetInstanceName(), rspb.CacheType_AC, ind.GetDigestFunction())
	}

	// For backwards compatibility with the existing behavior of this code:
	// If the parsed remote_instance_name is empty, and the flag instance
	// name is set; override the instance name of `rn`.
	if ind.GetInstanceName() == "" && *instanceName != "" {
		ind = digest.NewResourceName(ind.GetDigest(), *instanceName, ind.GetCacheType(), ind.GetDigestFunction())
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		log.Fatalf("Error dialing CAS target: %s", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)
	acClient := repb.NewActionCacheClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)
	bbClient := bbspb.NewBuildBuddyServiceClient(conn)

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	if *showMetadata || *showMetadataOnly {
		req := &capb.GetCacheMetadataRequest{
			ResourceName: ind.ToProto(),
		}
		md, err := bbClient.GetCacheMetadata(ctx, req)
		if err != nil {
			log.Fatalf("Failed to get metadata: %s", err)
		}
		log.Infof(
			"Metadata: accessed=%q, modified=%q",
			time.UnixMicro(md.GetLastAccessUsec()),
			time.UnixMicro(md.GetLastModifyUsec()),
		)
		// If --metadata_only is set, print the metadata as JSON to stdout for
		// easy consumption in scripts.
		if *showMetadataOnly {
			b, err := protojson.MarshalOptions{Multiline: false}.Marshal(md)
			if err != nil {
				log.Fatalf("Failed to marshal metadata: %s", err)
			}
			writeToStdout(b)
			return
		}
	}

	// Handle raw string types
	if *blobType == "stdout" || *blobType == "stderr" || *blobType == "file" {
		var out bytes.Buffer
		if err := cachetools.GetBlob(ctx, bsClient, ind, &out); err != nil {
			log.Fatal(err.Error())
		}
		writeToStdout(out.Bytes())
		return
	}

	// Handle ActionResults (these are stored in the action cache)
	if *blobType == "ActionResult" {
		ar, err := cachetools.GetActionResult(ctx, acClient, ind)
		if err != nil {
			log.Infof("Could not fetch ActionResult; maybe the action failed. Attempting to fetch failed action using invocation ID = %q", *invocationID)
			failedDigest, err := digest.AddInvocationIDToDigest(ind.GetDigest(), ind.GetDigestFunction(), *invocationID)
			if err != nil {
				log.Fatalf(err.Error())
			}
			ind := digest.NewResourceName(failedDigest, ind.GetInstanceName(), rspb.CacheType_AC, repb.DigestFunction_SHA256)
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
		log.Fatalf(`Invalid --type: %q (allowed values: Action, ActionResult, Command, Tree, file, stderr, stdout)`, *blobType)
	}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, ind, msg); err != nil {
		log.Fatal(err.Error())
	}
	printMessage(msg)
}

func printMessage(msg proto.Message) {
	out, _ := protojson.MarshalOptions{Multiline: true}.Marshal(msg)
	writeToStdout(out)
}

func writeToStdout(b []byte) {
	os.Stdout.Write(b)
	// Print a trailing newline if there isn't one already, but only if stdout
	// is a terminal, to avoid incorrect digest computations e.g. when piping to
	// `sha256sum`
	if (len(b) == 0 || b[len(b)-1] != '\n') && isatty.IsTerminal(os.Stdout.Fd()) {
		os.Stdout.Write([]byte{'\n'})
	}
}
