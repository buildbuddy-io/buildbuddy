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

	showMetadata     = flag.Bool("metadata", false, "Whether to fetch and log metadata for the digest (printed to stderr).")
	showMetadataOnly = flag.Bool("metadata_only", false, "Whether to *only* fetch metadata, not the contents. This will print the metadata to stdout instead of stderr.")
)

// Examples:
//
// Show an action result proto:
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=/blobs/ac/HASH/SIZE -type=ActionResult
//
// Show a command proto:
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=/blobs/HASH/SIZE -type=Command
//
// Show stderr contents:
//
//	bazel run //tools/cas -- -target=grpcs://remote.buildbuddy.dev -digest=/blobs/HASH/SIZE -type=stderr
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

	var ind *rspb.ResourceName
	if *blobType == "ActionResult" {
		indDownload, err := digest.ParseActionCacheResourceName(digestString)
		if err != nil {
			log.Fatal(status.Message(err))
		}
		ind = indDownload.ToProto()
	} else {
		indDownload, err := digest.ParseDownloadResourceName(digestString)
		if err != nil {
			log.Fatal(status.Message(err))
		}
		ind = indDownload.ToProto()
	}

	// For backwards compatibility with the existing behavior of this code:
	// If the parsed remote_instance_name is empty, and the flag instance
	// name is set; override the instance name of `rn`.
	if ind.GetInstanceName() == "" && *instanceName != "" {
		ind = digest.NewResourceName(ind.GetDigest(), *instanceName, ind.GetCacheType(), ind.GetDigestFunction()).ToProto()
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
			ResourceName: ind,
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
		r, err := digest.CASResourceNameFromProto(ind)
		if err != nil {
			log.Fatalf("Failed to convert resource name to CAS: %s", err)
		}
		if err := cachetools.GetBlob(ctx, bsClient, r, &out); err != nil {
			log.Fatal(err.Error())
		}
		writeToStdout(out.Bytes())
		return
	}

	// Handle ActionResults (these are stored in the action cache)
	if *blobType == "ActionResult" {
		r, err := digest.ACResourceNameFromProto(ind)
		if err != nil {
			log.Fatalf("Failed to convert resource name to AC: %s", err)
		}
		ar, err := cachetools.GetActionResult(ctx, acClient, r)
		if err != nil {
			log.Fatal(err.Error())
		}
		printMessage(ar)
		return
	}

	// Handle Trees (these are stored in the CAS)
	if *blobType == "Tree" {
		r, err := digest.CASResourceNameFromProto(ind)
		if err != nil {
			log.Fatalf("Failed to convert resource name to CAS: %s", err)
		}
		inputTree, err := cachetools.GetTreeFromRootDirectoryDigest(ctx, casClient, r)
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
	r, err := digest.CASResourceNameFromProto(ind)
	if err != nil {
		log.Fatalf("Failed to convert resource name to CAS: %s", err)
	}
	if err := cachetools.GetBlobAsProto(ctx, bsClient, r, msg); err != nil {
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
