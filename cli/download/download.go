package download

import (
	"context"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mattn/go-isatty"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	flags = flag.NewFlagSet("download", flag.ContinueOnError)

	target     = flags.String("target", "grpcs://remote.buildbuddy.io", "Cache gRPC target")
	blobType   = flags.String("type", "", "Type of blob (used to interpret): Action, Command, Directory")
	outputFile = flags.String("output_file", "", "A destination file where the output should be written; stdout will be used if not set")

	usage = `
usage: bb ` + flags.Name() + ` {digest}/{size}

Downloads the blob identified by digest from the CAS and outputs its contents.

If the blob type is specified, the downloaded bytes are interpreted as a proto
of the specified type and text-marshalled before being output. Otherwise the raw
uninterpreted bytes will be output.

Example of downloading a blob and piping it to another command:
  $ bb download 686f25dbadc55ac13aca8580efead40b84d2a812278900b95ce15014dfd94702/80 | sha256sum

Example of displaying an Action with a remote instance name to stdout:
  $ bb download /foo/blobs/b8907c47a5c0954a440c673832f6e219e33311f577a59ea7c9de4686900140ac/234 --type=Action

Example of dumping a Command to a file:
  $ bb download 0e83f9edaff969afa4d16de9f8f5c1de2778148a982e1c19c744fc11a4f96811/1321 --type=Command --output_file=cmd.pb.txt
`
)

type newlineStdoutCloser struct {
	*os.File
}

func (n *newlineStdoutCloser) Close() error {
	if isatty.IsTerminal(n.File.Fd()) {
		n.File.Write([]byte{'\n'})
	}
	return n.File.Close()
}

func getOutput() (io.WriteCloser, error) {
	if *outputFile != "" {
		return os.OpenFile(*outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
	return &newlineStdoutCloser{os.Stdout}, nil
}

func printProtoToOutput(msg proto.Message, output io.Writer) error {
	buf, err := protojson.MarshalOptions{Multiline: true}.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = output.Write(buf)
	return err
}

func downloadFile(uri string) error {
	if !strings.HasPrefix(uri, "/blobs") {
		uri = "/blobs/" + uri
	}
	ind, err := digest.ParseDownloadResourceName(uri)
	if err != nil {
		return err
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}
	bsClient := bspb.NewByteStreamClient(conn)

	var msg proto.Message
	switch *blobType {
	case "":
		msg = nil // if nil; do not interpret bytes
	case "Action":
		msg = &repb.Action{}
	case "Command":
		msg = &repb.Command{}
	case "Directory":
		msg = &repb.Directory{}
	default:
		return status.InvalidArgumentErrorf(`Invalid --type: %q (allowed values: Action, Command, Directory, "")`, *blobType)
	}

	ctx := context.Background()
	if apiKey, err := storage.ReadRepoConfig("api-key"); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	wc, err := getOutput()
	if err != nil {
		return err
	}
	defer wc.Close()

	if msg != nil {
		if err := cachetools.GetBlobAsProto(ctx, bsClient, ind, msg); err != nil {
			return err
		}
		return printProtoToOutput(msg, wc)
	}
	return cachetools.GetBlob(ctx, bsClient, ind, wc)
}

func HandleDownload(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	if len(flags.Args()) != 1 {
		log.Print(usage)
		return 1, nil
	}

	if *target == "" {
		log.Printf("A non-empty --target must be specified")
		return 1, nil
	}

	uri := flags.Args()[0]
	if err := downloadFile(uri); err != nil {
		log.Print(err)
		return 1, nil
	}
	return 0, nil
}
