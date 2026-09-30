package download

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/mdutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/docker/go-units"
	"github.com/mattn/go-isatty"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	flags = flag.NewFlagSet("download", flag.ContinueOnError)
	Flags = flags

	target          = flags.String("target", login.DefaultApiTarget, "Cache gRPC target")
	blobType        = flags.String("type", "", "Type of blob (used to interpret): Action, Command, Directory")
	outputDirectory = flags.String("output_directory", "", "A directory where Directory contents will be recursively extracted. Implies --type=Directory.")
	outputFile      = flags.String("output_file", "", "A destination file where the output should be written; stdout will be used if not set")
	remoteHeader    = flag.New(flags, "remote_header", []string{}, "Arbitrary remote headers to set on the request, in `KEY=VALUE` format. Can be specified multiple times.")
	apiKey          = flags.String("api_key", "", "Optionally override the API key with this value")

	usage = `
usage: bb ` + flags.Name() + ` <digest>/<size>
       bb ` + flags.Name() + ` artifacts <invocation-id-or-url> [--output_directory=DIR]

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

Example of downloading all artifacts attached to an invocation:
  $ bb download artifacts https://app.buildbuddy.io/invocation/01234567-89ab-cdef-0123-456789abcdef --output_directory=/tmp/artifacts
`
)

type newlineStdoutCloser struct {
	io.WriteCloser
}

func (n *newlineStdoutCloser) Close() error {
	if f, ok := n.WriteCloser.(*os.File); ok && isatty.IsTerminal(f.Fd()) {
		n.WriteCloser.Write([]byte{'\n'})
	}
	return n.WriteCloser.Close()
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

func downloadFile(ctx context.Context, ind *digest.CASResourceName, bsClient bspb.ByteStreamClient) error {
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

	wc, err := getOutput()
	if err != nil {
		return err
	}
	defer wc.Close()

	if msg != nil {
		if err := cachetools.GetBlobAsProto(ctx, bsClient, ind, msg); err != nil {
			return fmt.Errorf("get blob as %T proto: %w", msg, err)
		}
		return printProtoToOutput(msg, wc)
	}
	return cachetools.GetBlob(ctx, bsClient, ind, wc)
}

func downloadArtifacts(ctx context.Context, client apipb.ApiServiceClient, invocationID, outputDir string) error {
	rsp, err := client.GetInvocation(ctx, &apipb.GetInvocationRequest{
		Selector:         &apipb.InvocationSelector{InvocationId: invocationID},
		IncludeArtifacts: true,
	})
	if err != nil {
		return fmt.Errorf("get invocation: %w", err)
	}
	if len(rsp.GetInvocation()) == 0 {
		return fmt.Errorf("invocation %s not found", invocationID)
	}
	artifacts := rsp.GetInvocation()[0].GetArtifacts()
	if len(artifacts) == 0 {
		log.Printf("Invocation %s has no artifacts", invocationID)
		return nil
	}

	paths, skipped := artifactOutputPaths(artifacts, outputDir)
	if skipped > 0 {
		log.Warnf("Skipped %d artifacts due to invalid paths or duplicates", skipped)
	}

	downloaded := 0
	for i, artifact := range artifacts {
		path := paths[i]
		if path == "" {
			continue
		}
		if strings.HasPrefix(artifact.GetUri(), "file://") {
			log.Debugf("Skipping local artifact %q (%s)", artifact.GetName(), artifact.GetUri())
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf("create artifact directory: %w", err)
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("create artifact %q: %w", path, err)
		}
		var getFileErr error
		if artifact.GetUri() == "" {
			_, err = f.Write(artifact.GetContents())
		} else {
			stream, streamErr := client.GetFile(ctx, &apipb.GetFileRequest{Uri: artifact.GetUri()})
			getFileErr = streamErr
			if streamErr == nil {
				for {
					chunk, recvErr := stream.Recv()
					if recvErr == io.EOF {
						break
					}
					if recvErr != nil {
						getFileErr = recvErr
						break
					}
					if _, err = f.Write(chunk.GetData()); err != nil {
						break
					}
				}
			}
		}
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("download artifact %q: %w", artifact.GetName(), err)
		}
		if closeErr != nil {
			return fmt.Errorf("close artifact %q: %w", artifact.GetName(), closeErr)
		}
		if getFileErr != nil {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				log.Warnf("Failed to remove partial artifact %q: %s", artifact.GetName(), err)
			}
			log.Warnf("Skipping artifact %q after GetFile failed: %s", artifact.GetName(), getFileErr)
			continue
		}
		log.Debugf("Downloaded %s", path)
		downloaded++
	}
	log.Printf("Downloaded %d artifact(s) to %s", downloaded, outputDir)
	return nil
}

func artifactOutputPaths(artifacts []*apipb.File, outputDir string) ([]string, int) {
	paths := make([]string, len(artifacts))
	seen := make(map[string]struct{}, len(artifacts))
	skipped := 0
	for i, artifact := range artifacts {
		name := filepath.Clean(filepath.FromSlash(artifact.GetName()))
		if name == "." || !filepath.IsLocal(name) {
			log.Warnf("Skipping artifact with invalid path %q", name)
			skipped++
			continue
		}
		if _, ok := seen[name]; ok {
			log.Warnf("Skipping duplicate artifact path %q (URI %q)", name, artifact.GetUri())
			skipped++
			continue
		}
		seen[name] = struct{}{}
		paths[i] = filepath.Join(outputDir, name)
	}
	return paths, skipped
}

func HandleDownload(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	artifactMode := len(flags.Args()) > 0 && flags.Args()[0] == "artifacts"
	if (!artifactMode && len(flags.Args()) != 1) || (artifactMode && len(flags.Args()) != 2) {
		log.Print(usage)
		return 1, nil
	}

	if *target == "" {
		log.Printf("A non-empty --target must be specified")
		return 1, nil
	}
	if artifactMode && (*blobType != "" || *outputFile != "") {
		log.Printf("--type and --output_file are not supported with 'download artifacts'")
		return 1, nil
	}
	if *outputDirectory != "" && *blobType == "" {
		*blobType = "Directory"
	}
	if *outputDirectory != "" && *blobType != "Directory" {
		log.Printf("blob type %q is not compatible with output_directory option", *blobType)
		return 1, nil
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	} else if apiKey, err := login.GetAPIKey(); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}
	if len(*remoteHeader) > 0 {
		md, err := mdutil.Parse(*remoteHeader...)
		if err != nil {
			return -1, fmt.Errorf("parse remote headers: %w", err)
		}
		for key, values := range md {
			ctx = metadata.AppendToOutgoingContext(ctx, key, values[len(values)-1])
		}
	}
	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return -1, fmt.Errorf("dial %q: %w", *target, err)
	}
	if artifactMode {
		invocationID, ok := uuid.ParseInvocationID(flags.Args()[1])
		if !ok {
			return -1, fmt.Errorf("%q is not an invocation ID or invocation URL", flags.Args()[1])
		}
		outputDir := *outputDirectory
		if outputDir == "" {
			outputDir, err = os.MkdirTemp("", "buildbuddy-artifacts-*")
			if err != nil {
				return -1, fmt.Errorf("create temporary artifact directory: %w", err)
			}
			log.Printf("%sDownloading artifacts to %s%s", terminal.Esc(90), outputDir, terminal.Esc())
		}
		if err := downloadArtifacts(ctx, apipb.NewApiServiceClient(conn), invocationID, outputDir); err != nil {
			log.Print(err)
			return 1, nil
		}
		return 0, nil
	}

	uri := flags.Args()[0]
	if !strings.HasPrefix(uri, "/blobs") {
		// Interpret HASH/SIZE as a resource name using the default digest
		// function (SHA256).
		uri = "/blobs/" + uri
	}
	rn, err := digest.ParseDownloadResourceName(uri)
	if err != nil {
		return -1, fmt.Errorf("parse resource name: %w", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)
	casClient := repb.NewContentAddressableStorageClient(conn)
	capsClient := repb.NewCapabilitiesClient(conn)

	if err := downloadFile(ctx, rn, bsClient); err != nil {
		log.Print(err)
		return 1, nil
	}

	if *outputDirectory != "" {
		log.Printf("Downloading directory contents to %q", *outputDirectory)
		inputTree, err := cachetools.GetAndMaybeCacheTreeFromRootDirectoryDigest(
			ctx, casClient, rn, nil, bsClient)
		if err != nil {
			return -1, fmt.Errorf("get tree: %w", err)
		}
		env := real_environment.NewBatchEnv()
		// TODO: remove env dependency from DownloadTree
		env.SetContentAddressableStorageClient(casClient)
		env.SetByteStreamClient(bsClient)
		env.SetCapabilitiesClient(capsClient)
		start := time.Now()
		if txInfo, err := dirtools.DownloadTree(ctx, env, rn.GetInstanceName(), rn.GetDigestFunction(), inputTree, &dirtools.DownloadTreeOpts{RootDir: *outputDirectory}); err != nil {
			return -1, fmt.Errorf("download directory tree to %q: %w", *outputDirectory, err)
		} else {
			log.Printf("Downloaded %d files (%s) in %s", txInfo.FileCount, units.HumanSize(float64(txInfo.BytesTransferred)), time.Since(start))
		}
	}

	return 0, nil
}
