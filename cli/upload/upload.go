package upload

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	flags = flag.NewFlagSet("upload", flag.ContinueOnError)

	target             = flags.String("target", login.DefaultApiTarget, "Cache gRPC target")
	remoteInstanceName = flags.String("remote_instance_name", "", "Remote instance name")
	stdin              = flags.Bool("stdin", false, "If true, read from stdin")
	digestFlag         = flags.String("digest", "", "If set, use this precomputed digest/resource name and skip digest computation locally; the server still verifies content matches the digest.")
	forceTracing       = flags.Bool("trace", true, "If true, force tracing")
	apiKey             = flags.String("api_key", "", "Optionally override the API key with this value")

	// TODO: make it an error to specify either of these flags if the compressor or digest function is already specified in the --digest (as a full resource name).
	compress       = flags.Bool("compress", true, "If true, enable compression of uploads to remote caches")
	digestFunction = flags.String("digest_function", "SHA256", "If set, use this digest function for local digest computation. Ignored when --digest is set.")

	usage = `
usage: bb ` + flags.Name() + ` filename

Uploads the file specified by filename to the CAS and outputs the digest.

If an input file is specified, that file will be uploaded. To upload from
stdin, set the --stdin flag.

Example of uploading a blob from stdin:
  $ echo "buildbuddy" | bb upload --stdin

Example of uploading a file with a remote instance name:
  $ echo -n "buildbuddy" > input_file.txt
  $ bb upload input_file.txt --remote_instance_name=foo

Example of uploading from stdin with a precomputed digest:
  $ echo "buildbuddy" | bb upload --stdin --digest=<hash>/<size>

Example of specifying digest function in the resource name:
  $ bb upload input_file.txt --digest=/blobs/sha256/<hash>/<size>
`
)

type noSeekReader struct {
	io.Reader
}

func stdinIsPipe() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeNamedPipe) != 0
}

func parseDigestOrResourceName(s string) (*digest.CASResourceName, error) {
	if s == "" {
		return nil, nil
	}
	uri := s
	if !strings.Contains(uri, "/blobs/") && !strings.Contains(uri, "/compressed-blobs/") {
		// Interpret HASH/SIZE as a resource name using the default digest
		// function (SHA256).
		uri = "/blobs/" + uri
	}
	rn, err := digest.ParseDownloadResourceName(uri)
	if err != nil {
		return nil, fmt.Errorf("parse resource name: %w", err)
	}
	return rn, nil
}

func uploadFile(args []string) error {
	providedRN, err := parseDigestOrResourceName(*digestFlag)
	if err != nil {
		return fmt.Errorf("parse digest: %w", err)
	}

	var inputReader io.Reader
	var inputFile *os.File
	if len(args) == 0 && *stdin {
		if providedRN == nil {
			// If input is coming from stdin without a precomputed digest, stream it
			// to a temp file first so we can compute a digest and then upload it.
			f, err := os.CreateTemp("", "bb-upload-*.tmp")
			if err != nil {
				return fmt.Errorf("create temp file: %w", err)
			}
			defer os.Remove(f.Name())
			if _, err := io.Copy(f, os.Stdin); err != nil {
				f.Close()
				return fmt.Errorf("copy stdin: %w", err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				f.Close()
				return fmt.Errorf("rewind temp file: %w", err)
			}
			inputReader = f
			inputFile = f
		} else {
			// cachetools.UploadFromReader retries only for io.Seeker inputs.
			// Wrap piped stdin so it is treated as a streaming reader.
			if stdinIsPipe() {
				inputReader = noSeekReader{Reader: os.Stdin}
			} else {
				inputReader = os.Stdin
			}
		}
	} else if len(args) == 1 {
		// If input is a file, just use it.
		f, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("open input file: %w", err)
		}
		inputReader = f
		inputFile = f
	} else {
		return errors.New(usage)
	}
	if inputFile != nil {
		defer inputFile.Close()
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	} else if apiKey, err := login.GetAPIKey(); err == nil && apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}
	if *forceTracing {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-trace", "force")
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return fmt.Errorf("dial target: %w", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)

	ind := (*digest.CASResourceName)(nil)
	if providedRN != nil {
		instanceName := providedRN.GetInstanceName()
		if instanceName == "" {
			instanceName = *remoteInstanceName
		}
		ind = digest.NewCASResourceName(providedRN.GetDigest(), instanceName, providedRN.GetDigestFunction())
	} else {
		digestFunction, err := digest.ParseFunction(*digestFunction)
		if err != nil {
			return fmt.Errorf("parse digest function: %w", err)
		}
		var d *repb.Digest
		d, err = digest.Compute(inputReader, digestFunction)
		if err != nil {
			return fmt.Errorf("compute digest: %w", err)
		}
		seekableReader, ok := inputReader.(io.Seeker)
		if !ok {
			return fmt.Errorf("validate input stream: %w", errors.New("input stream is not seekable; provide --digest for streaming uploads"))
		}
		if _, err := seekableReader.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewind input: %w", err)
		}
		ind = digest.NewCASResourceName(d, *remoteInstanceName, digestFunction)
	}
	if *compress {
		ind.SetCompressor(repb.Compressor_ZSTD)
	}
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, ind, inputReader)
	if err != nil {
		return fmt.Errorf("upload blob: %w", err)
	}

	log.Print(ind.DownloadString())
	return nil
}

func HandleUpload(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	if *target == "" {
		log.Printf("A non-empty --target must be specified")
		return 1, nil
	}

	if err := uploadFile(flags.Args()); err != nil {
		log.Print(err)
		return 1, nil
	}
	return 0, nil
}
