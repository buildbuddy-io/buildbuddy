package upload

import (
	"context"
	"errors"
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
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	flags = flag.NewFlagSet("upload", flag.ContinueOnError)

	target             = flags.String("target", "grpcs://remote.buildbuddy.io", "Cache gRPC target")
	remoteInstanceName = flags.String("remote_instance_name", "", "Remote instance name")
	compress           = flags.Bool("compress", true, "If true, enable compression of uploads to remote caches")
	stdin              = flags.Bool("stdin", false, "If true, read from stdin")
	digestFunction     = flags.String("digest_function", "SHA256", "If set, use this digest function for uploads.")

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
`
)

func parseDigestFuncString() repb.DigestFunction_Value {
	if df, ok := repb.DigestFunction_Value_value[strings.ToUpper(*digestFunction)]; ok {
		return repb.DigestFunction_Value(df)
	}
	return repb.DigestFunction_UNKNOWN
}

func uploadFile(args []string) error {
	var inputFile string
	if len(args) == 0 && *stdin {
		// If input is coming from stdin; stream it to a file first
		// because we need to compute the digest before uploading.
		f, err := os.CreateTemp("/tmp/", "bb-upload-*.tmp")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())

		if _, err := io.Copy(f, os.Stdin); err != nil {
			return err
		}

		inputFile = f.Name()
		if err := f.Close(); err != nil {
			return err
		}
	} else if len(args) == 1 {
		// If input is a file, just use it.
		inputFile = args[0]
	} else {
		return errors.New(usage)
	}

	f, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()
	if apiKey, err := storage.ReadRepoConfig("api-key"); err == nil {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", strings.TrimSpace(apiKey))
	}

	conn, err := grpc_client.DialTarget(*target)
	if err != nil {
		return err
	}
	bsClient := bspb.NewByteStreamClient(conn)
	digestFunction := parseDigestFuncString()

	d, err := digest.Compute(f, digestFunction)
	if err != nil {
		return err
	}
	ind := digest.NewResourceName(d, *remoteInstanceName, rspb.CacheType_CAS, digestFunction)
	if *compress {
		ind.SetCompressor(repb.Compressor_ZSTD)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	_, err = cachetools.UploadFromReader(ctx, bsClient, ind, f)
	if err != nil {
		return err
	}

	ds, err := ind.DownloadString()
	if err != nil {
		return err
	}
	log.Print(ds)
	return nil
}

func HandleUpload(args []string) (int, error) {
	cmd, idx := arg.GetCommandAndIndex(args)
	if cmd != flags.Name() {
		return -1, nil
	}
	if err := arg.ParseFlagSet(flags, args[idx+1:]); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
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
