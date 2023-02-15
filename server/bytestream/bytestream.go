package bytestream

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/ziputil"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	zipb "github.com/buildbuddy-io/buildbuddy/proto/zip"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func FetchBytestreamZipManifest(ctx context.Context, env environment.Env, url *url.URL) (*zipb.Manifest, error) {
	r, err := digest.ParseDownloadResourceName(strings.TrimPrefix(url.RequestURI(), "/"))
	if err != nil {
		return nil, err
	}
	// Let's just read 64K and see if we can find the central directory in there.
	// We probably don't want to be in the business of rendering 3000-file zips'
	// contents anyway.
	offset := r.GetDigest().GetSizeBytes() - 65536
	if offset < 0 {
		offset = 0
	}

	var buf bytes.Buffer
	err = StreamBytestreamFileChunk(ctx, env, url, offset, r.GetDigest().GetSizeBytes()-offset, &buf)
	if err != nil {
		return nil, err
	}

	// We dump the full contents out into a buffer, but that should be 64K or less.
	return parseZipManifestFooter(buf.Bytes(), offset, r.GetDigest().GetSizeBytes())
}

func parseZipManifestFooter(footer []byte, offset int64, trueFileSize int64) (*zipb.Manifest, error) {
	eocd, err := ziputil.ReadDirectoryEnd(footer, trueFileSize)
	if err != nil {
		return nil, err
	}

	cdStart := eocd.DirectoryOffset - offset
	cdEnd := cdStart + eocd.DirectorySize

	if cdStart < 0 {
		return nil, status.UnimplementedError("directory size is very large.")
	}

	entries, err := ziputil.ReadDirectoryHeader(footer[cdStart:cdEnd], eocd)
	if err != nil {
		return nil, err
	}

	return &zipb.Manifest{Entry: entries}, nil
}

// Just a little song and dance so that we can mock out streamingin tests.
type Bytestreamer func(ctx context.Context, env environment.Env, url *url.URL, offset int64, limit int64, writer io.Writer) error

func validateLocalFileHeader(ctx context.Context, env environment.Env, url *url.URL, entry *zipb.ManifestEntry, streamer Bytestreamer) (int, error) {
	var buf bytes.Buffer
	err := streamer(ctx, env, url, entry.GetHeaderOffset(), ziputil.FileHeaderLen, &buf)
	if err != nil {
		return -1, err
	}
	return ziputil.ValidateLocalFileHeader(buf.Bytes(), entry)
}

func StreamSingleFileFromBytestreamZip(ctx context.Context, env environment.Env, url *url.URL, entry *zipb.ManifestEntry, out io.Writer) error {
	return streamSingleFileFromBytestreamZipInternal(ctx, env, url, entry, out, StreamBytestreamFileChunk)
}

func streamSingleFileFromBytestreamZipInternal(ctx context.Context, env environment.Env, url *url.URL, entry *zipb.ManifestEntry, out io.Writer, streamer Bytestreamer) error {
	dynamicHeaderBytes, err := validateLocalFileHeader(ctx, env, url, entry, streamer)
	if err != nil {
		return err
	}

	// Stream the dynamic portion of the header and the compressed file as one read.
	reader, writer := io.Pipe()
	defer reader.Close()
	go func() {
		err := streamer(ctx, env, url, entry.GetHeaderOffset()+ziputil.FileHeaderLen, entry.GetCompressedSize()+int64(dynamicHeaderBytes), writer)
		// StreamBytestreamFileChunk shouldn't return EOF, but let's just be safe.
		if err != nil && err != io.EOF {
			writer.CloseWithError(err)
		}
	}()

	// Validate that the dynamic portion of the file header makes sense.
	extras := make([]byte, dynamicHeaderBytes)
	if _, err := io.ReadFull(reader, extras); err != nil {
		return err
	}
	if err := ziputil.ValidateLocalFileNameAndExtras(extras, entry); err != nil {
		return err
	}

	// And, finally, actually decompress.
	if err := ziputil.DecompressAndStream(out, reader, entry); err != nil {
		return err
	}

	return nil
}

func StreamBytestreamFile(ctx context.Context, env environment.Env, url *url.URL, writer io.Writer) error {
	return StreamBytestreamFileChunk(ctx, env, url, 0, 0, writer)
}

func StreamBytestreamFileChunk(ctx context.Context, env environment.Env, url *url.URL, offset int64, limit int64, writer io.Writer) error {
	if url.Scheme != "bytestream" && url.Scheme != "actioncache" {
		return status.InvalidArgumentErrorf("Only bytestream:// uris are supported")
	}

	var err error

	// If we have a cache enabled, try connecting to that first
	if env.GetCache() != nil {
		localURL, _ := url.Parse(url.String())
		grpcPort := "1985"
		if p, err := flagutil.GetDereferencedValue[int]("grpc_port"); err == nil {
			grpcPort = strconv.Itoa(p)
		}
		localURL.Host = "localhost:" + grpcPort
		err = streamFromUrl(ctx, localURL, false, offset, limit, writer)
	}

	// If the local cache did not work, maybe a remote cache is being used.
	// Try to connect to that, first over grpcs.
	if err != nil || env.GetCache() == nil {
		err = streamFromUrl(ctx, url, true, offset, limit, writer)
	}

	// If that didn't work, try plain old grpc.
	if err != nil {
		err = streamFromUrl(ctx, url, false, offset, limit, writer)
	}

	// Sanitize the error so as to not expose internal services via the
	// error message.
	if err != nil {
		log.Errorf("Error byte-streaming from URL %q: %s", url.String(), err)
		return status.UnavailableErrorf("Error reading from URL: %q", url.String())
	}
	return nil
}

func grpcTargetForFileURL(u *url.URL, grpcs bool) string {
	target := url.URL{Scheme: "grpc", User: u.User, Host: u.Host}
	if grpcs {
		target.Scheme = "grpcs"
	}
	return target.String()
}

func streamFromUrl(ctx context.Context, url *url.URL, grpcs bool, offset int64, limit int64, writer io.Writer) error {
	if url.Port() == "" && grpcs {
		url.Host = url.Hostname() + ":443"
	} else if url.Port() == "" {
		url.Host = url.Hostname() + ":80"
	}

	conn, err := grpc_client.DialTarget(grpcTargetForFileURL(url, grpcs))
	if err != nil {
		return err
	}
	defer conn.Close()

	if url.Scheme == "actioncache" {
		acClient := repb.NewActionCacheClient(conn)
		r, err := digest.ParseActionCacheResourceName(strings.TrimPrefix(url.RequestURI(), "/"))
		if err != nil {
			return err
		}

		// Request the ActionResult
		req := &repb.GetActionResultRequest{
			InstanceName: r.GetInstanceName(),
			ActionDigest: r.GetDigest(),
		}
		actionResult, err := acClient.GetActionResult(ctx, req)
		if err != nil {
			return err
		}

		// Turn ActionResult into []byte
		buf, err := proto.Marshal(actionResult)
		if err != nil {
			return err
		}
		if _, err := writer.Write(buf); err != nil {
			return err
		}
		return nil
	}
	client := bspb.NewByteStreamClient(conn)

	// Request the file bytestream
	req := &bspb.ReadRequest{
		ResourceName: strings.TrimPrefix(url.RequestURI(), "/"), // trim leading "/"
		ReadOffset:   offset,                                    // started from the bottom now we here
		ReadLimit:    limit,                                     // no limit
	}
	readClient, err := client.Read(ctx, req)
	if err != nil {
		return err
	}

	for {
		rsp, err := readClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := writer.Write(rsp.Data); err != nil {
			return err
		}
	}
	return nil
}
