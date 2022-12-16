package bytestream

import (
	"context"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/ziputil"
	"google.golang.org/protobuf/proto"

	arpb "github.com/buildbuddy-io/buildbuddy/proto/archive"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func FetchBytestreamZipManifest(ctx context.Context, env environment.Env, url *url.URL) (*arpb.ArchiveManifest, error) {
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

	reader, writer := io.Pipe()
	go func() {
		err = StreamBytestreamFileChunk(ctx, env, url, offset, r.GetDigest().GetSizeBytes()-offset, func(data []byte) {
			writer.Write(data)
		})
		writer.CloseWithError(err)
	}()

	// Dump the full contents out into a buffer (should be 64K or less).
	footer, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Find and parse the End of Central Directory header or fail.
	eocd, err := ziputil.ReadDirectoryEnd(footer, r.GetDigest().GetSizeBytes())
	if err != nil {
		return nil, err
	}

	cdStart := eocd.DirectoryOffset - offset
	cdEnd := cdStart + eocd.DirectorySize

	out := &arpb.ArchiveManifest{}
	entries, err := ziputil.ReadDirectoryHeader(footer[cdStart:cdEnd], eocd)
	out.Entry = entries
	return out, nil
}

func validateLocalFileHeader(ctx context.Context, env environment.Env, url *url.URL, entry *arpb.ManifestEntry) (int, error) {
	reader, writer := io.Pipe()
	go func() {
		err := StreamBytestreamFileChunk(ctx, env, url, entry.GetHeaderOffset(), ziputil.FileHeaderLen, func(data []byte) {
			writer.Write(data)
		})
		writer.CloseWithError(err)
	}()
	header := make([]byte, ziputil.FileHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return -1, err
	}
	return ziputil.ValidateLocalFileHeader(header, entry)
}

func StreamSingleFileFromBytestreamZip(ctx context.Context, env environment.Env, url *url.URL, entry *arpb.ManifestEntry, out io.Writer) error {
	dynamicHeaderBytes, err := validateLocalFileHeader(ctx, env, url, entry)
	if err != nil {
		return err
	}

	// Stream the dynamic portion of the header and the compressed file as one read.
	reader, writer := io.Pipe()
	go func() {
		err := StreamBytestreamFileChunk(ctx, env, url, entry.GetHeaderOffset()+ziputil.FileHeaderLen, entry.GetCompressedSize()+int64(dynamicHeaderBytes), func(data []byte) {
			writer.Write(data)
		})
		writer.CloseWithError(err)
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

func StreamBytestreamFile(ctx context.Context, env environment.Env, url *url.URL, callback func([]byte)) error {
	return StreamBytestreamFileChunk(ctx, env, url, 0, 0, callback)
}

func StreamBytestreamFileChunk(ctx context.Context, env environment.Env, url *url.URL, offset int64, limit int64, callback func([]byte)) error {
	if url.Scheme != "bytestream" && url.Scheme != "actioncache" {
		return status.InvalidArgumentErrorf("Only bytestream:// uris are supported")
	}

	err := error(nil)

	// If we have a cache enabled, try connecting to that first
	if env.GetCache() != nil {
		localURL, _ := url.Parse(url.String())
		grpcPort := "1985"
		if p, err := flagutil.GetDereferencedValue[int]("grpc_port"); err == nil {
			grpcPort = strconv.Itoa(p)
		}
		localURL.Host = "localhost:" + grpcPort
		err = streamFromUrl(ctx, localURL, false, offset, limit, callback)
	}

	// If that fails, try to connect over grpcs
	if err != nil || env.GetCache() == nil {
		err = streamFromUrl(ctx, url, true, offset, limit, callback)
	}

	// If that fails, try grpc
	if err != nil {
		err = streamFromUrl(ctx, url, false, offset, limit, callback)
	}

	return err
}

func streamFromUrl(ctx context.Context, url *url.URL, grpcs bool, offset int64, limit int64, callback func([]byte)) error {
	if url.Port() == "" && grpcs {
		url.Host = url.Hostname() + ":443"
	} else if url.Port() == "" {
		url.Host = url.Hostname() + ":80"
	}

	conn, err := grpc_client.DialTargetWithOptions(url.String(), grpcs)
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
		callback(buf)
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
		callback(rsp.Data)
	}
	return nil
}
