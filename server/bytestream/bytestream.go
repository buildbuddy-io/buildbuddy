package bytestream

import (
	"compress/flate"
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
	"google.golang.org/protobuf/proto"

	arpb "github.com/buildbuddy-io/buildbuddy/proto/archive"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func FetchBytestreamZipManifest(ctx context.Context, env environment.Env, url *url.URL) (*arpb.ArchiveManifest, error) {
	// XXX: Validate.
	r, err := digest.ParseDownloadResourceName(strings.TrimPrefix(url.RequestURI(), "/"))
	if err != nil {
		return nil, err
	}
	// Pull 64K, read last 1K for directory, then try to read full manifest.
	// If that's not good enough, tough.
	// Manifest output: Header Location, File start, Name, CRC, Compressed size,
	// Uncompressed size, Direct download support, TOTAL # OF FILES (for summary)?
	offset := r.GetDigest().GetSizeBytes() - 65536
	if offset < 0 {
		offset = 0
	}

	reader, writer := io.Pipe()
	// XXX: Take subset down to file size.
	go func() {
		err = StreamBytestreamFile(ctx, env, url, 0, func(data []byte) {
			writer.Write(data)
		})
		writer.CloseWithError(err)
	}()

	if err != nil {
		return nil, err
	}

	// XXX: ReadFull instead.
	footer, err := io.ReadAll(reader)
	// XXX: need to check length..
	if err != nil {
		return nil, err
	}

	// Find and parse the End of Central Directory header or fail.
	eocd, _, err := readDirectoryEnd(footer, r.GetDigest().GetSizeBytes())
	if err != nil {
		return nil, err
	}

	// XXX:
	cdStart := eocd.directoryOffset
	cdEnd := eocd.directoryOffset + eocd.directorySize

	out := &arpb.ArchiveManifest{}
	entries, err := readDirectoryHeader(footer[cdStart:cdEnd], eocd)
	out.Entry = entries
	return out, nil
}

func StreamSingleFileFromBytestreamZip(ctx context.Context, env environment.Env, url *url.URL, entry *arpb.ManifestEntry, out io.Writer) error {
	reader, writer := io.Pipe()
	go func() {
		err := StreamBytestreamFile(ctx, env, url, entry.GetHeaderOffset(), func(data []byte) {
			writer.Write(data)
		})
		writer.CloseWithError(err)
	}()

	// XXX: Parse header and compare,
	// XXX: Stream remaining file into CRC32 and out.
	header := make([]byte, fileHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	buf := readBuf(header[:])
	if sig := buf.uint32(); sig != fileHeaderSignature {
		return ErrFormat
	}

	buf = buf[10:] // Skip junk we don't care about.
	// XXX: Check compression.

	crc32 := buf.uint32()
	compsize := int64(buf.uint32())
	uncompsize := int64(buf.uint32())
	if entry.GetCrc32() != crc32 || entry.GetCompressedSize() != compsize || entry.GetUncompressedSize() != uncompsize {
		return ErrFormat
	}

	filenameLen := int(buf.uint16())
	extraLen := int(buf.uint16())

	if len(entry.GetName()) != filenameLen {
		return ErrFormat
	}

	names := make([]byte, filenameLen+extraLen)
	if _, err := io.ReadFull(reader, names); err != nil {
		return err
	}

	if string(names[:filenameLen]) != entry.GetName() {
		return ErrFormat
	}

	var outReader io.Reader
	// XXX: Stream!
	if entry.GetCompression() == arpb.ManifestEntry_COMPRESSION_TYPE_FLATE {
		outReader = flate.NewReader(io.LimitReader(reader, int64(entry.GetCompressedSize())))
	} else if entry.GetCompression() == arpb.ManifestEntry_COMPRESSION_TYPE_NONE {
		outReader = io.LimitReader(reader, int64(entry.GetCompressedSize()))
	} else {
		return ErrAlgorithm
	}

	// XXX: Buffer?
	if _, err := io.Copy(out, outReader); err != nil {
		return err
	}

	// XXX: Is this right?
	// XXX: Need to close the flate reader.
	reader.Close()
	writer.Close()

	return nil
}

func StreamBytestreamFile(ctx context.Context, env environment.Env, url *url.URL, offset int64, callback func([]byte)) error {
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
		err = streamFromUrl(ctx, localURL, false, offset, callback)
	}

	// If that fails, try to connect over grpcs
	if err != nil || env.GetCache() == nil {
		err = streamFromUrl(ctx, url, true, offset, callback)
	}

	// If that fails, try grpc
	if err != nil {
		err = streamFromUrl(ctx, url, false, offset, callback)
	}

	return err
}

func streamFromUrl(ctx context.Context, url *url.URL, grpcs bool, offset int64, callback func([]byte)) error {
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
		ReadLimit:    0,                                         // no limit
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
