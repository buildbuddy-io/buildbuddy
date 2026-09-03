// oci_to_ext4 converts an OCI container image to a chunked ext4 root
// filesystem image. The app runs it as a remote action so that each image is
// converted once for the whole fleet, and executors use the action result as a
// Firecracker containerfs snapshot, fetching chunks from CAS on demand.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	image     = flag.String("image", "", "OCI image reference to convert")
	manifest  = flag.String("manifest", "", "Path to write the image manifest, a serialized firecracker.ChunkedFile proto")
	chunksDir = flag.String("chunks_dir", "", "Directory to write image chunks into, named by byte offset")
	chunkSize = flag.Int64("chunk_size", 1000*4096, "Chunk size in bytes, which must be a multiple of the executor page size")
)

func main() {
	flag.Parse()
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "oci_to_ext4: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if *image == "" || *manifest == "" || *chunksDir == "" {
		return fmt.Errorf("--image, --manifest, and --chunks_dir are required")
	}
	resolver, err := oci.NewResolver(real_environment.NewBatchEnv())
	if err != nil {
		return fmt.Errorf("create OCI resolver: %w", err)
	}
	creds := oci.Credentials{
		Username: os.Getenv("BUILDBUDDY_OCI_USERNAME"),
		Password: os.Getenv("BUILDBUDDY_OCI_PASSWORD"),
	}
	ext4Path, err := ociconv.ConvertContainerToExt4FS(ctx, resolver, ".", *image, creds, false /*=useOCIFetcher*/)
	if err != nil {
		return fmt.Errorf("convert image: %w", err)
	}
	defer os.Remove(ext4Path)
	rootfs, err := chunkFile(ext4Path, *chunksDir, *chunkSize)
	if err != nil {
		return fmt.Errorf("chunk image: %w", err)
	}
	b, err := proto.Marshal(rootfs)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := os.WriteFile(*manifest, b, 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// chunkFile splits the file at path into chunk files under dir, named by byte
// offset, and returns the manifest describing them. Chunks that are entirely
// zero are omitted, since executors treat a missing chunk as a hole. Chunk
// digests are BLAKE3, matching the digest function of the conversion action so
// that they name the chunk files the executor uploads to CAS.
func chunkFile(path, dir string, chunkSize int64) (*fcpb.ChunkedFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	rootfs := &fcpb.ChunkedFile{
		Name:      "containerfs.ext4",
		Size:      info.Size(),
		ChunkSize: chunkSize,
	}
	buf := make([]byte, chunkSize)
	zeros := make([]byte, chunkSize)
	for offset := int64(0); offset < info.Size(); offset += chunkSize {
		chunk := buf[:min(chunkSize, info.Size()-offset)]
		if _, err := io.ReadFull(f, chunk); err != nil {
			return nil, fmt.Errorf("read chunk at offset %d: %w", offset, err)
		}
		if bytes.Equal(chunk, zeros[:len(chunk)]) {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, strconv.FormatInt(offset, 10)), chunk, 0644); err != nil {
			return nil, err
		}
		d, err := digest.Compute(bytes.NewReader(chunk), repb.DigestFunction_BLAKE3)
		if err != nil {
			return nil, err
		}
		rootfs.Chunks = append(rootfs.Chunks, &fcpb.Chunk{Offset: offset, Digest: d})
	}
	return rootfs, nil
}
