package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const usage = `splice_test tests how well a remote cache server performs 
with CDC chunking by uploading file using ByteStream/Read, querying chunk 
boundaries via CAS/SplitBlob, and measuring deduplication savings. 
The script calls ByteStream/Read to ensure the file is actually stored 
in the remote cache.`

type fileInfo struct {
	path string
	size int64
}

type chunkStats struct {
	digest *repb.Digest
	files  []string
	size   int64
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("splice_test", flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "%s\n\n", usage)
		flags.PrintDefaults()
	}
	target := flags.String("target", "localhost:1985", "gRPC target address")
	directory := flags.String("directory", "", "Directory to walk")
	instanceName := flags.String("instance", "test-instance", "Remote instance name")

	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" {
		return errors.New("directory must be specified")
	}
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	casClient := repb.NewContentAddressableStorageClient(conn)
	bsClient := bspb.NewByteStreamClient(conn)

	files := findFiles(*directory)
	if len(files) == 0 {
		return fmt.Errorf("no files found in %s", *directory)
	}

	fmt.Fprintf(output, "Processing %d files from %s...\n", len(files), *directory)

	allChunks := make(map[string]*chunkStats)
	totalChunks := 0
	processedFiles := 0
	chunkedFiles := 0
	var totalOriginalSize int64

	for i, file := range files {
		if (i+1)%100 == 0 {
			fmt.Fprintf(output, "  Processed %d/%d files...\n", i+1, len(files))
		}

		fileSize, chunks, isChunked, err := processFile(ctx, casClient, bsClient, file.path, *instanceName)
		if err != nil {
			log.Printf("Error processing %s: %v", file.path, err)
			continue
		}

		processedFiles++
		totalChunks += len(chunks)
		totalOriginalSize += fileSize
		if isChunked {
			chunkedFiles++
		}

		fileName := filepath.Base(file.path)
		for _, chunk := range chunks {
			if stats, exists := allChunks[chunk.Hash]; exists {
				stats.files = append(stats.files, fileName)
			} else {
				allChunks[chunk.Hash] = &chunkStats{
					digest: chunk,
					files:  []string{fileName},
					size:   chunk.SizeBytes,
				}
			}
		}
	}

	fmt.Fprintf(output, "Files processed: %d\n", processedFiles)
	fmt.Fprintf(output, "Chunked files: %d\n", chunkedFiles)
	fmt.Fprintf(output, "Non-chunked files: %d\n", processedFiles-chunkedFiles)
	fmt.Fprintf(output, "Total chunks: %d\n", totalChunks)
	if chunkedFiles > 0 {
		fmt.Fprintf(output, "Average chunks per chunked file: %.1f\n", float64(totalChunks-processedFiles+chunkedFiles)/float64(chunkedFiles))
	}
	fmt.Fprintln(output)

	printChunkStats(output, allChunks, totalOriginalSize, chunkedFiles, processedFiles-chunkedFiles)
	return nil
}

func findFiles(dir string) []fileInfo {
	var files []fileInfo
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			files = append(files, fileInfo{path: path, size: info.Size()})
		}
		return nil
	})
	return files
}

func processFile(ctx context.Context, casClient repb.ContentAddressableStorageClient, bsClient bspb.ByteStreamClient, filePath, instanceName string) (int64, []*repb.Digest, bool, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, nil, false, err
	}

	fileDigest, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_SHA256)
	if err != nil {
		return 0, nil, false, err
	}

	rn := digest.NewCASResourceName(fileDigest, instanceName, repb.DigestFunction_SHA256)
	rn.SetCompressor(repb.Compressor_ZSTD)

	if _, _, err := cachetools.UploadFromReader(ctx, bsClient, rn, bytes.NewReader(data)); err != nil {
		return 0, nil, false, fmt.Errorf("upload: %w", err)
	}

	var chunks []*repb.Digest
	var isChunked bool
	splitResp, err := casClient.SplitBlob(ctx, &repb.SplitBlobRequest{
		InstanceName:   instanceName,
		BlobDigest:     fileDigest,
		DigestFunction: repb.DigestFunction_SHA256,
	})
	if err != nil {
		if !status.IsNotFoundError(err) {
			return 0, nil, false, fmt.Errorf("split: %w", err)
		}
		chunks = []*repb.Digest{fileDigest}
		isChunked = false
	} else {
		chunks = splitResp.ChunkDigests
		isChunked = true
	}

	var buf bytes.Buffer
	if err := cachetools.GetBlob(ctx, bsClient, rn, &buf); err != nil {
		return 0, nil, false, fmt.Errorf("download: %w", err)
	}

	downloadedDigest, err := digest.Compute(bytes.NewReader(buf.Bytes()), repb.DigestFunction_SHA256)
	if err != nil {
		return 0, nil, false, err
	}

	if downloadedDigest.Hash != fileDigest.Hash || downloadedDigest.SizeBytes != fileDigest.SizeBytes {
		return 0, nil, false, fmt.Errorf("digest mismatch: expected %s/%d, got %s/%d",
			fileDigest.Hash, fileDigest.SizeBytes, downloadedDigest.Hash, downloadedDigest.SizeBytes)
	}

	return int64(len(data)), chunks, isChunked, nil
}

func printChunkStats(output io.Writer, allChunks map[string]*chunkStats, totalOriginalSize int64, chunkedFiles, nonChunkedFiles int) {
	uniqueChunks := len(allChunks)
	sharedChunks := 0
	var uniqueChunkBytes int64

	for _, stats := range allChunks {
		uniqueChunkBytes += stats.size
		if len(stats.files) > 1 {
			sharedChunks++
		}
	}

	dedupSavings := totalOriginalSize - uniqueChunkBytes

	fmt.Fprintln(output, "=== Storage Efficiency ===")
	fmt.Fprintf(output, "Original size: %.2f MB\n", float64(totalOriginalSize)/(1024*1024))
	fmt.Fprintf(output, "Deduplicated size: %.2f MB\n", float64(uniqueChunkBytes)/(1024*1024))
	fmt.Fprintln(output)
	fmt.Fprintf(output, "Chunked files: %d (server-chunked)\n", chunkedFiles)
	fmt.Fprintf(output, "Non-chunked files: %d (stored whole)\n", nonChunkedFiles)
	fmt.Fprintf(output, "Unique chunks: %d\n", uniqueChunks)
	fmt.Fprintf(output, "Shared chunks: %d (%.1f%%)\n",
		sharedChunks, float64(sharedChunks)/float64(uniqueChunks)*100)
	fmt.Fprintln(output)
	fmt.Fprintf(output, "Dedup savings: %.2f MB (%.1f%%)\n",
		float64(dedupSavings)/(1024*1024),
		float64(dedupSavings)/float64(totalOriginalSize)*100)
}
