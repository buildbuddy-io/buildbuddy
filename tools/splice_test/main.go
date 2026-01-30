package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const usage = `splice_test tests how well a remote cache server performs
with CDC chunking by uploading files using ByteStream/Write, querying chunk
boundaries via CAS/SplitBlob, and measuring deduplication savings.
After upload, the tool verifies integrity by reading files back via
ByteStream/Read and comparing digests.`

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
	file := flags.String("file", "", "Single file to test")
	instanceName := flags.String("instance", "test-instance", "Remote instance name")
	useTLS := flags.Bool("tls", false, "Use TLS for gRPC connection")
	remoteHeader := flags.String("remote_header", "", "Header to send with requests (format: key=value)")
	verbose := flags.Bool("v", false, "Verbose output with RPC timing")

	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" && *file == "" {
		return errors.New("either -directory or -file must be specified")
	}
	if *directory != "" && *file != "" {
		return errors.New("cannot specify both -directory and -file")
	}
	if *remoteHeader != "" {
		parts := strings.SplitN(*remoteHeader, "=", 2)
		if len(parts) != 2 {
			return errors.New("header must be in format key=value")
		}
		ctx = metadata.AppendToOutgoingContext(ctx, parts[0], parts[1])
	}
	var creds credentials.TransportCredentials
	if *useTLS {
		creds = credentials.NewTLS(&tls.Config{})
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer conn.Close()

	casClient := repb.NewContentAddressableStorageClient(conn)
	bsClient := bspb.NewByteStreamClient(conn)

	var files []fileInfo
	var source string
	if *file != "" {
		info, err := os.Stat(*file)
		if err != nil {
			return fmt.Errorf("cannot stat file %s: %w", *file, err)
		}
		if info.IsDir() {
			return fmt.Errorf("%s is a directory, use -directory instead", *file)
		}
		files = []fileInfo{{path: *file, size: info.Size()}}
		source = *file
	} else {
		files = findFiles(*directory)
		source = *directory
	}
	if len(files) == 0 {
		return fmt.Errorf("no files found in %s", source)
	}

	fmt.Fprintf(output, "Processing %d files from %s...\n", len(files), source)

	allChunks := make(map[string]*chunkStats)
	totalChunks := 0
	processedFiles := 0
	chunkedFiles := 0
	var totalOriginalSize int64

	for i, file := range files {
		if (i+1)%100 == 0 {
			fmt.Fprintf(output, "  Processed %d/%d files...\n", i+1, len(files))
		}

		fileSize, chunks, isChunked, err := processFile(ctx, casClient, bsClient, file.path, *instanceName, *verbose, output)
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

	nonChunkedFiles := processedFiles - chunkedFiles
	fmt.Fprintf(output, "Files processed: %d\n", processedFiles)
	fmt.Fprintf(output, "Chunked files: %d\n", chunkedFiles)
	fmt.Fprintf(output, "Non-chunked files: %d\n", nonChunkedFiles)
	fmt.Fprintf(output, "Total chunks: %d\n", totalChunks)
	if chunkedFiles > 0 {
		// Non-chunked files contribute 1 chunk each; subtract to get chunked-file chunks only
		chunksFromChunkedFiles := totalChunks - nonChunkedFiles
		fmt.Fprintf(output, "Average chunks per chunked file: %.1f\n", float64(chunksFromChunkedFiles)/float64(chunkedFiles))
	}
	fmt.Fprintln(output)

	printChunkStats(output, allChunks, totalOriginalSize, chunkedFiles, nonChunkedFiles)
	return nil
}

func findFiles(dir string) []fileInfo {
	var files []fileInfo
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v", path, err)
			return nil
		}
		if !info.IsDir() {
			files = append(files, fileInfo{path: path, size: info.Size()})
		}
		return nil
	}); err != nil {
		log.Printf("Error walking directory %q: %v", dir, err)
	}
	return files
}

func processFile(ctx context.Context, casClient repb.ContentAddressableStorageClient, bsClient bspb.ByteStreamClient, filePath, instanceName string, verbose bool, output io.Writer) (int64, []*repb.Digest, bool, error) {
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

	if verbose {
		fmt.Fprintf(output, "[%s] Uploading %s (%d bytes)...\n", filePath, fileDigest.Hash[:12], len(data))
	}
	uploadStart := time.Now()
	if _, _, err := cachetools.UploadFromReader(ctx, bsClient, rn, bytes.NewReader(data)); err != nil {
		return 0, nil, false, fmt.Errorf("upload: %w", err)
	}
	uploadDur := time.Since(uploadStart)
	if verbose {
		fmt.Fprintf(output, "[%s] Upload completed in %v (%.2f MB/s)\n", filePath, uploadDur, float64(len(data))/(1024*1024)/uploadDur.Seconds())
	}

	var chunks []*repb.Digest
	var isChunked bool
	if verbose {
		fmt.Fprintf(output, "[%s] Calling SplitBlob...\n", filePath)
	}
	splitStart := time.Now()
	splitResp, err := casClient.SplitBlob(ctx, &repb.SplitBlobRequest{
		InstanceName:   instanceName,
		BlobDigest:     fileDigest,
		DigestFunction: repb.DigestFunction_SHA256,
	})
	splitDur := time.Since(splitStart)
	if err != nil {
		notChunked := status.IsNotFoundError(err) ||
			(status.IsUnimplementedError(err) && strings.Contains(err.Error(), "was not stored with chunking"))
		if !notChunked {
			return 0, nil, false, fmt.Errorf("split: %w", err)
		}
		chunks = []*repb.Digest{fileDigest}
		isChunked = false
		if verbose {
			fmt.Fprintf(output, "[%s] SplitBlob completed in %v (not chunked)\n", filePath, splitDur)
		}
	} else {
		chunks = splitResp.ChunkDigests
		isChunked = true
		if verbose {
			fmt.Fprintf(output, "[%s] SplitBlob completed in %v (%d chunks)\n", filePath, splitDur, len(chunks))
		}
	}

	if verbose {
		fmt.Fprintf(output, "[%s] Downloading for verification...\n", filePath)
	}
	downloadStart := time.Now()
	var buf bytes.Buffer
	if err := cachetools.GetBlob(ctx, bsClient, rn, &buf); err != nil {
		return 0, nil, false, fmt.Errorf("download: %w", err)
	}
	downloadDur := time.Since(downloadStart)
	if verbose {
		fmt.Fprintf(output, "[%s] Download completed in %v (%.2f MB/s)\n", filePath, downloadDur, float64(buf.Len())/(1024*1024)/downloadDur.Seconds())
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

	fmt.Fprintln(output, "=== Storage Efficiency ===")
	fmt.Fprintf(output, "Original size: %.2f MB\n", float64(totalOriginalSize)/(1024*1024))
	fmt.Fprintf(output, "Deduplicated size: %.2f MB\n", float64(uniqueChunkBytes)/(1024*1024))
	fmt.Fprintln(output)
	fmt.Fprintf(output, "Chunked files: %d (server-chunked)\n", chunkedFiles)
	fmt.Fprintf(output, "Non-chunked files: %d (stored whole)\n", nonChunkedFiles)
	fmt.Fprintf(output, "Unique chunks: %d\n", uniqueChunks)
	if uniqueChunks > 0 {
		fmt.Fprintf(output, "Shared chunks: %d (%.1f%%)\n",
			sharedChunks, float64(sharedChunks)/float64(uniqueChunks)*100)
	} else {
		fmt.Fprintf(output, "Shared chunks: 0\n")
	}
	fmt.Fprintln(output)
	if totalOriginalSize > 0 {
		dedupSavings := totalOriginalSize - uniqueChunkBytes
		if dedupSavings >= 0 {
			fmt.Fprintf(output, "Dedup savings: %.2f MB (%.1f%%)\n",
				float64(dedupSavings)/(1024*1024),
				float64(dedupSavings)/float64(totalOriginalSize)*100)
		} else {
			fmt.Fprintf(output, "Dedup overhead: %.2f MB (%.1f%% increase due to chunking)\n",
				float64(-dedupSavings)/(1024*1024),
				float64(-dedupSavings)/float64(totalOriginalSize)*100)
		}
	} else {
		fmt.Fprintln(output, "Dedup savings: N/A (no data processed)")
	}
}
