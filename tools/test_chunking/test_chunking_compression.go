package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"

	bspb "google.golang.org/genproto/googleapis/bytestream"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// streamReadCloser converts a ByteStream ReadClient to an io.ReadCloser
type streamReadCloser struct {
	stream          bspb.ByteStream_ReadClient
	buf             []byte
	err             error
	compressedBytes int64 // Track compressed bytes received
}

func (s *streamReadCloser) Read(p []byte) (int, error) {
	if len(s.buf) == 0 {
		if s.err != nil {
			return 0, s.err
		}
		resp, err := s.stream.Recv()
		if err == io.EOF {
			s.err = io.EOF
			return 0, io.EOF
		}
		if err != nil {
			s.err = err
			return 0, err
		}
		s.buf = resp.Data
		s.compressedBytes += int64(len(resp.Data))
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func (s *streamReadCloser) Close() error {
	return nil // ByteStream client handles cleanup
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("test_chunking_compression", flag.ContinueOnError)
	filePath := fs.String("file_path", os.Getenv("HOME")+"/buildbuddy/bazel-bin/external/gazelle++go_deps+com_github_mattn_go_sqlite3/go-sqlite3.a", "Path to the file to test")
	serverAddr := fs.String("server_addr", "localhost:1985", "Address of the BuildBuddy server")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Printf("Connecting to BuildBuddy server at %s...\n", *serverAddr)

	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to server: %w", err)
	}
	defer conn.Close()

	casClient := repb.NewContentAddressableStorageClient(conn)
	bsClient := bspb.NewByteStreamClient(conn)
	fmt.Println("✓ Connected to server")

	// Read the file
	fmt.Printf("\nReading file: %s\n", *filePath)
	fileData, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}
	fmt.Printf("File size: %d bytes\n", len(fileData))

	// Compute digest for the file
	blobDigest, err := digest.Compute(bytes.NewReader(fileData), repb.DigestFunction_BLAKE3)
	if err != nil {
		return fmt.Errorf("computing digest: %w", err)
	}
	fmt.Printf("Blob digest: %s/%d\n", blobDigest.Hash, blobDigest.SizeBytes)

	// Step 1: Write blob using ByteStream.Write with ZSTD compression
	fmt.Println("\n=== Step 1: Writing blob with ZSTD compression ===")
	compressedData := compression.CompressZstd(nil, fileData)
	fmt.Printf("Original size: %d bytes\n", len(fileData))
	fmt.Printf("Compressed size: %d bytes (%.1f%%)\n", len(compressedData),
		float64(len(compressedData))/float64(len(fileData))*100)

	resourceName := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	resourceName.SetCompressor(repb.Compressor_ZSTD)

	stream, err := bsClient.Write(ctx)
	if err != nil {
		return fmt.Errorf("starting write stream: %w", err)
	}

	// Send data in chunks to avoid gRPC message size limits
	chunkSize := 10 * 1024 * 1024 // 10MB chunks
	offset := int64(0)
	firstChunk := true

	for offset < int64(len(compressedData)) {
		end := offset + int64(chunkSize)
		if end > int64(len(compressedData)) {
			end = int64(len(compressedData))
		}

		chunk := compressedData[offset:end]
		finishWrite := end == int64(len(compressedData))

		req := &bspb.WriteRequest{
			WriteOffset: offset,
			Data:        chunk,
			FinishWrite: finishWrite,
		}

		// Only send ResourceName in the first chunk
		if firstChunk {
			req.ResourceName = resourceName.NewUploadString()
			firstChunk = false
		}

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("sending write chunk at offset %d: %w", offset, err)
		}

		offset = end
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("closing write stream: %w", err)
	}

	fmt.Println("✓ Blob written successfully")

	// Step 2: Call SplitBlob to verify manifest is stored
	fmt.Println("\n=== Step 2: Calling SplitBlob ===")
	splitReq := &repb.SplitBlobRequest{
		BlobDigest:     blobDigest,
		DigestFunction: repb.DigestFunction_BLAKE3,
	}

	splitResp, err := casClient.SplitBlob(ctx, splitReq)
	if err != nil {
		return fmt.Errorf("splitting blob: %w", err)
	}

	fmt.Printf("✓ SplitBlob succeeded! Got %d chunk digests\n", len(splitResp.ChunkDigests))
	for i, chunkDigest := range splitResp.ChunkDigests {
		hashPreview := chunkDigest.Hash
		if len(hashPreview) > 16 {
			hashPreview = hashPreview[:16] + "..."
		}
		fmt.Printf("  Chunk %d: %s (size: %d bytes)\n", i, hashPreview, chunkDigest.SizeBytes)
	}

	// Step 3: Read blob back via ByteStream with ZSTD compression
	fmt.Println("\n=== Step 3: Reading blob back with ZSTD compression ===")
	readResourceName := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	readResourceName.SetCompressor(repb.Compressor_ZSTD)

	readReq := &bspb.ReadRequest{
		ResourceName: readResourceName.DownloadString(),
	}

	readStream, err := bsClient.Read(ctx, readReq)
	if err != nil {
		return fmt.Errorf("starting read stream: %w", err)
	}

	// Create a ReadCloser from the ByteStream
	streamReader := &streamReadCloser{stream: readStream}

	// Use streaming decompression
	decompressingReader, err := compression.NewZstdDecompressingReader(streamReader)
	if err != nil {
		return fmt.Errorf("creating decompressing reader: %w", err)
	}
	defer decompressingReader.Close()

	// Read decompressed data in chunks
	var decompressedData bytes.Buffer
	buf := make([]byte, 1024*1024) // 1MB buffer
	totalDecompressed := 0
	for {
		n, err := decompressingReader.Read(buf)
		if n > 0 {
			decompressedData.Write(buf[:n])
			totalDecompressed += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading decompressed data: %w", err)
		}
	}

	compressedBytes := streamReader.compressedBytes
	fmt.Printf("✓ Read %d compressed bytes, decompressed to %d bytes (%.1f%% compression ratio)\n",
		compressedBytes, totalDecompressed, float64(compressedBytes)/float64(totalDecompressed)*100)

	// Verify the data matches
	decompressedBytes := decompressedData.Bytes()
	if !bytes.Equal(decompressedBytes, fileData) {
		fmt.Println("\n❌ Data mismatch detected! Analyzing differences...")

		// Compare lengths
		if len(decompressedBytes) != len(fileData) {
			fmt.Printf("Length mismatch: decompressed=%d, original=%d (diff: %d)\n",
				len(decompressedBytes), len(fileData), len(decompressedBytes)-len(fileData))
		} else {
			fmt.Printf("Lengths match: %d bytes\n", len(fileData))
		}

		// Find first difference
		minLen := len(decompressedBytes)
		if len(fileData) < minLen {
			minLen = len(fileData)
		}

		firstDiff := -1
		for i := 0; i < minLen; i++ {
			if decompressedBytes[i] != fileData[i] {
				firstDiff = i
				break
			}
		}

		if firstDiff >= 0 {
			fmt.Printf("First difference at byte offset: %d\n", firstDiff)

			// Show hex dump around the difference
			start := firstDiff - 16
			if start < 0 {
				start = 0
			}
			end := firstDiff + 16
			if end > minLen {
				end = minLen
			}

			fmt.Println("\nHex dump around first difference:")
			fmt.Println("Offset | Decompressed                    | Original")
			fmt.Println("-------|--------------------------------|--------------------------------")

			for i := start; i < end; i += 16 {
				fmt.Printf("%06x | ", i)

				// Decompressed hex
				decompLen := 0
				for j := 0; j < 16 && i+j < len(decompressedBytes); j++ {
					if i+j == firstDiff {
						fmt.Printf("\033[31m%02x\033[0m ", decompressedBytes[i+j]) // Red for diff
					} else {
						fmt.Printf("%02x ", decompressedBytes[i+j])
					}
					decompLen++
				}
				for j := decompLen; j < 16; j++ {
					fmt.Print("   ")
				}

				fmt.Print("| ")

				// Original hex
				for j := 0; j < 16 && i+j < len(fileData); j++ {
					if i+j == firstDiff {
						fmt.Printf("\033[31m%02x\033[0m ", fileData[i+j]) // Red for diff
					} else {
						fmt.Printf("%02x ", fileData[i+j])
					}
				}
				fmt.Println()
			}

			// Count total differences
			diffCount := 0
			for i := 0; i < minLen; i++ {
				if decompressedBytes[i] != fileData[i] {
					diffCount++
				}
			}
			if len(decompressedBytes) != len(fileData) {
				diffCount += abs(len(decompressedBytes) - len(fileData))
			}
			fmt.Printf("\nTotal bytes different: %d out of %d (%.2f%%)\n",
				diffCount, max(len(decompressedBytes), len(fileData)),
				float64(diffCount)/float64(max(len(decompressedBytes), len(fileData)))*100)
		} else if len(decompressedBytes) != len(fileData) {
			fmt.Printf("Data matches up to byte %d, but lengths differ\n", minLen)
		}

		return fmt.Errorf("reconstructed data does not match original!")
	}

	fmt.Println("✓ Data matches original file")
	fmt.Println("\n=== All tests passed! ===")

	return nil
}
