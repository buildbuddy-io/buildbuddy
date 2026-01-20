package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cdc "github.com/buildbarn/go-cdc"
	"github.com/cespare/xxhash/v2"
	fastcdc "github.com/jotfs/fastcdc-go"
	fastcdc2020 "github.com/jotfs/fastcdc-go/v2020"
)

const usage = `chunk_benchmark compares different CDC chunking algorithms.

Algorithms tested:
  - fastcdc-2016: Original FastCDC implementation
  - fastcdc-2020: FastCDC 2020 with 2-byte rolling optimization
  - go-cdc-max: Buildbarn's MaxCDC with lookahead

Files are streamed and only chunk hashes are stored (not data).`

type fileInfo struct {
	path string
	size int64
}

type algorithmResult struct {
	name             string
	averageSize      int
	chunkTime        time.Duration
	totalFiles       int64
	chunkedFiles     int64
	skippedFiles     int64
	totalChunks      int64
	uniqueChunks     int64
	reusedChunks     int64
	totalBytes       int64
	uniqueChunkBytes int64
	dedupPercent     float64

	// CDC chunk stats
	cdcChunks        int64
	cdcChunkBytes    int64
	cdcAvgChunkSize  float64
	cdcMinChunkSize  int64
	cdcMaxChunkSize  int64
	cdcStdevChunk    float64
	avgChunksPerFile float64

	// Memory stats
	heapAlloc    uint64
	totalAlloc   uint64
	numGC        uint32
	gcPauseTotal time.Duration
}

type chunkerFunc func(data []byte, averageSize int) ([]chunkResult, error)

// fastCDC2020Opts allows configuring normalization level
type fastCDC2020Opts struct {
	normalization        int
	disableNormalization bool
}

func makeFastCDC2020Chunker(opts fastCDC2020Opts) chunkerFunc {
	return func(data []byte, averageSize int) ([]chunkResult, error) {
		chunkerOpts := fastcdc2020.Options{
			AverageSize:          averageSize,
			MinSize:              averageSize / 4,
			MaxSize:              averageSize * 4,
			BufSize:              averageSize * 8,
			Normalization:        opts.normalization,
			DisableNormalization: opts.disableNormalization,
		}

		chunker, err := fastcdc2020.NewChunker(bytes.NewReader(data), chunkerOpts)
		if err != nil {
			return nil, err
		}

		var results []chunkResult
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			results = append(results, chunkResult{
				hash: hashBytes(chunk.Data),
				size: int64(len(chunk.Data)),
			})
		}

		return results, nil
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("chunk_benchmark", flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "%s\n\n", usage)
		flags.PrintDefaults()
	}
	directory := flags.String("directory", "", "Directory to walk and benchmark")
	workers := flags.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	avgSize := flags.Int("avg-size", 512*1024, "Average chunk size in bytes (default 512KB)")

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *directory == "" {
		return fmt.Errorf("directory is required")
	}

	files := findFiles(*directory)
	if len(files) == 0 {
		return fmt.Errorf("no files found in %s", *directory)
	}

	var totalSize int64
	for _, f := range files {
		totalSize += f.size
	}

	fmt.Fprintf(output, "Found %d files totaling %s\n", len(files), formatBytes(totalSize))
	fmt.Fprintf(output, "Using %d parallel workers\n", *workers)
	fmt.Fprintf(output, "Average chunk size: %s (min: %s, max: %s)\n\n",
		formatSize(*avgSize), formatSize(*avgSize/4), formatSize(*avgSize*4))

	algorithms := []struct {
		name    string
		chunker chunkerFunc
	}{
		{"fastcdc-2016", chunkFastCDC2016},
		{"fastcdc-2020-nc0", makeFastCDC2020Chunker(fastCDC2020Opts{disableNormalization: true})},
		{"fastcdc-2020-nc1", makeFastCDC2020Chunker(fastCDC2020Opts{normalization: 1})},
		{"fastcdc-2020-nc2", makeFastCDC2020Chunker(fastCDC2020Opts{normalization: 2})},
		{"fastcdc-2020-nc3", makeFastCDC2020Chunker(fastCDC2020Opts{normalization: 3})},
		{"go-cdc-max", chunkGoCDCMax},
	}

	var results []algorithmResult

	for _, algo := range algorithms {
		fmt.Fprintf(output, "\n%s Testing: %s %s\n",
			strings.Repeat("━", 20), algo.name, strings.Repeat("━", 20))
		result, err := runBenchmark(output, files, *avgSize, *workers, algo.name, algo.chunker)
		if err != nil {
			return fmt.Errorf("benchmark failed for %s: %w", algo.name, err)
		}
		results = append(results, result)
		printSingleResult(output, result)
	}

	fmt.Fprintln(output)
	fmt.Fprintln(output, strings.Repeat("═", 90))
	printResultsTable(output, results)
	printBestResult(output, results)

	return nil
}

func findFiles(dir string) []fileInfo {
	var files []fileInfo
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
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

type threadSafeResult struct {
	mu              sync.Mutex
	chunkHashes     map[string]int64
	reusedChunks    int64
	cdcChunkSizes   []int64
	cdcMinChunkSize int64
	cdcMaxChunkSize int64
}

func (r *threadSafeResult) addCDCChunk(h string, size int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.chunkHashes[h]; !exists {
		r.chunkHashes[h] = size
	} else {
		r.reusedChunks++
	}
	r.cdcChunkSizes = append(r.cdcChunkSizes, size)
	if r.cdcMinChunkSize == 0 || size < r.cdcMinChunkSize {
		r.cdcMinChunkSize = size
	}
	if size > r.cdcMaxChunkSize {
		r.cdcMaxChunkSize = size
	}
}

func runBenchmark(output io.Writer, files []fileInfo, averageSize int, numWorkers int, algoName string, chunker chunkerFunc) (algorithmResult, error) {
	// Force GC and capture starting memory stats
	runtime.GC()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	tsResult := &threadSafeResult{
		chunkHashes: make(map[string]int64),
	}

	var totalFiles, chunkedFiles, skippedFiles, totalChunks, totalBytes, totalChunkBytes int64
	var cdcChunks, cdcChunkBytes int64 // CDC-only stats (excludes files treated as single chunks)

	var processedBytes int64
	var totalBytesAll int64
	for _, f := range files {
		totalBytesAll += f.size
	}

	startTime := time.Now()

	workChan := make(chan fileInfo, numWorkers*2)

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt64(&processedBytes)
				files := atomic.LoadInt64(&totalFiles)
				elapsed := time.Since(startTime)
				throughput := float64(processed) / elapsed.Seconds() / (1024 * 1024)
				pct := float64(processed) / float64(totalBytesAll) * 100
				fmt.Fprintf(output, "  [%s] %d files (%.1f%%), %s/%s, %.1f MB/s\n",
					algoName, files, pct, formatBytes(processed), formatBytes(totalBytesAll), throughput)
			case <-progressDone:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			maxChunkSize := averageSize * 4

			for file := range workChan {
				data, err := os.ReadFile(file.path)
				if err != nil {
					continue
				}

				fileSize := int64(len(data))
				atomic.AddInt64(&processedBytes, fileSize)

				// Skip files under chunking threshold from all stats
				if len(data) <= maxChunkSize {
					atomic.AddInt64(&skippedFiles, 1)
					continue
				}

				atomic.AddInt64(&totalFiles, 1)
				atomic.AddInt64(&totalBytes, fileSize)
				atomic.AddInt64(&chunkedFiles, 1)

				chunks, err := chunker(data, averageSize)
				if err != nil {
					log.Printf("Error chunking %s: %v", file.path, err)
					continue
				}

				for _, c := range chunks {
					tsResult.addCDCChunk(c.hash, c.size)
					atomic.AddInt64(&totalChunks, 1)
					atomic.AddInt64(&totalChunkBytes, c.size)
					atomic.AddInt64(&cdcChunks, 1)
					atomic.AddInt64(&cdcChunkBytes, c.size)
				}
			}
		}()
	}

	for _, file := range files {
		workChan <- file
	}
	close(workChan)

	wg.Wait()
	close(progressDone)

	chunkTime := time.Since(startTime)

	// Capture ending memory stats
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	var uniqueChunkBytes int64
	for _, size := range tsResult.chunkHashes {
		uniqueChunkBytes += size
	}

	// Calculate GC pause time during this run
	var gcPauseTotal time.Duration
	for i := uint32(0); i < memEnd.NumGC-memStart.NumGC && i < 256; i++ {
		idx := (memEnd.NumGC - 1 - i) % 256
		gcPauseTotal += time.Duration(memEnd.PauseNs[idx])
	}

	// Calculate CDC stdev
	var cdcStdev float64
	if len(tsResult.cdcChunkSizes) > 0 {
		mean := float64(cdcChunkBytes) / float64(len(tsResult.cdcChunkSizes))
		var sumSquares float64
		for _, size := range tsResult.cdcChunkSizes {
			diff := float64(size) - mean
			sumSquares += diff * diff
		}
		cdcStdev = math.Sqrt(sumSquares / float64(len(tsResult.cdcChunkSizes)))
	}

	result := algorithmResult{
		name:             algoName,
		averageSize:      averageSize,
		chunkTime:        chunkTime,
		totalFiles:       totalFiles,
		chunkedFiles:     chunkedFiles,
		skippedFiles:     skippedFiles,
		totalChunks:      totalChunks,
		uniqueChunks:     int64(len(tsResult.chunkHashes)),
		reusedChunks:     tsResult.reusedChunks,
		totalBytes:       totalBytes,
		uniqueChunkBytes: uniqueChunkBytes,
		cdcChunks:        cdcChunks,
		cdcChunkBytes:    cdcChunkBytes,
		cdcMinChunkSize:  tsResult.cdcMinChunkSize,
		cdcMaxChunkSize:  tsResult.cdcMaxChunkSize,
		cdcStdevChunk:    cdcStdev,
		heapAlloc:        memEnd.HeapAlloc,
		totalAlloc:       memEnd.TotalAlloc - memStart.TotalAlloc,
		numGC:            memEnd.NumGC - memStart.NumGC,
		gcPauseTotal:     gcPauseTotal,
	}

	if cdcChunks > 0 {
		result.cdcAvgChunkSize = float64(cdcChunkBytes) / float64(cdcChunks)
	}
	if chunkedFiles > 0 {
		result.avgChunksPerFile = float64(cdcChunks) / float64(chunkedFiles)
	}

	if result.totalBytes > 0 {
		result.dedupPercent = float64(result.totalBytes-result.uniqueChunkBytes) / float64(result.totalBytes) * 100
		if result.dedupPercent < 0 {
			result.dedupPercent = 0
		}
	}

	return result, nil
}

type chunkResult struct {
	hash string
	size int64
}

// chunkFastCDC2016 uses the original FastCDC implementation
func chunkFastCDC2016(data []byte, averageSize int) ([]chunkResult, error) {
	opts := fastcdc.Options{
		AverageSize: averageSize,
		MinSize:     averageSize / 4,
		MaxSize:     averageSize * 4,
	}

	chunker, err := fastcdc.NewChunker(bytes.NewReader(data), opts)
	if err != nil {
		return nil, err
	}

	var results []chunkResult
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		results = append(results, chunkResult{
			hash: hashBytes(chunk.Data),
			size: int64(len(chunk.Data)),
		})
	}

	return results, nil
}

// chunkFastCDC2020 uses the FastCDC 2020 algorithm with 2-byte optimization
func chunkFastCDC2020(data []byte, averageSize int) ([]chunkResult, error) {
	opts := fastcdc2020.Options{
		AverageSize: averageSize,
		MinSize:     averageSize / 4,
		MaxSize:     averageSize * 4,
		BufSize:     averageSize * 8,
	}

	chunker, err := fastcdc2020.NewChunker(bytes.NewReader(data), opts)
	if err != nil {
		return nil, err
	}

	var results []chunkResult
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		results = append(results, chunkResult{
			hash: hashBytes(chunk.Data),
			size: int64(len(chunk.Data)),
		})
	}

	return results, nil
}

// chunkGoCDCMax uses Buildbarn's MaxCDC algorithm
func chunkGoCDCMax(data []byte, averageSize int) ([]chunkResult, error) {
	// Use a tighter min/max spread (~4x) for better MaxCDC performance.
	// This also produces similar average chunk sizes to FastCDC.
	minSize := averageSize * 2 / 5 // 1 - 3/5
	maxSize := minSize * 4         // 1 + 3/5
	bufSize := maxSize * 2

	chunker := cdc.NewMaxContentDefinedChunker(bytes.NewReader(data), bufSize, minSize, maxSize)

	var results []chunkResult
	for {
		chunkData, err := chunker.ReadNextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		results = append(results, chunkResult{
			hash: hashBytes(chunkData),
			size: int64(len(chunkData)),
		})
	}

	return results, nil
}

func hashBytes(data []byte) string {
	return fmt.Sprintf("%x", xxhash.Sum64(data))
}

func formatSize(bytes int) string {
	if bytes >= 1024*1024 {
		return fmt.Sprintf("%dMB", bytes/(1024*1024))
	}
	return fmt.Sprintf("%dKB", bytes/1024)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}

func printSingleResult(output io.Writer, r algorithmResult) {
	filesChunkedPct := float64(r.chunkedFiles) / float64(r.totalFiles) * 100
	chunksReusedPct := float64(r.reusedChunks) / float64(r.totalChunks) * 100
	uniquePct := float64(r.uniqueChunks) / float64(r.totalChunks) * 100
	bytesSaved := r.totalBytes - r.uniqueChunkBytes
	throughput := float64(r.totalBytes) / r.chunkTime.Seconds() / (1024 * 1024)

	fmt.Fprintf(output, "  Completed in %s (%.1f MB/s)\n", formatDuration(r.chunkTime), throughput)
	fmt.Fprintln(output)
	fmt.Fprintf(output, "  %-25s %d / %d (%.1f%%)\n", "Files chunked:",
		r.chunkedFiles, r.totalFiles, filesChunkedPct)
	fmt.Fprintf(output, "  %-25s %d / %d (%.1f%%)\n", "Chunks reused:",
		r.reusedChunks, r.totalChunks, chunksReusedPct)
	fmt.Fprintf(output, "  %-25s %d / %d (%.1f%%)\n", "Unique chunks:",
		r.uniqueChunks, r.totalChunks, uniquePct)
	fmt.Fprintf(output, "  %-25s %.1f\n", "Avg chunks/file:",
		r.avgChunksPerFile)
	fmt.Fprintln(output)
	fmt.Fprintln(output, "  CDC Chunk Distribution:")
	fmt.Fprintf(output, "    %-23s %s\n", "Avg:", formatBytes(int64(r.cdcAvgChunkSize)))
	fmt.Fprintf(output, "    %-23s %s\n", "Stdev:", formatBytes(int64(r.cdcStdevChunk)))
	fmt.Fprintf(output, "    %-23s %s\n", "Min:", formatBytes(r.cdcMinChunkSize))
	fmt.Fprintf(output, "    %-23s %s\n", "Max:", formatBytes(r.cdcMaxChunkSize))
	fmt.Fprintln(output)
	fmt.Fprintf(output, "  %-25s %s / %s (%.1f%%)\n", "Bytes deduped:",
		formatBytes(bytesSaved), formatBytes(r.totalBytes), r.dedupPercent)
	fmt.Fprintf(output, "  %-25s %s → %s\n", "Storage:",
		formatBytes(r.totalBytes), formatBytes(r.uniqueChunkBytes))
}

func printResultsTable(output io.Writer, results []algorithmResult) {
	fmt.Fprintln(output)
	fmt.Fprintln(output, "═══ COMPARISON TABLE ═══")
	fmt.Fprintln(output)

	fmt.Fprintf(output, "%-17s │ %-8s │ %-12s │ %-8s │ %-10s │ %-12s\n",
		"Algorithm", "Dedup%", "Saved", "Chunks/F", "Throughput", "Time")
	fmt.Fprintln(output, strings.Repeat("─", 85))

	for _, r := range results {
		throughput := float64(r.totalBytes) / r.chunkTime.Seconds() / (1024 * 1024)
		bytesSaved := r.totalBytes - r.uniqueChunkBytes

		fmt.Fprintf(output, "%-17s │ %7.2f%% │ %12s │ %8.1f │ %7.1f MB/s │ %s\n",
			r.name,
			r.dedupPercent,
			formatBytes(bytesSaved),
			r.avgChunksPerFile,
			throughput,
			formatDuration(r.chunkTime),
		)
	}

	// CDC chunk distribution table
	fmt.Fprintln(output)
	fmt.Fprintln(output, "═══ CDC CHUNK DISTRIBUTION ═══")
	fmt.Fprintln(output)

	fmt.Fprintf(output, "%-17s │ %-10s │ %-10s │ %-10s │ %-10s\n",
		"Algorithm", "Avg", "Stdev", "Min", "Max")
	fmt.Fprintln(output, strings.Repeat("─", 70))

	for _, r := range results {
		fmt.Fprintf(output, "%-17s │ %10s │ %10s │ %10s │ %10s\n",
			r.name,
			formatBytes(int64(r.cdcAvgChunkSize)),
			formatBytes(int64(r.cdcStdevChunk)),
			formatBytes(r.cdcMinChunkSize),
			formatBytes(r.cdcMaxChunkSize),
		)
	}

	// Memory and resource table
	fmt.Fprintln(output)
	fmt.Fprintln(output, "═══ RESOURCE USAGE ═══")
	fmt.Fprintln(output)

	fmt.Fprintf(output, "%-15s │ %-12s │ %-12s │ %-8s │ %-12s\n",
		"Algorithm", "HeapAlloc", "TotalAlloc", "GC Runs", "GC Pause")
	fmt.Fprintln(output, strings.Repeat("─", 70))

	for _, r := range results {
		fmt.Fprintf(output, "%-15s │ %12s │ %12s │ %8d │ %12s\n",
			r.name,
			formatBytes(int64(r.heapAlloc)),
			formatBytes(int64(r.totalAlloc)),
			r.numGC,
			formatDuration(r.gcPauseTotal),
		)
	}
}

func printBestResult(output io.Writer, results []algorithmResult) {
	if len(results) == 0 {
		return
	}

	sortedByDedup := make([]algorithmResult, len(results))
	copy(sortedByDedup, results)
	sort.Slice(sortedByDedup, func(i, j int) bool {
		return sortedByDedup[i].dedupPercent > sortedByDedup[j].dedupPercent
	})

	sortedBySpeed := make([]algorithmResult, len(results))
	copy(sortedBySpeed, results)
	sort.Slice(sortedBySpeed, func(i, j int) bool {
		ti := float64(sortedBySpeed[i].totalBytes) / sortedBySpeed[i].chunkTime.Seconds()
		tj := float64(sortedBySpeed[j].totalBytes) / sortedBySpeed[j].chunkTime.Seconds()
		return ti > tj
	})

	fmt.Fprintln(output)
	fmt.Fprintln(output, "═══ RANKINGS ═══")
	fmt.Fprintln(output)

	fmt.Fprintln(output, "By Deduplication:")
	for i, r := range sortedByDedup {
		saved := r.totalBytes - r.uniqueChunkBytes
		fmt.Fprintf(output, "  %d. %-15s │ %6.2f%% dedup │ saved %s\n",
			i+1, r.name, r.dedupPercent, formatBytes(saved))
	}

	fmt.Fprintln(output)
	fmt.Fprintln(output, "By Throughput:")
	for i, r := range sortedBySpeed {
		throughput := float64(r.totalBytes) / r.chunkTime.Seconds() / (1024 * 1024)
		fmt.Fprintf(output, "  %d. %-15s │ %6.1f MB/s │ %s\n",
			i+1, r.name, throughput, formatDuration(r.chunkTime))
	}
}
