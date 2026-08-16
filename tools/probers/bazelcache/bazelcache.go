// bazelcache is a prober that tests remote cache services by writing and reading random bytes.
// This prober tests ByteStream, ActionCache, and ContentAddressableStorage services.
//
// It is intended to be run as a cloudprober EXTERNAL probe. Per-operation
// metrics are printed to stdout in cloudprober's payload format:
//
//	op_latency_usec{op=ByteStream.Read,compressor=zstd} 123456
//	op_failure_count{op=ByteStream.Read,compressor=zstd} 0
//
// Latency is only reported for successful operations. The process exits
// non-zero if any check failed.
//
// Each operation runs with its own --op_timeout so that a single slow RPC
// produces a targeted error log and failure metric instead of tripping the
// overall prober deadline. The worst-case run is the warm-up followed by the
// longest check chain (the 4-op CAS chain), i.e. 5 sequential ops, so the
// cloudprober probe timeout should exceed 5 * --op_timeout plus some slack:
// with the default 8s op timeout that is 40s, so a 50s probe timeout (and a
// 60s interval) leaves headroom.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	cacheTarget  = flag.String("cache_target", "", "Cache grpc target (required)")
	instanceName = flag.String("instance_name", "", "Remote instance name")
	apiKey       = flag.String("api_key", "", "API key for authentication")
	blobSize     = flag.Int64("blob_size", 100_000, "Size of test blobs in bytes")
	opTimeout    = flag.Duration("op_timeout", 8*time.Second, "Timeout for each individual cache operation")

	numConnections = flag.Int("connections", 8, "Number of independent connections to run the full check suite on, in parallel. Each opens its own gRPC connection and is warmed up separately. The connections run concurrently, so this does not change the overall run time.")
)

type opResult struct {
	op         string
	compressor string // empty if not applicable to this op
	latency    time.Duration
	err        error
}

// results is the shared sink that every connection's prober records into.
type results struct {
	mu   sync.Mutex
	data []opResult
}

func (r *results) add(res opResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = append(r.data, res)
}

// prober runs the full check suite over a single connection. Multiple probers,
// one per connection, run concurrently and record into a shared results sink.
type prober struct {
	ctx     context.Context
	results *results
	bs      bspb.ByteStreamClient
	ac      repb.ActionCacheClient
	cas     repb.ContentAddressableStorageClient
}

// do runs a single cache operation with its own timeout, records its latency
// and outcome for metrics reporting, and logs failures.
func (p *prober) do(op, compressor string, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(p.ctx, *opTimeout)
	defer cancel()
	start := time.Now()
	err := fn(ctx)
	latency := time.Since(start)

	p.results.add(opResult{op: op, compressor: compressor, latency: latency, err: err})

	if err != nil {
		log.Errorf("%s failed after %s: %s", opDesc(op, compressor), latency, err)
	}
	return err
}

func opDesc(op, compressor string) string {
	if compressor == "" {
		return op
	}
	return op + " (compressor=" + compressor + ")"
}

func compressorName(compressor repb.Compressor_Value) string {
	return strings.ToLower(compressor.String())
}

func (p *prober) checkByteStream(compressor repb.Compressor_Value) error {
	c := compressorName(compressor)
	gen := digest.RandomGenerator(rand.Int63())
	d, buf, err := gen.RandomDigestBuf(*blobSize)
	if err != nil {
		log.Errorf("failed to generate random data: %s", err)
		return err
	}
	rn := digest.NewCASResourceName(d, *instanceName, repb.DigestFunction_SHA256)
	rn.SetCompressor(compressor)

	if err := p.do("ByteStream.Write", c, func(ctx context.Context) error {
		_, _, err := cachetools.UploadFromReader(ctx, p.bs, rn, bytes.NewReader(buf))
		return err
	}); err != nil {
		return err
	}
	return p.do("ByteStream.Read", c, func(ctx context.Context) error {
		var downloaded bytes.Buffer
		if err := cachetools.GetBlob(ctx, p.bs, rn, &downloaded); err != nil {
			return err
		}
		if !bytes.Equal(buf, downloaded.Bytes()) {
			return fmt.Errorf("downloaded data does not match uploaded data")
		}
		return nil
	})
}

func (p *prober) checkActionCache() error {
	gen := digest.RandomGenerator(rand.Int63())
	actionDigest, _, err := gen.RandomDigestBuf(100)
	if err != nil {
		log.Errorf("failed to generate random digest: %s", err)
		return err
	}

	actionResult := &repb.ActionResult{
		ExitCode:  0,
		StdoutRaw: []byte("test stdout"),
		StderrRaw: []byte("test stderr"),
	}

	if err := p.do("ActionCache.UpdateActionResult", "", func(ctx context.Context) error {
		_, err := p.ac.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
			InstanceName:   *instanceName,
			ActionDigest:   actionDigest,
			ActionResult:   actionResult,
			DigestFunction: repb.DigestFunction_SHA256,
		})
		return err
	}); err != nil {
		return err
	}
	return p.do("ActionCache.GetActionResult", "", func(ctx context.Context) error {
		got, err := p.ac.GetActionResult(ctx, &repb.GetActionResultRequest{
			ActionDigest:   actionDigest,
			InstanceName:   *instanceName,
			DigestFunction: repb.DigestFunction_SHA256,
		})
		if err != nil {
			return err
		}
		if got.GetExitCode() != actionResult.GetExitCode() {
			return fmt.Errorf("exit code mismatch: expected %d, got %d", actionResult.GetExitCode(), got.GetExitCode())
		}
		if !bytes.Equal(got.GetStdoutRaw(), actionResult.GetStdoutRaw()) {
			return fmt.Errorf("stdout mismatch")
		}
		if !bytes.Equal(got.GetStderrRaw(), actionResult.GetStderrRaw()) {
			return fmt.Errorf("stderr mismatch")
		}
		return nil
	})
}

func (p *prober) checkCAS(compressor repb.Compressor_Value) error {
	c := compressorName(compressor)
	gen := digest.RandomGenerator(rand.Int63())
	const numBlobs = 3

	var digests []*repb.Digest
	blobsByHash := make(map[string][]byte, numBlobs)
	for i := 0; i < numBlobs; i++ {
		d, buf, err := gen.RandomDigestBuf(1024)
		if err != nil {
			log.Errorf("failed to generate random data: %s", err)
			return err
		}
		digests = append(digests, d)
		blobsByHash[d.GetHash()] = buf
	}

	findReq := &repb.FindMissingBlobsRequest{
		InstanceName:   *instanceName,
		DigestFunction: repb.DigestFunction_SHA256,
		BlobDigests:    digests,
	}
	if err := p.do("CAS.FindMissingBlobs", c, func(ctx context.Context) error {
		resp, err := p.cas.FindMissingBlobs(ctx, findReq)
		if err != nil {
			return err
		}
		// The blobs are freshly generated random data, so none of them
		// should exist in the cache yet.
		if n := len(resp.GetMissingBlobDigests()); n != numBlobs {
			return fmt.Errorf("expected all %d blobs to be missing before upload, got %d", numBlobs, n)
		}
		return nil
	}); err != nil {
		return err
	}

	var requests []*repb.BatchUpdateBlobsRequest_Request
	for _, d := range digests {
		data := blobsByHash[d.GetHash()]
		if compressor == repb.Compressor_ZSTD {
			data = compression.CompressZstd(nil, data)
		}
		requests = append(requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     d,
			Data:       data,
			Compressor: compressor,
		})
	}
	if err := p.do("CAS.BatchUpdateBlobs", c, func(ctx context.Context) error {
		resp, err := p.cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
			InstanceName:   *instanceName,
			DigestFunction: repb.DigestFunction_SHA256,
			Requests:       requests,
		})
		if err != nil {
			return err
		}
		for _, r := range resp.GetResponses() {
			if r.GetStatus().GetCode() != 0 {
				return fmt.Errorf("blob %s upload failed: %s", r.GetDigest().GetHash(), r.GetStatus())
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := p.do("CAS.FindMissingBlobsAfterUpload", c, func(ctx context.Context) error {
		resp, err := p.cas.FindMissingBlobs(ctx, findReq)
		if err != nil {
			return err
		}
		if n := len(resp.GetMissingBlobDigests()); n != 0 {
			return fmt.Errorf("expected 0 missing blobs after upload, got %d", n)
		}
		return nil
	}); err != nil {
		return err
	}

	return p.do("CAS.BatchReadBlobs", c, func(ctx context.Context) error {
		resp, err := cachetools.BatchReadBlobs(ctx, p.cas, &repb.BatchReadBlobsRequest{
			InstanceName:          *instanceName,
			DigestFunction:        repb.DigestFunction_SHA256,
			Digests:               digests,
			AcceptableCompressors: []repb.Compressor_Value{compressor},
		})
		if err != nil {
			return err
		}
		if len(resp) != numBlobs {
			return fmt.Errorf("expected %d batch read responses, got %d", numBlobs, len(resp))
		}
		for _, r := range resp {
			if r.Err != nil {
				return fmt.Errorf("blob %s download failed: %s", r.Digest.GetHash(), r.Err)
			}
			expected, ok := blobsByHash[r.Digest.GetHash()]
			if !ok {
				return fmt.Errorf("unexpected blob %s in response", r.Digest.GetHash())
			}
			if !bytes.Equal(expected, r.Data) {
				return fmt.Errorf("blob %s data mismatch", r.Digest.GetHash())
			}
		}
		return nil
	})
}

// printMetrics writes one op_latency_usec/op_failure_count line pair per
// operation in cloudprober's external probe payload format. Latencies from all
// connections are pooled into a single distribution per op, emitted as
// cloudprober's comma-separated value list; op_failure_count is the total
// number of failures across connections for that op.
func (r *results) printMetrics(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	type key struct{ op, compressor string }
	type agg struct {
		latencyUsec []int64
		failures    int
	}
	var order []key
	groups := map[key]*agg{}
	for _, res := range r.data {
		k := key{res.op, res.compressor}
		a := groups[k]
		if a == nil {
			a = &agg{}
			groups[k] = a
			order = append(order, k)
		}
		if res.err != nil {
			a.failures++
		} else {
			a.latencyUsec = append(a.latencyUsec, res.latency.Microseconds())
		}
	}
	for _, k := range order {
		a := groups[k]
		labels := "op=" + k.op
		if k.compressor != "" {
			labels += ",compressor=" + k.compressor
		}
		if len(a.latencyUsec) > 0 {
			vals := make([]string, len(a.latencyUsec))
			for i, v := range a.latencyUsec {
				vals[i] = strconv.FormatInt(v, 10)
			}
			fmt.Fprintf(w, "op_latency_usec{%s} %s\n", labels, strings.Join(vals, ","))
		}
		fmt.Fprintf(w, "op_failure_count{%s} %d\n", labels, a.failures)
	}
}

// warmup forces the shared connection to fully establish before the timed
// checks run, so the one-time handshake isn't charged to them. It issues a
// single cheap, read-only lookup that leaves no state behind, recorded as the
// Connection.Setup op so the handshake cost is visible as its own metric.
func (p *prober) warmup() error {
	return p.do("Connection.Setup", "", func(ctx context.Context) error {
		_, err := p.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
			InstanceName:   *instanceName,
			DigestFunction: repb.DigestFunction_SHA256,
			BlobDigests:    []*repb.Digest{{Hash: digest.EmptyHash, SizeBytes: 0}},
		})
		return err
	})
}

// run warms up this connection and then runs the check suite over it. Warm-up
// failure short-circuits the checks (they would all fail the same way).
func (p *prober) run() error {
	if err := p.warmup(); err != nil {
		return err
	}
	g := &errgroup.Group{}
	g.Go(func() error { return p.checkByteStream(repb.Compressor_IDENTITY) })
	g.Go(func() error { return p.checkByteStream(repb.Compressor_ZSTD) })
	g.Go(p.checkActionCache)
	g.Go(func() error { return p.checkCAS(repb.Compressor_IDENTITY) })
	g.Go(func() error { return p.checkCAS(repb.Compressor_ZSTD) })
	return g.Wait()
}

func main() {
	flag.Parse()

	if *cacheTarget == "" {
		log.Fatalf("--cache_target is required")
	}

	if *numConnections < 1 {
		log.Fatalf("--connections must be at least 1")
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	res := &results{}

	log.Infof("Connecting to cache at %s using %d connection(s)", *cacheTarget, *numConnections)
	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()
	probers := make([]*prober, 0, *numConnections)
	for range *numConnections {
		conn, err := grpc_client.DialSimpleWithoutPooling(*cacheTarget)
		if err != nil {
			log.Fatalf("failed to connect to cache: %s", err)
		}
		closers = append(closers, conn)
		probers = append(probers, &prober{
			ctx:     ctx,
			results: res,
			bs:      bspb.NewByteStreamClient(conn),
			ac:      repb.NewActionCacheClient(conn),
			cas:     repb.NewContentAddressableStorageClient(conn),
		})
	}

	// Each connection runs the full suite independently and in parallel, so a
	// single degraded backend behind the load balancer is more likely to be hit
	// within a run without changing the overall run time. Any failure on any
	// connection fails the run.
	g := &errgroup.Group{}
	for _, p := range probers {
		g.Go(p.run)
	}
	err := g.Wait()

	res.printMetrics(os.Stdout)

	if err != nil {
		os.Exit(1)
	}
}
