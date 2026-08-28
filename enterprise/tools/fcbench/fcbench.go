// fcbench is a load-generation / micro-benchmark harness for Firecracker
// action preparation.
//
// It drives FirecrackerContainer directly through the same APIs that the
// executor uses (NewContainer, PullImageIfNecessary, Create/Unpause,
// dirtools.DownloadTree, Exec, Pause, Remove) so that each stage of "action
// preparation" can be timed in isolation, without the scheduler in the way.
//
// It runs an in-process CAS (disk_cache + bytestream/CAS/AC gRPC servers on a
// local TCP port) unless --cache_target is given, generates synthetic input
// trees, uploads them, and then runs N iterations at concurrency K, recording
// per-iteration stage timings plus every tracing span emitted by the
// buildbuddy code under test.
//
// Must run as root (needs mount() for VBD/FUSE and jailer).
//
// Example:
//
//	fcbench --root=/fcb --data_dir=/fcb-data --workload=medium=2000:32768 \
//	  --iterations=10 --concurrency=2 --mode=recycle --out=/tmp/results.json \
//	  --executor.enable_local_snapshot_sharing=true \
//	  --executor.enable_remote_snapshot_sharing=true
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/cache/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	root        = flag.String("root", "/fcb", "Jailer/build root. Must be a short path (<38 chars).")
	dataDir     = flag.String("data_dir", "/fcb-data", "Directory for the local CAS (disk cache) and filecache.")
	fcSizeBytes = flag.Int64("filecache_size_bytes", 50_000_000_000, "Filecache size.")
	casSize     = flag.Int64("cas_size_bytes", 50_000_000_000, "Local CAS size.")
	cacheTarget = flag.String("cache_target", "", "If set, use this remote cache (grpc://host:port) instead of the in-process CAS.")
	listenPort  = flag.Int("cas_port", 0, "Port for the in-process CAS server (0 = random).")
	image       = flag.String("image", "mirror.gcr.io/library/busybox:latest", "Container image.")
	iterations  = flag.Int("iterations", 5, "Iterations per VM slot.")
	concurrency = flag.Int("concurrency", 1, "Number of concurrent VM slots.")
	mode        = flag.String("mode", "recycle", "recycle: pause/unpause a snapshot between iterations; fresh: boot a new VM each iteration; keep: keep the same VM running and only re-run inputs+exec.")
	sharedKey   = flag.Bool("shared_snapshot_key", true, "All slots share one snapshot key (like many actions of one workflow). If false, each slot has its own key.")
	workloadStr = flag.String("workload", "small=50:8192", "Input workload spec: name=numFiles:avgFileBytes[:numDirs][:maxFileBytes]. Multiple comma-separated specs run sequentially.")
	uniqueIn    = flag.Bool("unique_inputs", false, "Generate a new input tree for every iteration (cold filecache).")
	guestCmd    = flag.String("cmd", "true", "Command to run in the guest (sh -c).")
	touchInputs = flag.Bool("touch_inputs", false, "Instead of --cmd, run a command that reads every input file.")
	outputFiles = flag.Int("output_files", 0, "Number of output files (64KiB each) the guest command writes into the workspace.")
	memMB       = flag.Int64("mem_mb", 2000, "VM memory.")
	numCPUs     = flag.Int64("cpus", 2, "VM CPUs.")
	scratchMB   = flag.Int64("scratch_mb", 1000, "Scratch disk size.")
	network     = flag.Bool("network", false, "Enable VM networking (like prod). Off by default because it needs iptables/veth setup.")
	dockerd     = flag.Bool("init_dockerd", false, "Start dockerd in the VM (requires --network and an image with docker).")
	outPath     = flag.String("out", "/tmp/fcbench-results.json", "Where to write JSON results.")
	metricsOut  = flag.String("metrics_out", "", "If set, dump prometheus metrics (text format) here at the end.")
	label       = flag.String("label", "", "Free-form label recorded in results (e.g. experiment name).")
	warmup      = flag.Int("warmup", 1, "Number of untimed warmup iterations per slot (creates the initial snapshot in recycle mode).")
	keepData    = flag.Bool("keep_data", true, "Keep CAS/filecache between runs (so images and snapshots are warm).")
	logLevel    = flag.String("fcbench_log_level", "info", "Log level.")
	printStdout = flag.Bool("print_stdout", false, "Log guest command stdout/stderr.")
	overlap     = flag.Bool("overlap_unpause", false, "Experimental: run snapshot restore (Create/Unpause) concurrently with input fetch instead of after it.")
	seedFlag    = flag.Int64("seed", 0, "Seed for synthetic input generation. 0 = fixed seed for shared inputs, or time-based when --unique_inputs (so re-runs don't hit the filecache from a previous run).")
	treeImage   = flag.Bool("tree_image", false, "Experimental: download inputs into the filecache only and build the workspace image directly from the input tree (no host workspace hardlinks).")
	useVFS      = flag.Bool("vfs", false, "Use the guest FUSE VFS (executor.enable_vfs path): inputs are served lazily from the host filecache/CAS instead of via a workspace ext4 image.")
)

// ---------------------------------------------------------------------------
// Span capture
// ---------------------------------------------------------------------------

type spanRecord struct {
	Name     string            `json:"name"`
	TraceID  string            `json:"trace"`
	SpanID   string            `json:"span"`
	ParentID string            `json:"parent"`
	StartUS  int64             `json:"start_us"` // relative to process start
	DurUS    int64             `json:"dur_us"`
	Attrs    map[string]string `json:"attrs,omitempty"`
}

type spanCollector struct {
	mu    sync.Mutex
	spans []spanRecord
	t0    time.Time
}

func (s *spanCollector) OnStart(parent context.Context, sp sdktrace.ReadWriteSpan) {}
func (s *spanCollector) Shutdown(ctx context.Context) error                        { return nil }
func (s *spanCollector) ForceFlush(ctx context.Context) error                      { return nil }
func (s *spanCollector) OnEnd(sp sdktrace.ReadOnlySpan) {
	rec := spanRecord{
		Name:    sp.Name(),
		TraceID: sp.SpanContext().TraceID().String(),
		SpanID:  sp.SpanContext().SpanID().String(),
		StartUS: sp.StartTime().Sub(s.t0).Microseconds(),
		DurUS:   sp.EndTime().Sub(sp.StartTime()).Microseconds(),
	}
	if sp.Parent().IsValid() {
		rec.ParentID = sp.Parent().SpanID().String()
	}
	if attrs := sp.Attributes(); len(attrs) > 0 {
		rec.Attrs = make(map[string]string, len(attrs))
		for _, a := range attrs {
			rec.Attrs[string(a.Key)] = a.Value.Emit()
		}
	}
	s.mu.Lock()
	s.spans = append(s.spans, rec)
	s.mu.Unlock()
}

func (s *spanCollector) drain() []spanRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.spans
	s.spans = nil
	return out
}

func setupTracing(sc *spanCollector) {
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sc),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

func getEnv(ctx context.Context) (*real_environment.RealEnv, error) {
	hc := healthcheck.NewHealthChecker("fcbench")
	env := real_environment.NewRealEnv(hc)
	env.SetAuthenticator(nullauth.NewNullAuthenticator(true /*anonymousEnabled*/))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	hit_tracker.Register(env)

	var conn *grpc_client.ClientConnPool
	if *cacheTarget == "" {
		casDir := filepath.Join(*dataDir, "cas")
		if err := os.MkdirAll(casDir, 0755); err != nil {
			return nil, err
		}
		dc, err := disk_cache.NewDiskCache(env, &disk_cache.Options{RootDirectory: casDir}, *casSize)
		if err != nil {
			return nil, status.WrapError(err, "disk cache")
		}
		env.SetCache(dc)
		casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
		if err != nil {
			return nil, err
		}
		bsServer, err := byte_stream_server.NewByteStreamServer(env)
		if err != nil {
			return nil, err
		}
		acServer, err := action_cache_server.NewActionCacheServer(env)
		if err != nil {
			return nil, err
		}
		lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *listenPort))
		if err != nil {
			return nil, err
		}
		srv := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
		repb.RegisterContentAddressableStorageServer(srv, casServer)
		repb.RegisterActionCacheServer(srv, acServer)
		bspb.RegisterByteStreamServer(srv, bsServer)
		env.SetGRPCServer(srv)
		go func() {
			if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
				log.Fatalf("CAS server: %s", err)
			}
		}()
		target := fmt.Sprintf("grpc://%s", lis.Addr().String())
		log.Infof("In-process CAS listening at %s", target)
		conn, err = grpc_client.DialSimple(target)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		conn, err = grpc_client.DialSimple(*cacheTarget)
		if err != nil {
			return nil, err
		}
	}
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))

	fcDir := filepath.Join(*dataDir, "filecache")
	if err := os.MkdirAll(fcDir, 0755); err != nil {
		return nil, err
	}
	fc, err := filecache.NewFileCache(fcDir, *fcSizeBytes, false)
	if err != nil {
		return nil, status.WrapError(err, "filecache")
	}
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)

	leaser, err := cpuset.NewLeaser(cpuset.LeaserOpts{})
	if err != nil {
		return nil, err
	}
	env.SetCPULeaser(leaser)
	return env, nil
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

type workload struct {
	Name         string `json:"name"`
	NumFiles     int    `json:"num_files"`
	AvgFileBytes int    `json:"avg_file_bytes"`
	MaxFileBytes int    `json:"max_file_bytes"`
	NumDirs      int    `json:"num_dirs"`
}

func parseWorkloads(s string) ([]workload, error) {
	var out []workload
	for spec := range strings.SplitSeq(s, ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		name, rest, ok := strings.Cut(spec, "=")
		if !ok {
			return nil, fmt.Errorf("bad workload %q", spec)
		}
		parts := strings.Split(rest, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("bad workload %q", spec)
		}
		w := workload{Name: name}
		var err error
		if w.NumFiles, err = strconv.Atoi(parts[0]); err != nil {
			return nil, err
		}
		if w.AvgFileBytes, err = strconv.Atoi(parts[1]); err != nil {
			return nil, err
		}
		w.NumDirs = max(1, w.NumFiles/20)
		if len(parts) > 2 {
			if w.NumDirs, err = strconv.Atoi(parts[2]); err != nil {
				return nil, err
			}
		}
		w.MaxFileBytes = w.AvgFileBytes * 8
		if len(parts) > 3 {
			if w.MaxFileBytes, err = strconv.Atoi(parts[3]); err != nil {
				return nil, err
			}
		}
		out = append(out, w)
	}
	return out, nil
}

type inputTree struct {
	Tree       *repb.Tree
	RootDigest *repb.Digest
	TotalBytes int64
	NumFiles   int
}

// fileSize picks a size from a skewed distribution with the given mean: most
// files small, a long tail up to max. Deterministic for a given rng.
func fileSize(rng *rand.Rand, avg, maxSize int) int {
	// exponential-ish distribution around avg
	f := rng.ExpFloat64() * float64(avg)
	if f > float64(maxSize) {
		f = float64(maxSize)
	}
	if f < 1 {
		f = 1
	}
	return int(f)
}

// generateAndUploadTree builds a synthetic input tree and uploads all blobs
// (files + directory protos) to the CAS.
func generateAndUploadTree(ctx context.Context, env environment.Env, w workload, seed int64) (*inputTree, error) {
	rng := rand.New(rand.NewSource(seed))
	bs := env.GetByteStreamClient()
	df := repb.DigestFunction_SHA256

	dirs := make([]*repb.Directory, w.NumDirs)
	for i := range dirs {
		dirs[i] = &repb.Directory{}
	}
	var total int64
	buf := make([]byte, w.MaxFileBytes)
	// Upload files with some parallelism.
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i := 0; i < w.NumFiles; i++ {
		size := fileSize(rng, w.AvgFileBytes, w.MaxFileBytes)
		data := make([]byte, size)
		rng.Read(data[:min(size, 64)]) // random prefix; rest is a cheap pattern (compressible, like real code)
		if size > 64 {
			copy(data[64:], buf[:size-64])
			for j := 64; j < size; j += 4096 {
				data[j] = byte(rng.Intn(256))
			}
		}
		d, err := digest.Compute(bytes.NewReader(data), df)
		if err != nil {
			return nil, err
		}
		dirIdx := rng.Intn(w.NumDirs)
		mu.Lock()
		dirs[dirIdx].Files = append(dirs[dirIdx].Files, &repb.FileNode{
			Name:         fmt.Sprintf("f%06d.dat", i),
			Digest:       d,
			IsExecutable: i%7 == 0,
		})
		total += int64(size)
		mu.Unlock()
		wg.Add(1)
		sem <- struct{}{}
		go func(data []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := cachetools.UploadBlob(ctx, bs, "", df, bytes.NewReader(data)); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(data)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	// Build a 2-level tree: root -> dN dirs.
	rootDir := &repb.Directory{}
	tree := &repb.Tree{}
	for i, d := range dirs {
		sort.Slice(d.Files, func(a, b int) bool { return d.Files[a].Name < d.Files[b].Name })
		dd, err := cachetools.UploadProto(ctx, bs, "", df, d)
		if err != nil {
			return nil, err
		}
		rootDir.Directories = append(rootDir.Directories, &repb.DirectoryNode{Name: fmt.Sprintf("d%04d", i), Digest: dd})
		tree.Children = append(tree.Children, d)
	}
	// A couple of common workspace-shaped entries: an output dir and a symlink.
	rootDir.Symlinks = append(rootDir.Symlinks, &repb.SymlinkNode{Name: "link_to_d0000", Target: "d0000"})
	rootDigest, err := cachetools.UploadProto(ctx, bs, "", df, rootDir)
	if err != nil {
		return nil, err
	}
	tree.Root = rootDir
	return &inputTree{Tree: tree, RootDigest: rootDigest, TotalBytes: total, NumFiles: w.NumFiles}, nil
}

// ---------------------------------------------------------------------------
// Iteration
// ---------------------------------------------------------------------------

type iterResult struct {
	Workload    string           `json:"workload"`
	Slot        int              `json:"slot"`
	Iter        int              `json:"iter"`
	Warmup      bool             `json:"warmup"`
	TraceID     string           `json:"trace"`
	StartUS     int64            `json:"start_us"`
	Stages      map[string]int64 `json:"stages_us"` // wall time per stage
	FromSnap    bool             `json:"from_snapshot"`
	InputBytes  int64            `json:"input_bytes"`
	InputFiles  int              `json:"input_files"`
	LinkCount   int64            `json:"filecache_links"`
	FetchCount  int64            `json:"files_fetched"`
	FetchBytes  int64            `json:"bytes_fetched"`
	ExitCode    int              `json:"exit_code"`
	Err         string           `json:"error,omitempty"`
	TotalPrepUS int64            `json:"total_prep_us"` // everything before the guest command starts (approx: unpause+input_fetch+exec_prep)
	TotalUS     int64            `json:"total_us"`
}

type bench struct {
	env     environment.Env
	cfg     *firecracker.ExecutorConfig
	sc      *spanCollector
	t0      time.Time
	results []iterResult
	mu      sync.Mutex
}

func (b *bench) guestCommand(w workload) *repb.Command {
	script := *guestCmd
	if *touchInputs {
		script = "find . -type f -exec cat {} + > /dev/null"
	}
	if *outputFiles > 0 {
		// One process writes all output files (64 KiB each) so the cost is
		// the writes, not process spawns.
		script += fmt.Sprintf(`; mkdir -p out && awk 'BEGIN{for(i=0;i<%d;i++){f="out/o" i ".bin"; printf "%%65536s", "", > f; close(f)}}'`, *outputFiles)
	}
	props := []*repb.Platform_Property{
		{Name: "workload-isolation-type", Value: "firecracker"},
	}
	if *mode != "fresh" {
		props = append(props, &repb.Platform_Property{Name: "recycle-runner", Value: "true"})
	}
	return &repb.Command{
		Arguments:   []string{"sh", "-c", script},
		Platform:    &repb.Platform{Properties: props},
		OutputPaths: []string{"out"},
	}
}

func (b *bench) runSlot(ctx context.Context, w workload, slot int, trees []*inputTree) error {
	keySalt := "fcbench"
	if !*sharedKey {
		keySalt = fmt.Sprintf("fcbench-slot-%d", slot)
	}
	if *useVFS {
		keySalt += "-vfs"
	}
	cmd := b.guestCommand(w)
	cmd.Platform.Properties = append(cmd.Platform.Properties, &repb.Platform_Property{Name: "salt", Value: keySalt + "-" + w.Name})

	var keepC *firecracker.FirecrackerContainer
	defer func() {
		if keepC != nil {
			_ = keepC.Remove(ctx)
		}
	}()
	total := *warmup + *iterations
	for i := 0; i < total; i++ {
		tree := trees[0]
		if *uniqueIn {
			tree = trees[i]
		}
		res := iterResult{Workload: w.Name, Slot: slot, Iter: i - *warmup, Warmup: i < *warmup, Stages: map[string]int64{}, InputBytes: tree.TotalBytes, InputFiles: tree.NumFiles}
		iterCtx, span := tracing.StartNamedSpan(ctx, "fcbench.iteration")
		res.TraceID = span.SpanContext().TraceID().String()
		iterStart := time.Now()
		res.StartUS = iterStart.Sub(b.t0).Microseconds()

		task := &repb.ExecutionTask{
			Command: cmd,
			Action:  &repb.Action{InputRootDigest: tree.RootDigest},
			ExecuteRequest: &repb.ExecuteRequest{
				DigestFunction: repb.DigestFunction_SHA256,
			},
		}
		workDir := filepath.Join(*root, "work", fmt.Sprintf("s%d-i%d", slot, i))
		if err := os.MkdirAll(workDir, 0755); err != nil {
			return err
		}
		opts := firecracker.ContainerOpts{
			ContainerImage:         *image,
			ActionWorkingDirectory: workDir,
			VMConfiguration: &fcpb.VMConfiguration{
				NumCpus:           *numCPUs,
				MemSizeMb:         *memMB,
				ScratchDiskSizeMb: *scratchMB,
				NetworkMode:       fcpb.NetworkMode_NETWORK_MODE_OFF,
				InitDockerd:       *dockerd,
			},
			ExecutorConfig: b.cfg,
		}
		if *network {
			opts.VMConfiguration.NetworkMode = fcpb.NetworkMode_NETWORK_MODE_EXTERNAL
		}

		err := func() error {
			var c *firecracker.FirecrackerContainer
			stage := func(name string, f func() error) error {
				s := time.Now()
				err := f()
				res.Stages[name] = time.Since(s).Microseconds()
				return err
			}
			// Production order (executor.go): PrepareForTask (pull image),
			// DownloadInputs, then Run -> Create/Unpause. With --overlap_unpause
			// the restore runs concurrently with the input fetch.
			fetchInputs := func() error {
				return stage("input_fetch", func() error {
					dlOpts := &dirtools.DownloadTreeOpts{RootDir: workDir}
					if *useVFS || *treeImage {
						// Only populate the filecache; the guest fetches
						// lazily (VFS) or the image is built from the tree.
						dlOpts.RootDir = ""
					}
					txInfo, err := dirtools.DownloadTree(iterCtx, b.env, "", repb.DigestFunction_SHA256, tree.Tree, dlOpts)
					if err != nil {
						return err
					}
					res.LinkCount = txInfo.LinkCount
					res.FetchCount = txInfo.FileCount
					res.FetchBytes = txInfo.BytesTransferred
					return nil
				})
			}
			if *mode == "keep" && keepC != nil {
				c = keepC
				if err := fetchInputs(); err != nil {
					return status.WrapError(err, "download inputs")
				}
			} else {
				var err error
				err = stage("new_container", func() error {
					c, err = firecracker.NewContainer(iterCtx, b.env, task, opts)
					return err
				})
				if err != nil {
					return status.WrapError(err, "new container")
				}
				if *useVFS {
					c.SetTaskFileSystemLayout(&container.FileSystemLayout{
						Inputs:         tree.Tree,
						DigestFunction: repb.DigestFunction_SHA256,
					})
				}
				if *treeImage {
					c.SetWorkspaceInputTree(&container.FileSystemLayout{
						Inputs:         tree.Tree,
						DigestFunction: repb.DigestFunction_SHA256,
					})
				}
				if *mode == "keep" {
					keepC = c
				} else {
					defer func() {
						_ = stage("remove", func() error { return c.Remove(iterCtx) })
					}()
				}
				if err := stage("pull_image", func() error {
					return container.PullImageIfNecessary(iterCtx, b.env, c, oci.Credentials{}, *image, false)
				}); err != nil {
					return status.WrapError(err, "pull image")
				}
				createOrUnpause := func() error {
					return stage("create_or_unpause", func() error { return c.Create(iterCtx, workDir) })
				}
				if *overlap {
					errCh := make(chan error, 1)
					go func() { errCh <- createOrUnpause() }()
					fetchErr := fetchInputs()
					createErr := <-errCh
					if createErr != nil {
						return status.WrapError(createErr, "create")
					}
					if fetchErr != nil {
						return status.WrapError(fetchErr, "download inputs")
					}
				} else {
					if err := fetchInputs(); err != nil {
						return status.WrapError(err, "download inputs")
					}
					if err := createOrUnpause(); err != nil {
						return status.WrapError(err, "create")
					}
				}
				res.FromSnap = *mode == "recycle" && i > 0
			}
			var cmdRes *interfaces.CommandResult
			_ = stage("exec", func() error {
				cmdRes = c.Exec(iterCtx, cmd, nil)
				return cmdRes.Error
			})
			if cmdRes.Error != nil {
				return status.WrapError(cmdRes.Error, "exec")
			}
			res.ExitCode = cmdRes.ExitCode
			if *printStdout {
				log.Infof("guest stdout:\n%s\nguest stderr:\n%s", string(cmdRes.Stdout), string(cmdRes.Stderr))
			}
			if cmdRes.ExitCode != 0 {
				return fmt.Errorf("guest command exited %d: %s", cmdRes.ExitCode, string(cmdRes.Stderr))
			}
			if *mode == "recycle" {
				if err := stage("pause", func() error { return c.Pause(iterCtx) }); err != nil {
					return status.WrapError(err, "pause")
				}
			}
			return nil
		}()
		if err != nil {
			res.Err = err.Error()
			log.Errorf("slot %d iter %d: %s", slot, i, err)
		}
		span.End()
		res.TotalUS = time.Since(iterStart).Microseconds()
		res.TotalPrepUS = res.Stages["new_container"] + res.Stages["pull_image"] + res.Stages["create_or_unpause"] + res.Stages["input_fetch"]
		_ = os.RemoveAll(workDir)
		b.mu.Lock()
		b.results = append(b.results, res)
		b.mu.Unlock()
		log.Infof("[%s slot=%d iter=%d warmup=%v] total=%dms prep=%dms stages=%s err=%q", w.Name, slot, i-*warmup, res.Warmup, res.TotalUS/1000, res.TotalPrepUS/1000, fmtStages(res.Stages), res.Err)
		if err != nil && res.Warmup {
			return err
		}
	}
	return nil
}

func fmtStages(m map[string]int64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%dms", k, m[k]/1000))
	}
	return strings.Join(parts, " ")
}

type output struct {
	Label     string            `json:"label"`
	Args      []string          `json:"args"`
	Flags     map[string]string `json:"flags"`
	Host      map[string]string `json:"host"`
	Workloads []workload        `json:"workloads"`
	Results   []iterResult      `json:"results"`
	Spans     []spanRecord      `json:"spans"`
	StartTime time.Time         `json:"start_time"`
	EndTime   time.Time         `json:"end_time"`
}

func main() {
	flag.Parse()
	*log.LogLevel = *logLevel
	*log.IncludeShortFileName = true
	log.Configure()

	if os.Getuid() != 0 {
		log.Fatalf("fcbench must run as root")
	}
	if len(*root) > 38 {
		log.Fatalf("--root must be < 38 chars")
	}
	ctx := context.Background()
	sc := &spanCollector{t0: time.Now()}
	setupTracing(sc)

	if !*keepData {
		_ = os.RemoveAll(*dataDir)
	}
	flagutil.SetValueForFlagName("executor.cpu_leaser.enable", true, nil, false)
	flagutil.SetValueForFlagName("debug_enable_anonymous_runner_recycling", true, nil, false)
	if err := resources.Configure(true /*=snapshotSharingEnabled*/); err != nil {
		log.Fatalf("resources: %s", err)
	}
	if err := vbd.CleanStaleMounts(); err != nil {
		log.Warningf("clean stale VBD mounts: %s", err)
	}
	if *network {
		if err := networking.Configure(ctx); err != nil {
			log.Fatalf("networking: %s", err)
		}
		if err := networking.EnableMasquerading(ctx); err != nil {
			log.Fatalf("masquerading: %s", err)
		}
	}
	env, err := getEnv(ctx)
	if err != nil {
		log.Fatalf("env: %s", err)
	}
	buildRoot := filepath.Join(*root, "build")
	cacheRoot := filepath.Join(*dataDir, "cache")
	if err := os.MkdirAll(cacheRoot, 0755); err != nil {
		log.Fatalf("mkdir cache root: %s", err)
	}
	cfg, err := firecracker.GetExecutorConfig(ctx, buildRoot, cacheRoot)
	if err != nil {
		log.Fatalf("executor config: %s", err)
	}
	workloads, err := parseWorkloads(*workloadStr)
	if err != nil {
		log.Fatalf("workloads: %s", err)
	}

	b := &bench{env: env, cfg: cfg, sc: sc, t0: sc.t0}
	out := output{Label: *label, Args: os.Args[1:], Flags: map[string]string{}, Host: hostInfo(), Workloads: workloads, StartTime: time.Now()}
	flag.VisitAll(func(f *flag.Flag) {
		if f.Value.String() != f.DefValue {
			out.Flags[f.Name] = f.Value.String()
		}
	})

	for _, w := range workloads {
		log.Infof("=== workload %s: %d files avg %d bytes (max %d) in %d dirs", w.Name, w.NumFiles, w.AvgFileBytes, w.MaxFileBytes, w.NumDirs)
		nTrees := 1
		if *uniqueIn {
			nTrees = *warmup + *iterations
		}
		// Each slot gets its own trees in unique-input mode so no slot warms
		// the filecache for another.
		treesPerSlot := make([][]*inputTree, *concurrency)
		for s := 0; s < *concurrency; s++ {
			for t := 0; t < nTrees; t++ {
				seed := int64(1000*s + t)
				if !*uniqueIn {
					seed = 0 // identical tree for everyone => warm filecache after first fetch
				} else if *seedFlag != 0 {
					seed += *seedFlag * 1_000_000
				} else {
					seed += time.Now().UnixNano() // genuinely cold across runs
				}
				seed = seed*1_000_003 + int64(len(w.Name)) + int64(w.NumFiles)*31 + int64(w.AvgFileBytes)
				tr, err := generateAndUploadTree(ctx, env, w, seed)
				if err != nil {
					log.Fatalf("generate tree: %s", err)
				}
				treesPerSlot[s] = append(treesPerSlot[s], tr)
			}
		}
		log.Infof("uploaded %d trees (%d files, %.1f MB each)", nTrees**concurrency, treesPerSlot[0][0].NumFiles, float64(treesPerSlot[0][0].TotalBytes)/1e6)
		// Discard spans emitted while uploading; only iteration spans are interesting.
		sc.drain()

		var wg sync.WaitGroup
		for s := 0; s < *concurrency; s++ {
			wg.Add(1)
			go func(slot int) {
				defer wg.Done()
				if err := b.runSlot(ctx, w, slot, treesPerSlot[slot]); err != nil {
					log.Errorf("slot %d failed: %s", slot, err)
				}
			}(s)
			// Stagger slot start slightly so the very first iteration doesn't
			// race on image pull / initial snapshot creation.
			if *mode == "recycle" && *warmup > 0 {
				time.Sleep(200 * time.Millisecond)
			}
		}
		wg.Wait()
	}
	out.EndTime = time.Now()
	out.Results = b.results
	out.Spans = sc.drain()
	data, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		log.Fatalf("marshal: %s", err)
	}
	if err := os.WriteFile(*outPath, data, 0644); err != nil {
		log.Fatalf("write: %s", err)
	}
	log.Infof("wrote %s (%d results, %d spans)", *outPath, len(out.Results), len(out.Spans))
	printSummary(out.Results)
	if *metricsOut != "" {
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err == nil {
			var buf bytes.Buffer
			enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
			for _, mf := range mfs {
				_ = enc.Encode(mf)
			}
			_ = os.WriteFile(*metricsOut, buf.Bytes(), 0644)
		}
	}
	env.GetHealthChecker().Shutdown()
}

func hostInfo() map[string]string {
	m := map[string]string{}
	if b, err := os.ReadFile("/proc/version"); err == nil {
		m["kernel"] = strings.TrimSpace(string(b))
	}
	m["cpu_millis"] = strconv.FormatInt(resources.GetAllocatedCPUMillis(), 10)
	if h, err := os.Hostname(); err == nil {
		m["hostname"] = h
	}
	return m
}

func pct(v []int64, p float64) int64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]int64(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(float64(len(s)-1) * p)
	return s[idx]
}

func printSummary(results []iterResult) {
	byWorkload := map[string][]iterResult{}
	var order []string
	for _, r := range results {
		if r.Warmup || r.Err != "" {
			continue
		}
		if _, ok := byWorkload[r.Workload]; !ok {
			order = append(order, r.Workload)
		}
		byWorkload[r.Workload] = append(byWorkload[r.Workload], r)
	}
	for _, w := range order {
		rs := byWorkload[w]
		stageNames := map[string]bool{}
		for _, r := range rs {
			for k := range r.Stages {
				stageNames[k] = true
			}
		}
		names := make([]string, 0, len(stageNames))
		for k := range stageNames {
			names = append(names, k)
		}
		sort.Strings(names)
		fmt.Printf("\n== %s (%d ok iterations) ==\n", w, len(rs))
		fmt.Printf("%-20s %10s %10s %10s %10s\n", "stage", "p50 ms", "p90 ms", "max ms", "mean ms")
		row := func(name string, get func(r iterResult) int64) {
			var v []int64
			var sum int64
			for _, r := range rs {
				x := get(r)
				v = append(v, x)
				sum += x
			}
			fmt.Printf("%-20s %10.1f %10.1f %10.1f %10.1f\n", name, float64(pct(v, .5))/1000, float64(pct(v, .9))/1000, float64(pct(v, 1))/1000, float64(sum)/float64(len(v))/1000)
		}
		for _, n := range names {
			n := n
			row(n, func(r iterResult) int64 { return r.Stages[n] })
		}
		row("TOTAL_PREP", func(r iterResult) int64 { return r.TotalPrepUS })
		row("TOTAL", func(r iterResult) int64 { return r.TotalUS })
	}
}
