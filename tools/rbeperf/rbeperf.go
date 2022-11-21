// rbeperf is a tool to measure Remote Execution Build performance.
//
// Example usage:
//
//	$ blaze run //tools/rbeperf -- \
//	  --server=grpcs://remote.buildbuddy.dev \
//	  --api_key=XXX \
//	  --workload basic_sequential
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbeclient"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/histogram"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"github.com/mattn/go-shellwords"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/push"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	server             = flag.String("server", "", "BuildBuddy server target")
	pushGateway        = flag.String("push_gateway", "", "Optional address of Prometheus Pushgateway for recording results. Results are only reported for predefined workloads.")
	apiKey             = flag.String("api_key", "", "API key to use")
	remoteInstanceName = flag.String("remote_instance_name", "", "Remote instance name to set in RBE RPCs.")
	summary            = flag.Bool("summary", true, "Whether to show summary at the end of the run.")
	verbose            = flag.Bool("verbose", false, "Print detailed information for each executed command")
	rawResultsDir      = flag.String("raw_results_dir", "", "If specified, raw per-command data will be written as a CSV file into this directory.")

	prepareConcurrency      = flag.Int("prepare_concurrency", 100, "Number of workers to start to prepare commands to be executed")
	commandAcceptTimeout    = flag.Duration("command_accept_timeout", 30*time.Second, "Amount of time to wait for server to accept a command before considering an execution to be failed.")
	commandExecutionTimeout = flag.Duration("command_execution_timeout", 240*time.Second, "Amount of time to wait before considering an execution to be failed.")
	commandPlatformOS       = flag.String("command_platform_os", "", "Override platform OS property.")
	commandPlatformPool     = flag.String("command_platform_pool", "", "Override platform Pool property.")

	// Workload flags.
	workloadName               = flag.String("workload", "", "Name of a predefined workload to execute. If not specified the workload_* flags will be used to perform the test.")
	workloadCommand            = flag.String("workload_command", `sh -c "echo hello"`, "Command to execute.")
	workloadNumCommands        = flag.Int("workload_num_commands", 0, "Number of total commands to execute")
	workloadConcurrentCommands = flag.Int("workload_concurrent_commands", 0, "Number of commands to execute concurrently")
	workloadRampUpAmount       = flag.Int("workload_ramp_up_amount", 0, "Every ramp up interval, the number of concurrent commands will be increased by this amount until the target concurrency is reached. If not specified defaults to (concurrent commands / 4)")
	workloadRampUpInterval     = flag.Duration("workload_ramp_up_interval", 5*time.Second, "The number of concurrent commands is increased this often until the target concurrency is reached.")
	workloadNumFilesPerCommand = flag.Int("workload_num_files_per_command", 0, "Number of files per command.")
	workloadFileSizeBytes      = flag.Int("workload_file_size_bytes", 0, "Input file size in bytes.")
)

const (
	workloadLabel   = "workload"
	stageLabel      = "stage"
	percentileLabel = "percentile"
	promNamespace   = "rbeperf"
)

var (
	RemoteStatsMillis = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Name:      "remote_stats_millis",
	}, []string{workloadLabel, stageLabel, percentileLabel})
	LocalStatsMillis = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Name:      "local_stats_millis",
	}, []string{workloadLabel, stageLabel, percentileLabel})
	NumCommands = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Name:      "num_commands",
	}, []string{workloadLabel})
	NumFailedCommands = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Name:      "num_failed_commands",
	}, []string{workloadLabel})
	LastSuccessfulRun = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Name:      "last_successful_run",
	}, []string{workloadLabel})
)

type workloadSpec struct {
	command            string
	numCommands        int
	concurrentCommands int
	rampUpAmount       int
	rampUpInterval     time.Duration
	numFilesPerCommand int
	fileSizeBytes      int
}

var predefinedWorkloads = map[string]workloadSpec{
	// No concurrency.
	"basic_sequential": {
		command:            `sh -c "echo hello"`,
		numCommands:        200,
		concurrentCommands: 1,
		numFilesPerCommand: 0,
		fileSizeBytes:      0,
		rampUpAmount:       1,
		rampUpInterval:     1 * time.Second,
	},
	// Similar to a TensorFlow build with large # of jobs.
	"basic_2k_concurrent": {
		command:            `sh -c "echo hello"`,
		numCommands:        20_000,
		concurrentCommands: 2000,
		numFilesPerCommand: 0,
		fileSizeBytes:      0,
		rampUpAmount:       500,
		rampUpInterval:     5 * time.Second,
	},
}

type runner struct {
	gRPCClientSource rbeclient.GRPCClientSource
	rbeClient        *rbeclient.Client
	workload         workloadSpec

	mu           sync.Mutex
	numFinished  int
	numExecuting int
	numErrors    int
}

func newRunner(gRPCClientSource rbeclient.GRPCClientSource, workload workloadSpec) *runner {
	return &runner{
		gRPCClientSource: gRPCClientSource,
		rbeClient:        rbeclient.New(gRPCClientSource),
		workload:         workload,
	}
}

func (g *runner) startCommand(ctx context.Context, cmd *rbeclient.Command, executionUpdates chan *rbeclient.CommandResult) {
	go func() {
		log.Debugf("Starting command %q.", cmd.Name)
		if err := cmd.Start(ctx, &rbeclient.StartOpts{}); err != nil {
			executionUpdates <- &rbeclient.CommandResult{
				Stage:       repb.ExecutionStage_COMPLETED,
				CommandName: cmd.Name,
				Err:         status.UnknownErrorf("command %q could not be executed: %v", cmd.Name, err),
			}
		}

		opName := ""
		timeout := time.NewTimer(*commandAcceptTimeout)
		accepted := false
		lastStage := repb.ExecutionStage_UNKNOWN
		for {
			select {
			case res := <-cmd.StatusChannel():
				opName = res.ID
				if lastStage != res.Stage && (res.Stage == repb.ExecutionStage_EXECUTING || res.Stage == repb.ExecutionStage_COMPLETED) {
					executionUpdates <- res
				}
				lastStage = res.Stage
				if res.Stage == repb.ExecutionStage_COMPLETED {
					return
				}
				if !accepted {
					accepted = true
					timeout.Reset(*commandExecutionTimeout)
				}
			case <-timeout.C:
				executionUpdates <- &rbeclient.CommandResult{
					Stage:       repb.ExecutionStage_COMPLETED,
					CommandName: cmd.Name,
					ID:          opName,
					Err:         status.DeadlineExceededErrorf("command %q was not accepted within %s", cmd.Name, *commandAcceptTimeout),
				}
				return
			}
		}
	}()
}

func addEmptyFileWithRandomName(dir *repb.Directory) error {
	randomFilename, err := random.RandomString(20)
	if err != nil {
		return err
	}
	dir.Files = append(dir.Files, &repb.FileNode{
		Name: randomFilename,
		Digest: &repb.Digest{
			Hash: digest.EmptySha256,
		},
	})
	return nil
}

func addFileWithRandomContent(ctx context.Context, byteStreamClient bspb.ByteStreamClient, dir *repb.Directory) error {
	data := make([]byte, *workloadFileSizeBytes)
	rand.Read(data)
	retry := 0
	retrySleep := 1 * time.Second
	var d *repb.Digest
	var err error
	for {
		reader := bytes.NewReader(data)
		d, err = cachetools.UploadBlob(ctx, byteStreamClient, *remoteInstanceName, reader)
		if err == nil {
			break
		}

		retry++
		if retry == 3 {
			return status.UnavailableErrorf("could not upload input file: %s", err)
		}
		log.Warningf("Upload failed, will retry: %s", err)
		time.Sleep(retrySleep)
		retrySleep *= 2
	}
	randomFilename, err := random.RandomString(20)
	if err != nil {
		return err
	}
	node := &repb.FileNode{
		Name:   randomFilename,
		Digest: d,
	}
	dir.Files = append(dir.Files, node)
	return nil
}

func prepareCommand(ctx context.Context, rbeClient *rbeclient.Client, byteStreamClient bspb.ByteStreamClient, commandNumber int, commandArgs []string) (*rbeclient.Command, error) {
	name := fmt.Sprintf("command %03d", commandNumber)
	log.Debugf("Preparing command %q", name)

	dir := &repb.Directory{}
	// Include a randomly-named empty file to force cache miss.
	if err := addEmptyFileWithRandomName(dir); err != nil {
		return nil, status.UnknownErrorf("could not add empty file with random name: %s", err)
	}

	for f := 0; f < *workloadNumFilesPerCommand; f++ {
		if err := addFileWithRandomContent(ctx, byteStreamClient, dir); err != nil {
			return nil, status.UnavailableErrorf("could not input file with random content: %s", err)
		}
	}

	log.Debugf("Uploading input root for %q.", name)

	inputRootDigest, err := cachetools.UploadProto(ctx, byteStreamClient, *remoteInstanceName, dir)
	if err != nil {
		return nil, status.UnavailableErrorf("could not upload directory descriptor: %s", err)
	}

	platformProps := []*repb.Platform_Property{
		{Name: "container-image", Value: "none"},
	}
	if *commandPlatformOS != "" {
		platformProps = append(platformProps, &repb.Platform_Property{Name: "OSFamily", Value: *commandPlatformOS})
	}
	if *commandPlatformPool != "" {
		platformProps = append(platformProps, &repb.Platform_Property{Name: "Pool", Value: *commandPlatformPool})
	}
	command := &repb.Command{
		Arguments: commandArgs,
		Platform:  &repb.Platform{Properties: platformProps},
	}
	cmd, err := rbeClient.PrepareCommand(ctx, *remoteInstanceName, name, inputRootDigest, command, 0 /*=timeout*/)
	if err != nil {
		return nil, status.UnknownErrorf("could not prepare command %q: %v", name, err)
	}
	return cmd, nil
}

func (g *runner) prepareCommands(ctx context.Context) ([]*rbeclient.Command, error) {
	cmdArgs, err := shellwords.Parse(g.workload.command)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse command: %s", err)
	}
	log.Infof("Command to run: [%s]", strings.Join(cmdArgs, ", "))

	log.Infof("Preparing %d commands.", g.workload.numCommands)

	startTime := time.Now()

	cmdsToPrep := make(chan int, g.workload.numCommands)
	for i := 0; i < g.workload.numCommands; i++ {
		cmdsToPrep <- i
	}

	cmdChan := make(chan *rbeclient.Command, 10)
	errChan := make(chan error, 10)

	// Start multiple workers to prepare commands.
	for i := 0; i < *prepareConcurrency; i++ {
		go func() {
			for cmdToPrep := range cmdsToPrep {
				cmd, err := prepareCommand(ctx, g.rbeClient, g.gRPCClientSource.GetByteStreamClient(), cmdToPrep, cmdArgs)
				if err != nil {
					errChan <- err
					break
				}
				cmdChan <- cmd
			}
		}()
	}

	var mu sync.Mutex
	var cmds []*rbeclient.Command

	// Start a goroutine to report on progress.
	prepDone := make(chan struct{})
	defer close(prepDone)
	go func() {
		for {
			select {
			case <-prepDone:
				break
			case <-time.After(1 * time.Second):
			}

			mu.Lock()
			numCmdsPrepared := len(cmds)
			mu.Unlock()

			if len(cmds) == g.workload.numCommands {
				break
			}

			log.Infof("Prepared %d out of %d commands.", numCmdsPrepared, g.workload.numCommands)
		}
	}()

	// Wait for the generated commands to roll in.
	for len(cmds) < g.workload.numCommands {
		select {
		case cmd := <-cmdChan:
			mu.Lock()
			cmds = append(cmds, cmd)
			mu.Unlock()
		case err := <-errChan:
			return nil, err
		}
	}

	log.Infof("Finished preparing %d commands in %s.", g.workload.numCommands, time.Now().Sub(startTime))
	return cmds, nil
}

func (g *runner) reportProgress(gen *loadGenerator) {
	g.mu.Lock()
	defer g.mu.Unlock()
	log.Infof("%d out of %d done, %d errors, %d in flight (%d executing).", g.numFinished, g.workload.numCommands, g.numErrors, gen.NumInFlightCommands(), g.numExecuting)
}

func (g *runner) startProgressReporter(done chan struct{}, gen *loadGenerator) {
	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
			case <-done:
				return
			}
			g.reportProgress(gen)
		}
	}()
}

type loadGenerator struct {
	workload workloadSpec
	allCmds  []*rbeclient.Command

	work            chan *rbeclient.Command
	commandFinished chan struct{}

	mu                  sync.Mutex
	numInFlightCommands int
}

func newLoadGenerator(workload workloadSpec, cmds []*rbeclient.Command) *loadGenerator {
	return &loadGenerator{
		workload:        workload,
		allCmds:         cmds,
		work:            make(chan *rbeclient.Command, workload.concurrentCommands),
		commandFinished: make(chan struct{}, 1),
	}
}

func (g *loadGenerator) startMaxConcurrencyRamper(maxConcurrencyChanges chan int) {
	go func() {
		c := 0
		for {
			c += g.workload.rampUpAmount
			if c > g.workload.concurrentCommands {
				c = g.workload.concurrentCommands
			}
			maxConcurrencyChanges <- c
			if c == g.workload.concurrentCommands {
				break
			}
			time.Sleep(g.workload.rampUpInterval)
		}
	}()
}

func (g *loadGenerator) Start() {
	maxConcurrencyChanges := make(chan int)
	g.startMaxConcurrencyRamper(maxConcurrencyChanges)

	go func() {
		maxConcurrentCommands := 0

		for {

			var newWork []*rbeclient.Command
			g.mu.Lock()
			for g.numInFlightCommands < maxConcurrentCommands && len(g.allCmds) > 0 {
				g.numInFlightCommands++
				cmd := g.allCmds[0]
				g.allCmds = g.allCmds[1:]
				newWork = append(newWork, cmd)
			}
			g.mu.Unlock()

			for _, w := range newWork {
				g.work <- w
			}

			select {
			case newMaxConcurrency := <-maxConcurrencyChanges:
				maxConcurrentCommands = newMaxConcurrency
			case <-g.commandFinished:
				g.mu.Lock()
				g.numInFlightCommands--
				g.mu.Unlock()
			}
		}
	}()
}

func (g *loadGenerator) CommandFinished() {
	g.commandFinished <- struct{}{}
}

func (g *loadGenerator) Work() chan *rbeclient.Command {
	return g.work
}

func (g *loadGenerator) NumInFlightCommands() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.numInFlightCommands
}

func (g *runner) executeCommands(ctx context.Context, gen *loadGenerator) []*rbeclient.CommandResult {
	gen.Start()

	executingCommands := make(map[string]struct{})
	executionUpdates := make(chan *rbeclient.CommandResult, 100)
	var results []*rbeclient.CommandResult
	for {
		g.mu.Lock()
		if g.numFinished == g.workload.numCommands {
			g.mu.Unlock()
			break
		}
		g.mu.Unlock()

		select {
		case cmd := <-gen.Work():
			g.startCommand(ctx, cmd, executionUpdates)
		case res := <-executionUpdates:
			log.Debugf("Got result for %q, stage %s", res.CommandName, res.Stage)

			if res.Stage == repb.ExecutionStage_COMPLETED {
				gen.CommandFinished()
				results = append(results, res)
			}

			g.mu.Lock()
			if res.Stage == repb.ExecutionStage_EXECUTING {
				executingCommands[res.CommandName] = struct{}{}
				g.numExecuting++
			} else if res.Stage == repb.ExecutionStage_COMPLETED {
				if _, ok := executingCommands[res.CommandName]; ok {
					g.numExecuting--
					delete(executingCommands, res.CommandName)
				}
				g.numFinished++
				if res.Err != nil {
					// This will happen if the command fails before being assigned an ID.
					id := res.ID
					if id == "" {
						id = "not known"
					}
					log.Warningf("Command %q (ID: %q) did not finish successfully: %v", res.CommandName, id, res.Err)
					g.numErrors++
				}
			}
			g.mu.Unlock()
		}
	}

	return results
}

func (g *runner) prepareAndExecuteCommands(ctx context.Context) ([]*rbeclient.CommandResult, error) {
	log.Info("Preparing commands.")
	cmds, err := g.prepareCommands(ctx)
	if err != nil {
		return nil, err
	}

	log.Info("Starting run.")

	done := make(chan struct{})
	defer close(done)

	gen := newLoadGenerator(g.workload, cmds)
	g.startProgressReporter(done, gen)
	return g.executeCommands(ctx, gen), nil
}

type remoteStats struct {
	fetchInputsDuration   time.Duration
	executeDuration       time.Duration
	uploadOutputsDuration time.Duration
	totalDuration         time.Duration
}

func durationFromTimes(start, end *timestamppb.Timestamp) time.Duration {
	return end.AsTime().Sub(start.AsTime())
}

type rawResultsWriter struct {
	csvFile   *os.File
	csvWriter *csv.Writer
}

func newRawResultsWriter(file string) (*rawResultsWriter, error) {
	f, err := os.Create(file)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	return &rawResultsWriter{csvWriter: w, csvFile: f}, nil
}

func (w *rawResultsWriter) WriteHeader(workload workloadSpec) error {
	if err := w.csvWriter.Write([]string{"num_commands", strconv.Itoa(workload.numCommands)}); err != nil {
		return err
	}
	if err := w.csvWriter.Write([]string{"concurrent_commands", strconv.Itoa(workload.concurrentCommands)}); err != nil {
		return err
	}
	if err := w.csvWriter.Write([]string{"ramp_up_amount", strconv.Itoa(workload.rampUpAmount)}); err != nil {
		return err
	}
	if err := w.csvWriter.Write([]string{"ramp_up_interval", workload.rampUpInterval.String()}); err != nil {
		return err
	}
	if err := w.csvWriter.Write(nil); err != nil {
		return err
	}
	if err := w.csvWriter.Write([]string{
		"command", "executor", "error", "local_execute_rpc_started", "local_time_to_accepted",
		"local_accepted_to_finished", "local_total", "remote_fetch_inputs", "remote_execute", "remote_upload_outputs",
		"remote_total", "id"}); err != nil {
		return err
	}
	return nil
}

func (w *rawResultsWriter) Close() {
	w.csvWriter.Flush()
	w.csvFile.Close()
}

func (w *rawResultsWriter) Write(res *rbeclient.CommandResult, remoteStats *remoteStats) error {
	errorString := ""
	if res.Err != nil {
		errorString = res.Err.Error()
	}

	return w.csvWriter.Write([]string{
		res.CommandName,
		res.Executor,
		errorString,
		strconv.FormatInt(res.LocalStats.ExecuteRPCStarted.Milliseconds(), 10),
		strconv.FormatInt(res.LocalStats.TimeToAccepted.Milliseconds(), 10),
		strconv.FormatInt(res.LocalStats.AcceptedToFinished.Milliseconds(), 10),
		strconv.FormatInt(res.LocalStats.Total.Milliseconds(), 10),
		strconv.FormatInt(remoteStats.fetchInputsDuration.Milliseconds(), 10),
		strconv.FormatInt(remoteStats.executeDuration.Milliseconds(), 10),
		strconv.FormatInt(remoteStats.uploadOutputsDuration.Milliseconds(), 10),
		strconv.FormatInt(remoteStats.totalDuration.Milliseconds(), 10),
		res.ID,
	})
}

func (g *runner) run(ctx context.Context) error {
	results, err := g.prepareAndExecuteCommands(ctx)
	if err != nil {
		return err
	}

	log.Infof("Perf run completed, calculating metrics.")

	histogramOpts := histogram.Options{BucketLabelFormatter: func(min int64, max int64) string {
		return fmt.Sprintf("%d - %d ms", min, max)
	}}

	clientReqHist := histogram.NewWithOptions(histogramOpts)
	clientAcceptHist := histogram.NewWithOptions(histogramOpts)
	clientAcceptToFinishHist := histogram.NewWithOptions(histogramOpts)
	clientTotalHist := histogram.NewWithOptions(histogramOpts)

	remoteInputFetchHist := histogram.NewWithOptions(histogramOpts)
	remoteExecHist := histogram.NewWithOptions(histogramOpts)
	remoteOutputUploadHist := histogram.NewWithOptions(histogramOpts)
	remoteTotalHist := histogram.NewWithOptions(histogramOpts)

	var resultsWriter *rawResultsWriter
	if *rawResultsDir != "" {
		rawResultsFile := filepath.Join(*rawResultsDir, "rbeperf_results_"+time.Now().Format("20060102_150405")+".csv")
		log.Infof("Raw results will be saved to %s", rawResultsFile)
		resultsWriter, err = newRawResultsWriter(rawResultsFile)
		if err != nil {
			return err
		}
		defer resultsWriter.Close()
		err := resultsWriter.WriteHeader(g.workload)
		if err != nil {
			return err
		}
	}

	numErrors := 0
	for _, res := range results {
		remoteStats := &remoteStats{
			fetchInputsDuration:   durationFromTimes(res.RemoteStats.GetInputFetchStartTimestamp(), res.RemoteStats.GetInputFetchCompletedTimestamp()),
			executeDuration:       durationFromTimes(res.RemoteStats.GetExecutionStartTimestamp(), res.RemoteStats.GetExecutionCompletedTimestamp()),
			uploadOutputsDuration: durationFromTimes(res.RemoteStats.GetOutputUploadStartTimestamp(), res.RemoteStats.GetOutputUploadCompletedTimestamp()),
			totalDuration:         durationFromTimes(res.RemoteStats.GetQueuedTimestamp(), res.RemoteStats.GetWorkerCompletedTimestamp()),
		}

		if resultsWriter != nil {
			err := resultsWriter.Write(res, remoteStats)
			if err != nil {
				return err
			}
		}

		if res.Err != nil {
			id := res.ID
			// This will happen if the command fails before being assigned an ID.
			if id == "" {
				id = "not known"
			}
			log.Warningf("Command %q (ID: %q) did not finish successfully: %v", res.CommandName, id, res.Err)
			numErrors++
			continue
		}

		clientReqHist.Add(res.LocalStats.ExecuteRPCStarted.Milliseconds())
		clientTotalHist.Add(res.LocalStats.Total.Milliseconds())
		clientAcceptHist.Add(res.LocalStats.TimeToAccepted.Milliseconds())
		clientAcceptToFinishHist.Add(res.LocalStats.AcceptedToFinished.Milliseconds())

		if res.RemoteStats == nil {
			log.Warningf("Missing remote stats for %q", res.CommandName)
			numErrors++
			continue
		}

		remoteInputFetchHist.Add(remoteStats.fetchInputsDuration.Milliseconds())
		remoteExecHist.Add(remoteStats.executeDuration.Milliseconds())
		remoteOutputUploadHist.Add(remoteStats.uploadOutputsDuration.Milliseconds())
		remoteTotalHist.Add(remoteStats.totalDuration.Milliseconds())
	}

	if *summary {
		fmt.Printf("****** LOCAL STATS ****\n")
		fmt.Printf("Req time:\n%s\n", clientReqHist.String())
		fmt.Printf("Time to accepted:\n%s\n", clientAcceptHist.String())
		fmt.Printf("Time from accepted to finished:\n%s\n", clientAcceptToFinishHist.String())
		fmt.Printf("Time to finish:\n%s\n", clientTotalHist)
		fmt.Printf("\n\n")

		fmt.Printf("****** REMOTE STATS ****\n")
		fmt.Printf("Input fetch time:\n%s\n", remoteInputFetchHist.String())
		fmt.Printf("Execution time:\n%s\n", remoteExecHist.String())
		fmt.Printf("Output upload time:\n%s\n", remoteOutputUploadHist.String())
		fmt.Printf("Total time:\n%s\n", remoteTotalHist.String())
	}

	fillMetric := func(g *prometheus.GaugeVec, h *histogram.Histogram, stage string) {
		p := h.Percentiles()
		g.With(prometheus.Labels{
			workloadLabel:   *workloadName,
			stageLabel:      stage,
			percentileLabel: "50",
		}).Set(float64(p.P50))
		g.With(prometheus.Labels{
			workloadLabel:   *workloadName,
			stageLabel:      stage,
			percentileLabel: "95",
		}).Set(float64(p.P95))
		g.With(prometheus.Labels{
			workloadLabel:   *workloadName,
			stageLabel:      stage,
			percentileLabel: "99",
		}).Set(float64(p.P99))
	}

	if *pushGateway != "" && *workloadName != "" {
		NumCommands.With(prometheus.Labels{workloadLabel: *workloadName}).Set(float64(len(results)))
		NumFailedCommands.With(prometheus.Labels{workloadLabel: *workloadName}).Set(float64(numErrors))
		if numErrors == 0 {
			LastSuccessfulRun.With(prometheus.Labels{workloadLabel: *workloadName}).SetToCurrentTime()
		}

		fillMetric(RemoteStatsMillis, remoteInputFetchHist, "fetch_inputs")
		fillMetric(RemoteStatsMillis, remoteExecHist, "execute")
		fillMetric(RemoteStatsMillis, remoteOutputUploadHist, "upload_outputs")
		fillMetric(RemoteStatsMillis, remoteTotalHist, "total")

		fillMetric(LocalStatsMillis, clientAcceptHist, "accept_command")
		fillMetric(LocalStatsMillis, clientAcceptToFinishHist, "queue_and_execute")
		fillMetric(LocalStatsMillis, clientTotalHist, "total")

		pusher := push.New(*pushGateway, "rbeperf").Gatherer(prometheus.DefaultGatherer)
		if err := pusher.Push(); err != nil {
			return status.UnavailableErrorf("could not push metrics to push gateway: %s", err)
		}
	}

	log.Infof("Total commands executed: %d", len(results))
	if numErrors > 0 {
		log.Warningf("***** THERE WERE %d FAILED COMMANDS *****", numErrors)
	}

	return nil
}

type ClientSource struct {
	byteStreamClient bspb.ByteStreamClient
	executionClient  repb.ExecutionClient
}

func (c *ClientSource) GetRemoteExecutionClient() repb.ExecutionClient {
	return c.executionClient
}

func (c *ClientSource) GetByteStreamClient() bspb.ByteStreamClient {
	return c.byteStreamClient
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	if *verbose {
		*log.LogLevel = "debug"
	}
	*log.IncludeShortFileName = true
	if err := log.Configure(); err != nil {
		log.Fatalf("Could not configure logger: %s", err)
	}

	var workload workloadSpec
	if *workloadName != "" {
		predefinedWorkload, ok := predefinedWorkloads[*workloadName]
		if !ok {
			var keys []string
			for key := range predefinedWorkloads {
				keys = append(keys, key)
			}
			log.Fatalf("%q is not a predefined workload, available workloads: %s", *workloadName, keys)
		}
		workload = predefinedWorkload
	} else {
		log.Warningf("--workload not specified, assuming custom workload")
		if *workloadNumCommands <= 0 {
			log.Fatalf("--workload_num_commands must be a positive number")
		}
		if *workloadConcurrentCommands <= 0 {
			log.Fatalf("--workload_concurrent_commands must be a positive number")
		}
		rampAmount := *workloadRampUpAmount
		if rampAmount == 0 {
			rampAmount = *workloadConcurrentCommands / 4
		}
		if rampAmount < 1 {
			rampAmount = 1
		}
		log.Infof("Ramp up %d commands every %s.", rampAmount, *workloadRampUpInterval)
		workload = workloadSpec{
			command:            *workloadCommand,
			numCommands:        *workloadNumCommands,
			concurrentCommands: *workloadConcurrentCommands,
			numFilesPerCommand: *workloadNumFilesPerCommand,
			fileSizeBytes:      *workloadFileSizeBytes,
			rampUpInterval:     *workloadRampUpInterval,
			rampUpAmount:       rampAmount,
		}
	}

	clientConn, err := grpc_client.DialTargetWithOptions(*server, true, grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		log.Fatalf("Could not connect to server %q: %s", *server, err)
	}

	source := &ClientSource{
		byteStreamClient: bspb.NewByteStreamClient(clientConn),
		executionClient:  repb.NewExecutionClient(clientConn),
	}

	ctx := context.Background()
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	invocationId := uuid.New().String()
	log.Infof("Invocation ID: %s", invocationId)
	md := &repb.RequestMetadata{
		ToolInvocationId: invocationId,
	}
	mdBin, err := proto.Marshal(md)
	if err != nil {
		log.Fatalf("Could not set up metadata: %s", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, bazel_request.RequestMetadataKey, string(mdBin))

	gen := newRunner(source, workload)
	err = gen.run(ctx)
	if err != nil {
		log.Fatalf("Run failed: %s", err)
	}

}
