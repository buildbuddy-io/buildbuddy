// Package git_fetch runs Git fetch commands with transfer metrics and slow
// transfer retries.
package git_fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_trace2"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/docker/go-units"
)

const (
	// retryDelay is the pause between consecutive slow fetch attempts.
	retryDelay = time.Second
	// rateSamplePeriod controls how often an active pack download is measured.
	rateSamplePeriod = time.Second
	// packTraceFileDescriptor is the descriptor assigned to the first file in
	// exec.Cmd.ExtraFiles.
	packTraceFileDescriptor = 3
)

// ErrSlowFetchDetected indicates that a fetch attempt was canceled because
// its receive rate remained below the configured threshold.
var ErrSlowFetchDetected = errors.New("slow Git fetch detected")

// Options controls slow fetch detection and retries.
type Options struct {
	// Output receives command output and status messages. If it is nil, output
	// is discarded.
	Output io.Writer
	// SlowRateBytesPerSecond is the receive rate below which a fetch is
	// considered slow.
	SlowRateBytesPerSecond int64
	// SlowTimeout is how long the receive rate must remain slow before an
	// attempt is canceled.
	SlowTimeout time.Duration
	// Retries is the number of additional attempts allowed after a slow fetch.
	// A value of zero disables slow fetch detection.
	Retries int
	// WriteStatus writes human-readable retry status. If it is nil, status is
	// written directly to Output.
	WriteStatus func(format string, args ...any)
}

// Result describes all attempts made for a Git fetch command.
type Result struct {
	// Output is the command output returned by the final attempt.
	Output string
	// Err is nil on success, ErrSlowFetchDetected if slow-fetch retries were
	// exhausted, the final command error, or the context error if cancellation
	// occurred while waiting to retry.
	Err error
	// TotalBytes is the sum of pack bytes received across all attempts.
	TotalBytes int64
	// Duration is the total time spent executing fetch attempts.
	Duration time.Duration
}

// CommandRunner executes Git with the supplied environment additions and
// extra files. It must pass extraFiles to exec.Cmd.ExtraFiles in the same order.
type CommandRunner func(ctx context.Context, out io.Writer, env map[string]string, extraFiles []*os.File, args ...string) (string, error)

type runOptions struct {
	Options
	retryDelay       time.Duration
	rateSamplePeriod time.Duration
}

// Run executes a Git fetch command, records transfer metrics, and retries
// attempts whose receive rate remains below the configured threshold.
func Run(ctx context.Context, opts Options, command CommandRunner, args ...string) Result {
	return run(ctx, runOptions{
		Options:          opts,
		retryDelay:       retryDelay,
		rateSamplePeriod: rateSamplePeriod,
	}, command, args...)
}

func run(ctx context.Context, opts runOptions, command CommandRunner, args ...string) Result {
	if opts.Output == nil {
		opts.Output = io.Discard
	}
	slowFetchDetectionEnabled := opts.Retries > 0 && opts.SlowTimeout > 0 && opts.SlowRateBytesPerSecond > 0
	result := Result{}

	for attempt := 0; ; attempt++ {
		attemptResult, attemptErr := runAttempt(ctx, opts, slowFetchDetectionEnabled, command, args...)
		result.Output = attemptResult.Output
		result.Err = attemptErr
		result.TotalBytes += attemptResult.TotalBytes
		result.Duration += attemptResult.Duration

		// A fetch that completed successfully does not need to be retried, even
		// if the slow-fetch timer fired at nearly the same time.
		if attemptErr == nil || !errors.Is(attemptErr, ErrSlowFetchDetected) {
			return result
		}

		writeStatus(opts, "Git fetch receive rate remained below %s/s for %s.", units.BytesSize(float64(opts.SlowRateBytesPerSecond)), opts.SlowTimeout)
		if attempt >= opts.Retries {
			writeStatus(opts, "Git fetch retries exhausted after repeated slow transfers.")
			return result
		}

		writeStatus(opts, "Retrying Git fetch in %s (retry %d of %d)...", opts.retryDelay, attempt+1, opts.Retries)
		timer := time.NewTimer(opts.retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Err = ctx.Err()
			return result
		case <-timer.C:
		}
	}
}

func writeStatus(opts runOptions, format string, args ...any) {
	if opts.WriteStatus != nil {
		opts.WriteStatus(format, args...)
		return
	}
	_, _ = fmt.Fprintf(opts.Output, format+"\n", args...)
}

func runAttempt(ctx context.Context, opts runOptions, slowFetchDetectionEnabled bool, command CommandRunner, args ...string) (*Result, error) {
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()

	// Trace2 identifies the Receiving objects region in real time, but its
	// total_bytes event is emitted only when that region completes. Use a Unix
	// socket so region lifecycle events and completed byte totals can be
	// processed as Git emits them.
	processor := newTraceProcessor(opts, slowFetchDetectionEnabled, cancelAttempt)
	trace2Server, err := git_trace2.NewServer(processor.observeTrace2Event)
	if err != nil {
		log.Warningf("Could not start the git trace2 event listener; git fetch stats and slow fetch detection may be unavailable: %s", err)
	}

	env := map[string]string{}
	if trace2Server != nil {
		env["GIT_TRACE2_EVENT"] = trace2Server.Target()
		// Progress data events can be nested inside other Trace2 regions. Raise
		// the nesting limit (default 2) so they are not dropped.
		env["GIT_TRACE2_EVENT_NESTING"] = "5"
	}

	var extraFiles []*os.File
	var packTraceReader *os.File
	var packTraceWriter *os.File
	var packTraceDone chan struct{}
	if slowFetchDetectionEnabled && trace2Server != nil {
		// GIT_TRACE_PACKFILE copies the received pack bytes as Git reads them.
		// Count this private pipe in real time instead of persisting repository
		// contents or parsing Git's human-readable progress output.
		reader, writer, err := os.Pipe()
		if err != nil {
			log.Warningf("Could not create the git pack trace pipe; slow fetch detection will be unavailable: %s", err)
		} else {
			packTraceReader = reader
			packTraceWriter = writer
			packTraceDone = make(chan struct{})
			env["GIT_TRACE_PACKFILE"] = strconv.Itoa(packTraceFileDescriptor)
			// os/exec maps the first ExtraFiles entry to descriptor 3 in the
			// child process.
			extraFiles = []*os.File{packTraceWriter}
			go func() {
				defer close(packTraceDone)
				defer packTraceReader.Close()
				if _, err := io.Copy(processor, packTraceReader); err != nil && attemptCtx.Err() == nil {
					log.Warningf("Could not read the git pack trace; slow fetch metrics may be undercounted: %s", err)
				}
			}()
		}
	}

	fetchStart := time.Now()
	output, fetchErr := command(attemptCtx, opts.Output, env, extraFiles, args...)
	duration := time.Since(fetchStart)
	// Once the command has returned, a rate sample must not retroactively mark
	// the completed attempt as timed out while its trace streams are drained.
	processor.commandFinished()

	if packTraceWriter != nil {
		_ = packTraceWriter.Close()
		select {
		case <-packTraceDone:
		case <-attemptCtx.Done():
			// A canceled Git process may leave descriptor 3 open in a child.
			// Close the read end so the copy goroutine cannot block shutdown.
			_ = packTraceReader.Close()
			<-packTraceDone
		}
	}
	if trace2Server != nil {
		trace2Server.Stop()
	}
	totalBytes, slowFetchErr := processor.stop()

	result := &Result{
		Output:     output,
		TotalBytes: totalBytes,
		Duration:   duration,
	}
	if fetchErr == nil {
		return result, nil
	}
	if slowFetchErr != nil {
		return result, slowFetchErr
	}
	return result, fetchErr
}

type traceProcessor struct {
	mu sync.Mutex

	activeReceiving  map[string]struct{}
	trace2TotalBytes int64
	packTotalBytes   int64

	slowFetchDetectionEnabled bool
	slowRateBytesPerSecond    int64
	slowTimeout               time.Duration
	rateSamplePeriod          time.Duration
	cancel                    context.CancelFunc

	sampling        bool
	lastSampleTime  time.Time
	lastSampleBytes int64
	slowSince       time.Time
	timedOut        bool
	stopped         bool

	monitorStop chan struct{}
	monitorDone chan struct{}
	now         func() time.Time
}

func newTraceProcessor(opts runOptions, slowFetchDetectionEnabled bool, cancel context.CancelFunc) *traceProcessor {
	samplePeriod := opts.rateSamplePeriod
	if samplePeriod <= 0 {
		samplePeriod = rateSamplePeriod
	}
	p := &traceProcessor{
		activeReceiving:           make(map[string]struct{}),
		slowFetchDetectionEnabled: slowFetchDetectionEnabled,
		slowRateBytesPerSecond:    opts.SlowRateBytesPerSecond,
		slowTimeout:               opts.SlowTimeout,
		rateSamplePeriod:          samplePeriod,
		cancel:                    cancel,
		monitorStop:               make(chan struct{}),
		monitorDone:               make(chan struct{}),
		now:                       time.Now,
	}
	go p.monitorRate()
	return p
}

func (p *traceProcessor) monitorRate() {
	defer close(p.monitorDone)
	ticker := time.NewTicker(p.rateSamplePeriod)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			p.sampleRate(now)
		case <-p.monitorStop:
			return
		}
	}
}

// Write counts raw pack bytes as Git receives them.
func (p *traceProcessor) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.packTotalBytes += int64(len(data))
	if p.slowFetchDetectionEnabled && len(p.activeReceiving) > 0 && !p.sampling {
		p.startSamplingLocked(p.now())
	}
	return len(data), nil
}

func (p *traceProcessor) observeTrace2Event(event git_trace2.Event) {
	if event.Type == "data" && event.Category == "progress" && event.Key == "total_bytes" {
		value := strings.Trim(string(event.Value), "\"")
		n, err := strconv.ParseInt(value, 10, 64)
		if err == nil && n >= 0 {
			p.mu.Lock()
			p.trace2TotalBytes += n
			p.mu.Unlock()
		}
		return
	}
	if event.Category != "progress" || event.Label != "Receiving objects" {
		return
	}

	key := event.SessionID + "\x00" + event.Thread
	p.mu.Lock()
	defer p.mu.Unlock()
	switch event.Type {
	case "region_enter":
		p.activeReceiving[key] = struct{}{}
		if p.slowFetchDetectionEnabled && p.packTotalBytes > 0 && !p.sampling {
			p.startSamplingLocked(p.now())
		}
	case "region_leave":
		delete(p.activeReceiving, key)
		if len(p.activeReceiving) == 0 {
			p.sampling = false
			p.slowSince = time.Time{}
		}
	}
}

func (p *traceProcessor) startSamplingLocked(now time.Time) {
	p.sampling = true
	p.lastSampleTime = now
	p.lastSampleBytes = p.packTotalBytes
	p.slowSince = time.Time{}
}

func (p *traceProcessor) sampleRate(now time.Time) {
	p.mu.Lock()
	if p.stopped || p.timedOut || !p.sampling {
		p.mu.Unlock()
		return
	}
	elapsed := now.Sub(p.lastSampleTime)
	if elapsed <= 0 {
		p.mu.Unlock()
		return
	}
	bytesReceived := p.packTotalBytes - p.lastSampleBytes
	sampleStart := p.lastSampleTime
	p.lastSampleTime = now
	p.lastSampleBytes = p.packTotalBytes
	rateBytesPerSecond := float64(bytesReceived) / elapsed.Seconds()
	if rateBytesPerSecond >= float64(p.slowRateBytesPerSecond) {
		p.slowSince = time.Time{}
		p.mu.Unlock()
		return
	}
	if p.slowSince.IsZero() {
		p.slowSince = sampleStart
	}
	if now.Sub(p.slowSince) < p.slowTimeout {
		p.mu.Unlock()
		return
	}
	p.timedOut = true
	p.mu.Unlock()
	p.cancel()
}

func (p *traceProcessor) commandFinished() {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
}

func (p *traceProcessor) stop() (int64, error) {
	p.commandFinished()
	close(p.monitorStop)
	<-p.monitorDone

	p.mu.Lock()
	defer p.mu.Unlock()
	totalBytes := p.trace2TotalBytes
	if p.packTotalBytes > totalBytes {
		totalBytes = p.packTotalBytes
	}
	if p.timedOut {
		return totalBytes, ErrSlowFetchDetected
	}
	return totalBytes, nil
}
