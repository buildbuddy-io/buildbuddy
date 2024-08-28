package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// Min/max range of task CPU estimates.
	minComputeUnits = 1
	maxComputeUnits = 8

	// Percent of tasks that should use twice the amount of their CPU estimate.
	misbehavingFraction = 0.2
	// EstimatedComputeUnits, as a fraction of how much CPU the task actually needs.
	// If 0, don't set hard-code EstimatedComputeUnits.
	estimateMultiplier = 0
	target             = "grpc://localhost:1985"

	stressTimeout       = 1 * time.Second
	stressTimeoutJitter = 0.5
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type TaskReport struct {
	EstimatedCPU      int           `json:"estimated_cpu"`
	ExecutionDuration time.Duration `json:"execution_duration"`
	CPUStressors      int           `json:"cpu_stressors"`
	CPUSomeStallUsec  int64         `json:"cpu_some_stall_usec"`
	CPUFullStallUsec  int64         `json:"cpu_full_stall_usec"`
}

func run() error {
	ctx := context.Background()
	env := real_environment.NewBatchEnv()
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))

	reports := make(chan *TaskReport, 1024)
	go func() {
		var totalCount, wellBehavedCount, totalSomeStallUsec, totalFullStallUsec, totalExecutionUsec int64
		var someStallObservations []float64

		for report := range reports {
			// jb, _ := json.Marshal(report)
			// fmt.Println(string(jb))

			// Count up the number of well-behaved tasks and report their
			// average CPU PSI
			totalCount += 1
			if report.EstimatedCPU == report.CPUStressors {
				wellBehavedCount += 1
				totalSomeStallUsec += report.CPUSomeStallUsec
				totalFullStallUsec += report.CPUFullStallUsec
				totalExecutionUsec += report.ExecutionDuration.Microseconds()

				someStallPercent := float64(totalSomeStallUsec) / float64(totalExecutionUsec) * 100
				fullStallPercent := float64(totalFullStallUsec) / float64(totalExecutionUsec) * 100
				someStallObservations = append(someStallObservations, someStallPercent)
				clear()
				printHistogram(someStallObservations, 80)
				fmt.Printf("Avg stall%% for %d of %d well-behaved tasks: some=%.2f%%\tfull=%.2f%%\n", wellBehavedCount, totalCount, someStallPercent, fullStallPercent)
			}
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(128)
	for ctx.Err() == nil {
		eg.Go(func() error {
			computeUnits := rand.IntN(maxComputeUnits-minComputeUnits+1) + minComputeUnits
			props := []string{
				"container-image=docker://gcr.io/flame-public/stress-ng",
				"dockerNetwork=off",
			}
			if estimateMultiplier > 0 {
				est := fmt.Sprintf("%d", int64(float64(computeUnits)*estimateMultiplier))
				props = append(props, "EstimatedComputeUnits="+est)
			}
			plat, err := rexec.MakePlatform(props...)
			if err != nil {
				return fmt.Errorf("make platform: %w", err)
			}

			n := 1
			// Occasionally, run a misbehaving action that uses significantly
			// more CPU than what was estimated.
			if rand.Float64() < misbehavingFraction {
				n = 2
			}

			timeout := time.Duration(float64(stressTimeout) * (1 + stressTimeoutJitter*(rand.Float64()*2-1)))

			rn, err := rexec.Prepare(ctx, env, "", repb.DigestFunction_SHA256, &repb.Action{
				DoNotCache: true,
				Timeout:    durationpb.New(timeout),
			}, &repb.Command{
				Arguments: []string{"sh", "-ec", `
					for _ in $(seq 1 $N); do
						stress-ng --cpu=` + fmt.Sprint(computeUnits) + ` &
					done
					wait
				`},
				Platform: plat,
			}, "")
			if err != nil {
				return fmt.Errorf("prepare action: %w", err)
			}
			// Set N as a remote header override to bypass the task sizer.
			ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-platform.env-overrides", fmt.Sprintf("N=%d", n))

			stream, err := rexec.Start(ctx, env, rn)
			if err != nil {
				return fmt.Errorf("start execution: %w", err)
			}
			rsp, err := rexec.Wait(stream)
			if err != nil {
				return fmt.Errorf("wait execution: %w", err)
			}
			if rsp.Err != nil && !status.IsDeadlineExceededError(rsp.Err) {
				return fmt.Errorf("execution error: %w", rsp.Err)
			}
			res := rsp.ExecuteResponse.GetResult()
			if res.GetExitCode() != 0 && !status.IsDeadlineExceededError(rsp.Err) {
				out, err := rexec.GetResult(ctx, env, "", repb.DigestFunction_SHA256, res)
				if err != nil {
					return fmt.Errorf("fetch outputs: %w", err)
				}
				os.Stderr.Write(out.Stderr)
				return fmt.Errorf("execution failed: exit code %d", res.GetExitCode())
			}

			md := res.GetExecutionMetadata()
			execDuration := md.GetExecutionCompletedTimestamp().AsTime().Sub(md.GetExecutionStartTimestamp().AsTime())
			stats := md.GetUsageStats()

			report := &TaskReport{
				EstimatedCPU:      computeUnits,
				CPUStressors:      computeUnits * n,
				ExecutionDuration: execDuration,
				CPUSomeStallUsec:  stats.GetCpuPressure().GetSome().GetTotal(),
				CPUFullStallUsec:  stats.GetCpuPressure().GetFull().GetTotal(),
			}
			reports <- report
			return nil
		})
	}
	return eg.Wait()
}

const barChar = '█'

func minMax(vals []float64) (min, max float64) {
	min, max = vals[0], vals[0]
	for _, val := range vals {
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
	}
	return
}

func clear() {
	os.Stdout.WriteString("\033[2J\033[H")
}

// printHistogram prints the histogram to the terminal.
func printHistogram(data []float64, widthChars int) {
	if len(data) == 0 {
		fmt.Println("No data to display.")
		return
	}

	// We're rendering percentages - configure buckets accordingly
	const (
		numBuckets = 25
		minVal     = 0.0
		maxVal     = 50.0
		rangeSize  = (maxVal - minVal) / numBuckets
	)
	buckets := make([]int, numBuckets)

	// Bucket the data.
	for _, value := range data {
		bucketIndex := 0
		if rangeSize > 0 {
			bucketIndex = int((value - minVal) / rangeSize)
		}
		if bucketIndex == numBuckets {
			bucketIndex-- // Fix edge case where the maximum value falls on the upper boundary
		}
		buckets[bucketIndex]++
	}

	// Find the maximum count in the buckets for scaling purposes.
	var maxCount int
	for _, count := range buckets {
		if count > maxCount {
			maxCount = count
		}
	}
	// Block character set: index i = i/8 of a full block, from the left.
	blockChars := []rune{
		' ',
		'▏',
		'▎',
		'▍',
		'▌',
		'▋',
		'▊',
		'▉',
		'█',
	}
	// Scale buckets so that we don't exceed the desired max width.
	// The unicode bar characters can render with a resolution of 8 ticks
	// per terminal column, so we use "eighths" as the unit here.

	widthLimitEighths := widthChars * 8
	scale := float64(1)
	if maxCount > widthLimitEighths {
		scale = float64(widthLimitEighths) / float64(maxCount)
	}

	// Print the histogram chart horizontally.
	for i := 0; i < numBuckets; i++ {
		lowerBound := minVal + float64(i)*rangeSize
		upperBound := lowerBound + rangeSize
		if i == numBuckets-1 {
			upperBound = maxVal // ensure the upper bound includes the max value
		}

		// Print the range.
		fmt.Printf("%6.2f - %6.2f ", lowerBound, upperBound)

		// Print the histogram bars.
		printedWidth := 0
		bucketWidth := float64(buckets[i]) * scale
		for float64(printedWidth) < bucketWidth {
			w := min(int(bucketWidth)-printedWidth, 8)
			if w == 0 {
				break
			}
			r := blockChars[int(w)]
			fmt.Print(string(r))
			printedWidth += w
		}

		// Print the count.
		fmt.Printf(" %d\n", buckets[i])
	}
}
