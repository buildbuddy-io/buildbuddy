package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

var script = `
start=$(date +%s.%N)

for _ in $(seq 1 $JOBS); do
	head -c ` + fmt.Sprint(int64(1e10)) + ` </dev/zero >/dev/null &
done
wait

end=$(date +%s.%N)
echo "$start $end"
`

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	for _, isolation := range []string{"firecracker", "oci"} {
		overprovisionTrials := []int{0}
		if isolation == "firecracker" {
			overprovisionTrials = []int{0, 3, runtime.NumCPU()}
		}
		for _, overprovision := range overprovisionTrials {
			for _, cpuPerTask := range []int{1, 2, 3, 4} {
				for _, parallelTasks := range []int{1, 4, 8, 12, 16} {
					for trial := range 3 {
						if err := runTrial(isolation, overprovision, cpuPerTask, parallelTasks, trial); err != nil {
							return fmt.Errorf("trial failed (isolation=%s, overprovision=%d, taskCPU=%d, jobs=%d, trial=%d)", isolation, overprovision, cpuPerTask, parallelTasks, trial)
						}
						// Wait a little bit for things to be cleaned up
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
		}
	}
	return nil
}

func runTrial(isolation string, overprovision, taskCPU, parallelTasks, trial int) error {
	var eg errgroup.Group
	start := time.Now()
	var mu sync.Mutex
	var execDurations []float64
	for range parallelTasks {
		eg.Go(func() error {
			cmd := exec.Command(
				"bb", "execute",
				"--remote_executor=grpc://localhost:1985",
				"--exec_properties=workload-isolation-type="+isolation,
				"--exec_properties=overprovision-cpu="+fmt.Sprint(overprovision),
				"--exec_properties=EstimatedComputeUnits="+fmt.Sprint(taskCPU),
				"--exec_properties=EstimatedMemory=200MB",
				"--action_env=JOBS="+fmt.Sprint(taskCPU),
				"--",
				"sh", "-c", script,
			)
			var buf bytes.Buffer
			cmd.Stdout = &buf
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return err
			}
			// Parse exec duration from stdout
			fields := strings.Fields(buf.String())
			start, err := strconv.ParseFloat(fields[0], 64)
			if err != nil {
				return err
			}
			end, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return err
			}
			mu.Lock()
			execDurations = append(execDurations, end-start)
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	duration := time.Since(start)
	result := map[string]any{
		"isolation":      isolation,
		"overprovision":  overprovision,
		"task_cpu":       taskCPU,
		"parallel_tasks": parallelTasks,
		"trial":          trial,
		"wall_time":      duration.Seconds(),
		"exec_durations": execDurations,
	}
	jb, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	os.Stdout.Write(jb)
	os.Stdout.WriteString("\n")
	return nil
}
