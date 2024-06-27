package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/creack/pty"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"io"
	"os"
	"os/exec"
	"regexp"
	"time"
)

var (
	testCommand = flag.String("cmd", "build //...", "Bazel command to run with remote bazel")
	apiKey      = flag.String("apiKey", "", "API key to authenticate the build")
	numRuns     = flag.Int("n", 1, "Number of times to run the bazel command")
	verbose     = flag.Bool("verbose", false, "Whether to log remote bazel output")
)

func main() {
	flag.Parse()

	//wg := sync.WaitGroup{}
	//wg.Add(2)
	//go func() {
	runTrial(true)
	//	wg.Done()
	//}()
	//go func() {
	runTrial(false)
	//	wg.Done()
	//}()
	//wg.Wait()

}

func runTrial(useBareRunner bool) {
	runner := "grpcs://remote.buildbuddy.dev"
	if useBareRunner {
		runner = "grpcs://cloud.metal.buildbuddy.dev"
	}

	results := "Cloud results:\n"
	if useBareRunner {
		results = "Bare runner results:\n"
	}
	for i := 0; i < *numRuns; i++ {
		guid, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf(err.Error())
		}
		randomID := guid.String()

		resultCleanRunner := triggerRemoteRun(runner, randomID)
		resultRecycledRunner := triggerRemoteRun(runner, randomID)

		results += fmt.Sprintf("[Run %d - Clean runner] %s", i+1, resultCleanRunner)
		results += fmt.Sprintf("[Run %d - Recycled runner] %s", i+1, resultRecycledRunner)
	}

	fmt.Println(results)
}

func triggerRemoteRun(runnerTarget string, runID string) string {
	startTime := time.Now()
	output, err := runCommand(fmt.Sprintf(
		"bb remote --remote_runner=%s --runner_exec_properties=instance_name=%s %s  --remote_header=x-buildbuddy-api-key=%s",
		runnerTarget, runID, *testCommand, *apiKey))
	if err != nil {
		log.Fatalf(err.Error())
	}
	endTime := time.Now()

	// Find first timestamp from remote run
	pattern := `2024-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} UTC`
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Fatalf(err.Error())
	}

	firstRemoteTimestamp := re.FindString(output)
	if firstRemoteTimestamp == "" {
		log.Fatalf("No remote timestamps found in output")
	}
	firstRemoteTimestampParsed, err := time.Parse("2006-01-02 15:04:05.000 MST", firstRemoteTimestamp)
	if err != nil {
		log.Fatalf("Could not parse timestamp %s: %s", firstRemoteTimestamp, err.Error())
	}

	remoteRunDuration := endTime.Sub(firstRemoteTimestampParsed)
	completeRunDuration := endTime.Sub(startTime)

	results := fmt.Sprintf("Remote run duration: %v min\n", remoteRunDuration.Minutes())
	results += fmt.Sprintf("Complete run duration (including queuing): %v min\n", completeRunDuration.Minutes())
	return results
}

func runCommand(run string) (string, error) {
	var buf bytes.Buffer
	out := io.Discard
	if *verbose {
		out = os.Stdout
	}
	w := io.MultiWriter(out, &buf)

	args, err := shlex.Split(run)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = os.Environ()
	cmd.Dir = "/Users/maggielou/bb/buildbuddy"
	size := &pty.Winsize{Rows: uint16(20), Cols: uint16(114)}
	f, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return "", err
	}
	defer f.Close()
	copyOutputDone := make(chan struct{})
	go func() {
		io.Copy(w, f)
		copyOutputDone <- struct{}{}
	}()
	err = cmd.Wait()
	<-copyOutputDone

	return buf.String(), err
}
