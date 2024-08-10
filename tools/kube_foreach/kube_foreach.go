package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/sync/errgroup"
)

var (
	deployment = flag.String("deployment", "executor", "Deployment to target. e.g. executor-workflows, executor-bare")
	container  = flag.String("container", "executor", "Container within the pod to run commands in.")
	namespace  = flag.String("namespace", "executor-dev", "Namespace to target.")
	output     = flag.String("output", "json", "Output format: `json|text`.")
	jobs       = flag.Int("jobs", 16, "How many commands to run concurrently.")
)

func run() error {
	flag.Usage = func() {
		fmt.Fprintf(
			flag.CommandLine.Output(),
			"Usage: kube_foreach [flags...] <executable> [args...]\n\n"+
				"Run a command across all pods in a deployment.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("missing <executable> [args...]")
	}

	os.Setenv("DEPLOYMENT", *deployment)
	os.Setenv("NAMESPACE", *namespace)

	// List pods
	var pods []string
	{
		cmd := exec.Command("sh", "-ec", `
			NSARGS=""
			if [ -n "$NAMESPACE" ]; then NSARGS="-n $NAMESPACE"; fi
			kubectl get pods -o json $NSARGS -l app="$DEPLOYMENT" |
				jq -r '.items[] | .metadata.name'
		`)
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Printf("Error listing pods: %s", err)
		}
		pods = strings.Split(strings.TrimSpace(stdout.String()), "\n")
	}

	// Execute commands concurrently on each pod
	results := make(chan Result)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for res := range results {
			if *output == "json" {
				if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
					log.Printf("JSON encode failed: %s", err)
				}
			} else {
				if res.Stdout != "" {
					lines := strings.Split(res.Stdout, "\n")
					for _, line := range lines {
						fmt.Fprintf(os.Stdout, "%s stdout: %s\n", res.Pod, line)
					}
				}
				if res.Stderr != "" {
					lines := strings.Split(res.Stderr, "\n")
					for _, line := range lines {
						fmt.Fprintf(os.Stderr, "%s stderr: %s\n", res.Pod, line)
					}
				}
			}
		}
	}()

	var eg errgroup.Group
	eg.SetLimit(*jobs)
	for _, pod := range pods {
		eg.Go(func() error {
			kubectlArgs := []string{"exec"}
			if *namespace != "" {
				kubectlArgs = append(kubectlArgs, "--namespace", *namespace)
			}
			if *container != "" {
				kubectlArgs = append(kubectlArgs, "--container", *container)
			}
			kubectlArgs = append(kubectlArgs, pod, "--")
			kubectlArgs = append(kubectlArgs, args...)
			cmd := exec.Command("kubectl", kubectlArgs...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			res := Result{
				Pod:    pod,
				Stdout: strings.TrimRight(stdout.String(), "\n"),
				Stderr: strings.TrimRight(stderr.String(), "\n"),
			}
			if err != nil {
				res.Error = err.Error()
			}
			results <- res
			return nil
		})
	}
	err := eg.Wait()
	close(results)
	<-done
	return err
}

type Result struct {
	Pod    string `json:"pod,omitempty"`
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
	Error  string `json:"error,omitempty"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}
