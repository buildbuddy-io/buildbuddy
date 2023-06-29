// bazelrbe is a prober that tests basic remote execution / caching.
// This prober invokes Bazel on a workspace that contains targets that copy their inputs to outputs.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	bazelBinary = flag.String("bazel_binary", "", "Path to bazel binary")
	bazelArgs   = flag.String("bazel_args", "", "Space separated list of args to pass to Bazel")
	proberName  = flag.String("prober_name", "", "Short, human-readable name of this prober. This name must be a valid bazel package name (only '.', '@', '-', '_' and alphanumeric characters allowed).")

	numTargets         = flag.Int("num_targets", 10, "Number targets to generate")
	numInputsPerTarget = flag.Int("num_inputs_per_target", 10, "Number of inputs each generated target will have")
	inputSizeBytes     = flag.Int("input_size_bytes", 100_000, "Size of each input file")
)

// createEchoRule creates a target that generates an action that echoes the contents of each input file to a separate
// output file.
func createEchoRule(targetName string, inputs, outputs []string) (string, error) {
	if len(inputs) != len(outputs) {
		return "", status.FailedPreconditionError("number inputs doesn't match number of outputs")
	}

	var srcs, outs []string
	for i, input := range inputs {
		srcs = append(srcs, `"`+input+`"`)
		outs = append(outs, `"`+outputs[i]+`"`)
	}

	return fmt.Sprintf(`
genrule(
      name = "%s",
      srcs = [%s],
      outs = [%s],
      cmd_bash = """
		  srcs=($(SRCS))
		  outs=($(OUTS))
		  for ((i=0; i < $${#srcs[@]}; i++)); do
			src=$${srcs[i]}  
			out=$${outs[i]}
			/bin/cat "$$src" > "$$out"
		  done
      """,
)
`, targetName, strings.Join(srcs, ","), strings.Join(outs, ",")), nil
}

func createWorkspace(dir string, numTargets, numInputsPerTarget, inputSizeBytes int) error {
	err := os.WriteFile(filepath.Join(dir, "WORKSPACE"), []byte(""), 0644)
	if err != nil {
		return err
	}

	if *proberName != "" {
		dir = dir + "/" + *proberName
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return err
		}
	}

	buildFile, err := os.Create(filepath.Join(dir, "BUILD"))
	if err != nil {
		return err
	}
	defer buildFile.Close()
	inputBuf := make([]byte, inputSizeBytes)
	for targetIdx := 0; targetIdx < numTargets; targetIdx++ {
		var inputs []string
		var outputs []string
		for inputIdx := 0; inputIdx < numInputsPerTarget; inputIdx++ {
			if _, err := rand.Read(inputBuf); err != nil {
				return err
			}
			src := fmt.Sprintf("target%d_input%d", targetIdx, inputIdx)
			out := fmt.Sprintf("target%d_output%d", targetIdx, inputIdx)
			if err := os.WriteFile(filepath.Join(dir, src), inputBuf, 0644); err != nil {
				return err
			}
			inputs = append(inputs, src)
			outputs = append(outputs, out)
		}
		rule, err := createEchoRule(fmt.Sprintf("target%d", targetIdx), inputs, outputs)
		if err != nil {
			return err
		}
		if _, err = buildFile.WriteString(rule); err != nil {
			return err
		}
	}

	return nil
}

func runProbe() error {
	workspaceDir, err := os.MkdirTemp("", "bazelrbeprober-*")
	if err != nil {
		log.Fatalf("Could not create temporary workspace directory: %s", err)
	}
	log.Infof("Workspace directory: %s", workspaceDir)
	defer func() {
		log.Infof("Cleaning up workspace directory %s", workspaceDir)
		if err := os.RemoveAll(workspaceDir); err != nil {
			log.Warningf("Could not cleanup temporary workspace: %s", err)
		}
	}()

	err = createWorkspace(workspaceDir, *numTargets, *numInputsPerTarget, *inputSizeBytes)
	if err != nil {
		return status.UnknownErrorf("Could not populate workspace: %s", err)
	}

	args := []string{"//" + *proberName + ":all"}
	if *bazelArgs != "" {
		extraArgs := strings.Split(*bazelArgs, " ")
		args = append(args, extraArgs...)
	}
	res := bazel.Invoke(context.Background(), *bazelBinary, workspaceDir, "build", args...)
	if res.Error != nil {
		return status.UnknownErrorf("Bazel did not exit successfully: %s", res.Error)
	}
	return nil
}

func main() {
	flag.Parse()
	if *bazelBinary == "" {
		log.Fatalf("--bazel_binary is required")
	}

	rand.Seed(time.Now().UnixNano())

	err := runProbe()
	if err != nil {
		log.Fatalf("Probe failure: %s", err)
	}
}
