package executor_smoke_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"google.golang.org/protobuf/encoding/protojson"

	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

// The test binary doubles as the tool that smoke test actions run. It is
// uploaded as an action input, and when the executor runs it with
// helperEnvVar set, TestMain dispatches to runHelper instead of running tests.
// This keeps the actions portable: they behave the same on every OS without
// depending on a shell or coreutils being present on the executor host.
const helperEnvVar = "EXECUTOR_SMOKE_HELPER"

// stepSeparator separates helper steps within a single command line, so that
// one action can run several steps in sequence.
const stepSeparator = ";;"

func helperName() string {
	if runtime.GOOS == "windows" {
		return "smoke_helper.exe"
	}
	return "smoke_helper"
}

// helperArgs builds an action command line that runs the given helper steps.
func helperArgs(steps ...[]string) []string {
	args := []string{"./" + helperName()}
	for i, s := range steps {
		if i > 0 {
			args = append(args, stepSeparator)
		}
		args = append(args, s...)
	}
	return args
}

func step(args ...string) []string { return args }

func runHelper(args []string) int {
	if len(args) > 0 && args[len(args)-1] == "--persistent_worker" {
		if err := runWorker(args[:len(args)-1]); err != nil {
			fmt.Fprintf(os.Stderr, "smoke helper worker: %s\n", err)
			return 100
		}
		return 0
	}
	var steps [][]string
	cur := []string{}
	for _, a := range args {
		if a == stepSeparator {
			steps = append(steps, cur)
			cur = []string{}
			continue
		}
		cur = append(cur, a)
	}
	steps = append(steps, cur)

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for _, s := range steps {
		if len(s) == 0 {
			continue
		}
		code, err := runHelperStep(out, s[0], s[1:])
		if err != nil {
			out.Flush()
			fmt.Fprintf(os.Stderr, "smoke helper: %s: %s\n", s[0], err)
			return 100
		}
		if code >= 0 {
			out.Flush()
			return code
		}
	}
	return 0
}

// runHelperStep runs a single step. It returns a non-negative exit code if the
// helper should exit immediately after the step.
func runHelperStep(out *bufio.Writer, name string, args []string) (int, error) {
	switch name {
	case "echo-args":
		return -1, json.NewEncoder(out).Encode(args)
	case "print":
		_, err := io.WriteString(out, args[0])
		return -1, err
	case "eprint":
		_, err := io.WriteString(os.Stderr, args[0])
		return -1, err
	case "exit":
		code, err := strconv.Atoi(args[0])
		return code, err
	case "env":
		m := map[string]*string{}
		for _, k := range args {
			if v, ok := os.LookupEnv(k); ok {
				m[k] = &v
			} else {
				m[k] = nil
			}
		}
		return -1, json.NewEncoder(out).Encode(m)
	case "pwd":
		wd, err := os.Getwd()
		if err != nil {
			return -1, err
		}
		_, err = fmt.Fprintln(out, wd)
		return -1, err
	case "chmod-x":
		if runtime.GOOS == "windows" {
			return -1, nil
		}
		return -1, os.Chmod(args[0], 0755)
	case "try-append":
		// Try to modify a file, ignoring errors, since the executor may have
		// made it read-only.
		if f, err := os.OpenFile(args[0], os.O_WRONLY|os.O_APPEND, 0); err == nil {
			f.WriteString(args[1])
			f.Close()
		}
		os.Chmod(args[0], 0777)
		return -1, nil
	case "mkfiles":
		// mkfiles <dir> <count>: create count small files under dir.
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return -1, err
		}
		for i := range n {
			d := filepath.Join(args[0], fmt.Sprintf("d%02d", i%10))
			if err := os.MkdirAll(d, 0755); err != nil {
				return -1, err
			}
			if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("f%04d.txt", i)), []byte(strconv.Itoa(i)), 0644); err != nil {
				return -1, err
			}
		}
		return -1, nil
	case "mkdir":
		return -1, os.MkdirAll(args[0], 0755)
	case "write":
		// Deliberately not creating parent directories: the executor is
		// responsible for creating parents of declared outputs.
		return -1, os.WriteFile(args[0], []byte(args[1]), 0644)
	case "gen":
		size, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return -1, err
		}
		seed, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return -1, err
		}
		f, err := os.Create(args[0])
		if err != nil {
			return -1, err
		}
		if _, err := io.Copy(f, genReader(size, seed)); err != nil {
			f.Close()
			return -1, err
		}
		return -1, f.Close()
	case "genstdout":
		size, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return -1, err
		}
		seed, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return -1, err
		}
		_, err = io.Copy(out, genReader(size, seed))
		return -1, err
	case "cat":
		b, err := os.ReadFile(args[0])
		if err != nil {
			return -1, err
		}
		_, err = out.Write(b)
		return -1, err
	case "sha256":
		f, err := os.Open(args[0])
		if err != nil {
			return -1, err
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return -1, err
		}
		_, err = fmt.Fprintln(out, hex.EncodeToString(h.Sum(nil)))
		return -1, err
	case "tree":
		entries, err := listTree(args[0])
		if err != nil {
			return -1, err
		}
		return -1, json.NewEncoder(out).Encode(entries)
	case "sleep":
		d, err := time.ParseDuration(args[0])
		if err != nil {
			return -1, err
		}
		time.Sleep(d)
		return -1, nil
	case "spawn-heartbeat":
		// Start a child process which keeps touching a file until it is
		// killed. Used to check that the executor kills the whole process
		// tree when an action times out.
		self, err := os.Executable()
		if err != nil {
			return -1, err
		}
		cmd := exec.Command(self, "heartbeat", args[0])
		cmd.Env = os.Environ()
		if err := cmd.Start(); err != nil {
			return -1, err
		}
		return -1, nil
	case "heartbeat":
		for {
			if err := os.WriteFile(args[0], []byte(time.Now().Format(time.RFC3339Nano)), 0644); err != nil {
				return -1, err
			}
			time.Sleep(100 * time.Millisecond)
		}
	default:
		return -1, fmt.Errorf("unknown step %q", name)
	}
}

// genReader returns size bytes of deterministic pseudo-random data.
func genReader(size, seed int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

func genBytes(size, seed int64) []byte {
	b, err := io.ReadAll(genReader(size, seed))
	if err != nil {
		panic(err)
	}
	return b
}

type treeEntry struct {
	Path       string `json:"path"`
	Dir        bool   `json:"dir,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Executable bool   `json:"executable,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
}

// listTree lists all files and directories under root, with slash-separated
// paths relative to root.
func listTree(root string) ([]treeEntry, error) {
	var entries []treeEntry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		e := treeEntry{Path: filepath.ToSlash(rel), Dir: d.IsDir()}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			e.Size = info.Size()
			// Windows has no executable bit.
			e.Executable = runtime.GOOS != "windows" && info.Mode()&0111 != 0
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(b)
			e.SHA256 = hex.EncodeToString(sum[:])
		}
		entries = append(entries, e)
		return nil
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, err
}

// runWorker implements the Bazel persistent worker protocol. Each response
// includes the worker's PID and how many requests it has handled, so that tests
// can tell whether the worker process was reused.
func runWorker(startupArgs []string) error {
	protocol := "proto"
	for _, a := range startupArgs {
		if p, ok := strings.CutPrefix(a, "--worker_protocol="); ok {
			protocol = p
		}
	}
	stdin := bufio.NewReader(os.Stdin)
	dec := json.NewDecoder(stdin)
	for n := 1; ; n++ {
		req := &wkpb.WorkRequest{}
		if protocol == "json" {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			if err := protojson.Unmarshal(raw, req); err != nil {
				return err
			}
		} else {
			size, err := binary.ReadUvarint(stdin)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			b := make([]byte, size)
			if _, err := io.ReadFull(stdin, b); err != nil {
				return err
			}
			if err := proto.Unmarshal(b, req); err != nil {
				return err
			}
		}
		argsJSON, err := json.Marshal(req.GetArguments())
		if err != nil {
			return err
		}
		rsp := &wkpb.WorkResponse{
			RequestId: req.GetRequestId(),
			Output:    fmt.Sprintf("pid=%d count=%d args=%s", os.Getpid(), n, argsJSON),
		}
		if protocol == "json" {
			b, err := protojson.Marshal(rsp)
			if err != nil {
				return err
			}
			// Responses must be on a single line.
			b = bytes.ReplaceAll(b, []byte("\n"), nil)
			if _, err := os.Stdout.Write(append(b, '\n')); err != nil {
				return err
			}
		} else {
			b, err := proto.Marshal(rsp)
			if err != nil {
				return err
			}
			if _, err := os.Stdout.Write(append(binary.AppendUvarint(nil, uint64(len(b))), b...)); err != nil {
				return err
			}
		}
	}
}
