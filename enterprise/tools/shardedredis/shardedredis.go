// shardedredis runs a local sharded redis setup.
//
// To run an app pointing at the cluster, run the app like this:
//
// $ bazel run -- enterprise/server $(bazel run enterprise/tools/shardedredis)
//
// This works because this tool runs redis servers in the background and prints
// out the app flags to stdout.
//
// If you prefer to run in the foreground and configure the app flags yourself,
// run this tool in its own terminal, with the -foreground flag:
//
// $ bazel run -- enterprise/tools/shardedredis --foreground

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/armon/circbuf"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/mattn/go-isatty"
)

var (
	n          = flag.Int("replicas", 8, "Number of replicas to run")
	basePort   = flag.Int("base_port", 8379, "Port number for the first replica; replica i will get port base_port+i")
	showOutput = flag.Bool("show_output", false, "Show redis server output")
	foreground = flag.Bool("foreground", false, "Run redis servers in the foreground")
)

// Set by x_defs in BUILD fie
var (
	redisRlocationpath  string
	configRlocationpath string
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type Replica struct {
	cmd          *exec.Cmd
	stderr       *circbuf.Buffer
	terminatedCh chan struct{}
	healthyCh    chan error
	err          error
}

func run() error {
	appFlags := make([]string, 0, *n)
	for i := range *n {
		appFlags = append(appFlags, fmt.Sprintf("--app.default_sharded_redis.shards=localhost:%d", *basePort+i))
	}
	if isatty.IsTerminal(os.Stdout.Fd()) {
		log.Info("App flags:")
	}
	fmt.Println(shlex.Quote(appFlags...))

	redisPath, err := runfiles.Rlocation(redisRlocationpath)
	if err != nil {
		return fmt.Errorf("rlocation %q: %w", redisRlocationpath, err)
	}
	configPath, err := runfiles.Rlocation(configRlocationpath)
	if err != nil {
		return fmt.Errorf("rlocation %q: %w", configRlocationpath, err)
	}

	var replicas []*Replica
	for i := range *n {
		buf, err := circbuf.NewBuffer(16 * 1024)
		if err != nil {
			return fmt.Errorf("create buffer: %w", err)
		}
		port := *basePort + i
		args := []string{configPath, "--port", strconv.Itoa(port)}
		// Forward residual args to redis server
		args = append(args, flag.Args()...)
		cmd := exec.Command(redisPath, args...)
		cmd.Stderr = buf
		if *showOutput {
			cmd.Stdout = log.Writer(fmt.Sprintf("redis-%d:stdout ", i))
			cmd.Stderr = io.MultiWriter(log.Writer(fmt.Sprintf("redis-%d:stderr ", i)), buf)
		}
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start redis-server %d: %w", i, err)
		}
		log.Infof("Started redis-server %d listening on localhost:%d", i, port)

		terminatedCh := make(chan struct{})
		healthyCh := make(chan error, 1)

		r := &Replica{
			cmd:          cmd,
			stderr:       buf,
			terminatedCh: terminatedCh,
			healthyCh:    healthyCh,
		}
		go func() {
			defer close(terminatedCh)
			r.err = cmd.Wait()
		}()
		go func() {
			defer close(healthyCh)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for ctx.Err() == nil {
				conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", *basePort+i))
				if err == nil {
					conn.Close()
					return
				}
				select {
				case <-time.After(10 * time.Millisecond):
					continue
				case <-ctx.Done():
				}
			}
			healthyCh <- ctx.Err()
		}()
		replicas = append(replicas, r)
	}

	log.Infof("Waiting for all redis-server replicas to become healthy...")
	for i, r := range replicas {
		select {
		case err := <-r.healthyCh:
			if err == nil {
				continue
			}
			return fmt.Errorf("replica %d: %w", i, err)
		case <-r.terminatedCh:
			return fmt.Errorf("replica %d: %w: %q", i, r.err, r.stderr.String())
		}
	}

	if *foreground {
		log.Infof("All redis-server replicas are healthy. Press Ctrl+C to quit")
		select {}
	}
	return nil
}
