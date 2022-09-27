package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/autoconfig"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/commandline"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/version"

	bblog "github.com/buildbuddy-io/buildbuddy/cli/logging"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/sidecar"
)

var (
	disable = flag.Bool("disable_buildbuddy", false, "If true, disable buildbuddy functionality and just run bazel.")
)

func die(exitCode int, err error) {
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}

func runBazelAndDie(ctx context.Context, bazelArgs *commandline.BazelArgs, opts *autoconfig.BazelOpts) {
	if opts.EnableRemoteBazel {
		repoConfig, err := remotebazel.Config(".")
		if err != nil {
			log.Fatalf("config err: %s", err)
		}

		exitCode, err := remotebazel.Run(ctx, remotebazel.RunOpts{
			Server:            opts.BuildBuddyEndpoint,
			APIKey:            opts.APIKey,
			Args:              bazelArgs,
			WorkspaceFilePath: opts.WorkspaceFilePath,
			SidecarSocket:     opts.SidecarSocket,
		}, repoConfig)
		if err != nil {
			die(1, err)
		}
		die(exitCode, nil)
	}

	bazelisk.Run(bazelArgs.ExecArgs())
}

func parseBazelRCs(bazelFlags *commandline.BazelFlags) []*parser.BazelOption {
	rcFiles := make([]string, 0)
	if !bazelFlags.NoSystemRC {
		rcFiles = append(rcFiles, "/etc/bazel.bazelrc")
		rcFiles = append(rcFiles, "%ProgramData%\bazel.bazelrc")
	}
	if !bazelFlags.NoWorkspaceRC {
		rcFiles = append(rcFiles, ".bazelrc")
	}
	if !bazelFlags.NoHomeRC {
		usr, err := user.Current()
		if err == nil {
			rcFiles = append(rcFiles, filepath.Join(usr.HomeDir, ".bazelrc"))
		}
	}
	if bazelFlags.BazelRC != "" {
		rcFiles = append(rcFiles, bazelFlags.BazelRC)
	}
	opts, err := parser.ParseRCFiles(rcFiles...)
	if err != nil {
		bblog.Printf("Error parsing .bazelrc file: %s", err.Error())
		return nil
	}
	return opts

}

// keepaliveSidecar validates the connection to the sidecar and keeps the
// sidecar alive as long as this process is alive by issuing background ping
// requests.
func keepaliveSidecar(ctx context.Context, sidecarSocket string) error {
	conn, err := grpc_client.DialTarget("unix://" + sidecarSocket)
	if err != nil {
		return err
	}
	s := scpb.NewSidecarClient(conn)
	connected := make(chan struct{})
	go func() {
		connectionValidated := false
		for {
			_, err := s.Ping(ctx, &scpb.PingRequest{})
			if connectionValidated && err != nil {
				bblog.Printf("sidecar did not respond to ping request: %s\n", err)
				return
			}
			if !connectionValidated && err == nil {
				close(connected)
				connectionValidated = true
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()
	select {
	case <-connected:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("could not connect to sidecar")
	}
}

func main() {
	// Parse any flags (and remove them so bazel isn't confused).
	bazelArgs := commandline.ParseFlagsAndRewriteArgs(os.Args[1:])

	ctx := context.Background()

	if *disable {
		bblog.Printf("Buildbuddy was disabled, just running bazel.")
		runBazelAndDie(ctx, bazelArgs, &autoconfig.BazelOpts{})
	}

	// Make sure we have a home directory to work in.
	bbHome := os.Getenv("BUILDBUDDY_HOME")
	if len(bbHome) == 0 {
		userCacheDir, err := os.UserCacheDir()
		if err != nil {
			die(-1, err)
		}
		bbHome = filepath.Join(userCacheDir, "buildbuddy")
	}
	if err := os.MkdirAll(bbHome, 0755); err != nil {
		die(-1, err)
	}

	bazelFlags := commandline.ExtractBazelFlags(bazelArgs.Filtered)
	autoconfOpts, err := autoconfig.Configure(ctx, bazelFlags)
	if err != nil {
		die(-1, err)
	}
	bazelArgs.Add(autoconfOpts.ExtraArgs...)
	opts := parseBazelRCs(bazelFlags)

	// Determine if cache or BES options are set.
	subcommand := commandline.GetSubCommand(bazelArgs.Filtered)
	besBackendFlag := parser.GetFlagValue(opts, subcommand, bazelFlags.Config, "--bes_backend", autoconfOpts.BESBackendOverride)
	remoteCacheFlag := parser.GetFlagValue(opts, subcommand, bazelFlags.Config, "--remote_cache", autoconfOpts.RemoteCacheOverride)
	remoteExecFlag := parser.GetFlagValue(opts, subcommand, bazelFlags.Config, "--remote_executor", autoconfOpts.RemoteExecutorOverride)

	bazelArgs.Add("--tool_tag=buildbuddy-cli")

	if subcommand == "version" {
		fmt.Printf("bb %s\n", version.AppVersion())
	}
	if besBackendFlag == "" && remoteCacheFlag == "" {
		runBazelAndDie(ctx, bazelArgs, autoconfOpts)
	}

	bblog.Printf("--bes_backend was %q", besBackendFlag)
	bblog.Printf("--remote_cache was %q", remoteCacheFlag)
	bblog.Printf("--remote_executor was %q", remoteExecFlag)

	if err := sidecar.ExtractBundledSidecar(ctx, bbHome); err != nil {
		bblog.Printf("Error extracting sidecar: %s", err)
	}
	// Re(Start) the sidecar if the flags set don't match.
	sidecarArgs := make([]string, 0)
	if besBackendFlag != "" {
		sidecarArgs = append(sidecarArgs, besBackendFlag)
	}
	if remoteCacheFlag != "" && remoteExecFlag == "" {
		sidecarArgs = append(sidecarArgs, remoteCacheFlag)
		// Also specify as disk cache directory.
		diskCacheDir := filepath.Join(bbHome, "filecache")
		sidecarArgs = append(sidecarArgs, fmt.Sprintf("--cache_dir=%s", diskCacheDir))
	}

	if len(sidecarArgs) > 0 {
		sidecarSocket, err := sidecar.RestartSidecarIfNecessary(ctx, bbHome, sidecarArgs)
		if err == nil {
			err = keepaliveSidecar(ctx, sidecarSocket)
		}
		if err == nil {
			if !autoconfOpts.EnableRemoteBazel {
				if besBackendFlag != "" {
					bazelArgs.Add(fmt.Sprintf("--bes_backend=unix://%s", sidecarSocket))
				}
				if remoteCacheFlag != "" && remoteExecFlag == "" {
					bazelArgs.Add(fmt.Sprintf("--remote_cache=unix://%s", sidecarSocket))
				}
				if remoteExecFlag != "" {
					bazelArgs.Add(remoteExecFlag)
				}
			}
			autoconfOpts.SidecarSocket = sidecarSocket
		} else {
			log.Printf("Sidecar could not be initialized, continuing without sidecar: %s", err)
		}
	}
	bblog.Printf("Rewrote bazel command line to: %s", bazelArgs)
	runBazelAndDie(ctx, bazelArgs, autoconfOpts)
}
