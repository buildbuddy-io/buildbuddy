package autoconfig

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/commandline"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
)

const (
	// --remote flag value indicating that Bazel should run locally, with actions being executed remotely.
	remoteExec = "exec"
	// --remote flag value indicating that Bazel and actions should both be run remotely.
	remoteBazel = "bazel"
)

var (
	events = flag.Bool("events", true, "If false, disable sending build events to buildbuddy.")
	remote = flag.String("remote", "", "A value of 'exec' enables remote execution of actions and 'bazel' enables running both bazel and actions remotely.")
	cache  = flag.Bool("cache", false, "If true, perform build with remote caching.")
	env    = flag.String("env", "prod", "The BuildBuddy environment to use. One of prod, dev or local.")
	apiKey = flag.String("api_key", "", "API key to use for BuildBuddy services.")
)

type BazelOpts struct {
	APIKey             string
	BuildBuddyEndpoint string
	WorkspaceFilePath  string
	EnableRemoteBazel  bool
	SidecarSocket      string

	// If the user is trying to use build events, remote caching or execution
	// but did not specify the endpoints on the command line, these fields will
	// contain the automatically configured values that should be passed to
	// Bazel.
	BESBackendOverride     string
	RemoteExecutorOverride string
	RemoteCacheOverride    string

	// Additional arguments to pass to Bazel.
	ExtraArgs []string
}

func bbToolchainArgs() []string {
	var args []string
	args = append(args, "--crosstool_top=@buildbuddy_toolchain//:toolchain")
	args = append(args, "--javabase=@buildbuddy_toolchain//:javabase_jdk8")
	args = append(args, "--host_javabase=@buildbuddy_toolchain//:javabase_jdk8")
	args = append(args, "--java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8")
	args = append(args, "--host_java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8")
	args = append(args, "--host_platform=@buildbuddy_toolchain//:platform")
	args = append(args, "--platforms=@buildbuddy_toolchain//:platform")
	args = append(args, "--extra_execution_platforms=@buildbuddy_toolchain//:platform")
	return args
}

func remoteBazelArgs(grpcURL string) []string {
	var args []string
	args = append(args, "--remote_default_exec_properties=container-image=docker://gcr.io/flame-public/buildbuddy-ci-runner:latest")
	args = append(args, "--remote_header=x-buildbuddy-api-key="+*apiKey)
	args = append(args, "--bes_backend="+grpcURL)
	args = append(args, "--remote_executor="+grpcURL)
	return args
}

type remoteEndpoints struct {
	grpcURL    string
	webBaseURL string
}

func configRemoteEndpoints(ctx context.Context) (*remoteEndpoints, error) {
	switch *env {
	case "prod":
		return &remoteEndpoints{
			grpcURL:    "grpcs://remote.buildbuddy.io",
			webBaseURL: "https://app.buildbuddy.io",
		}, nil
	case "dev":
		return &remoteEndpoints{
			grpcURL:    "grpcs://remote.buildbuddy.dev",
			webBaseURL: "https://app.buildbuddy.dev",
		}, nil
	case "local":
		// we need to use an IP that's reachable from within the firecracker VMs
		ip, err := networking.DefaultIP(ctx)
		if err != nil {
			return nil, err
		}
		return &remoteEndpoints{
			grpcURL:    fmt.Sprintf("grpc://%s:1985", ip),
			webBaseURL: "http://localhost:8080",
		}, nil
	default:
		return nil, fmt.Errorf("unknown environment %q", *env)
	}
}

func Configure(ctx context.Context, bazelFlags *commandline.BazelFlags) (*BazelOpts, error) {
	endpoints, err := configRemoteEndpoints(ctx)
	if err != nil {
		return nil, err
	}

	wsFilePath, err := bazel.FindWorkspaceFile(".")
	if err != nil {
		return nil, err
	}

	bazelOpts := &BazelOpts{
		BuildBuddyEndpoint: endpoints.grpcURL,
		WorkspaceFilePath:  wsFilePath,
		EnableRemoteBazel:  *remote == remoteBazel,
		APIKey:             *apiKey,
	}

	if *events && bazelFlags.BESBackend == "" {
		bazelOpts.BESBackendOverride = endpoints.grpcURL
		bazelOpts.ExtraArgs = append(bazelOpts.ExtraArgs, fmt.Sprintf("--bes_results_url=%s/invocation/", endpoints.webBaseURL))
	}

	if (*remote == remoteExec || *remote == remoteBazel) && bazelFlags.RemoteExecutor == "" {
		if *remote == remoteExec {
			bazelOpts.ExtraArgs = append(bazelOpts.ExtraArgs, bbToolchainArgs()...)
			addToolchainsToWorkspaceIfNotPresent(wsFilePath)
			bazelOpts.RemoteExecutorOverride = endpoints.grpcURL
			bazelOpts.ExtraArgs = append(bazelOpts.ExtraArgs, "--remote_download_minimal")
		} else {
			bazelOpts.ExtraArgs = append(bazelOpts.ExtraArgs, remoteBazelArgs(endpoints.grpcURL)...)
			// --remote_download_minimal is supposed to be enough for `bazel run` to work, but it's not the case in
			// practice. Use --remote_download_toplevel to make sure the top level binary is available for running.
			bazelOpts.ExtraArgs = append(bazelOpts.ExtraArgs, "--remote_download_toplevel")
		}
		bazelOpts.ExtraArgs = append(bazelOpts.ExtraArgs, "--jobs=200")
	}

	if (*cache || *remote == remoteBazel) && bazelFlags.RemoteCache == "" {
		bazelOpts.RemoteCacheOverride = endpoints.grpcURL
	}

	return bazelOpts, nil
}

func addToolchainsToWorkspaceIfNotPresent(workspaceFilePath string) {
	workspaceBytes, err := os.ReadFile(workspaceFilePath)
	if err != nil {
		log.Println(err)
		return
	}

	if strings.Contains(string(workspaceBytes), "buildbuddy_toolchain") {
		return
	}

	httpArchive := `
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
`

	buildbuddyToolchain := `
# BuildBuddy Toolchain

http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "a2a5cccec251211e2221b1587af2ce43c36d32a42f5d881737db3b546a536510",
    strip_prefix = "buildbuddy-toolchain-829c8a574f706de5c96c54ca310f139f4acda7dd",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/829c8a574f706de5c96c54ca310f139f4acda7dd.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")
`

	workspaceFile, err := os.OpenFile(workspaceFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	defer workspaceFile.Close()

	if !strings.Contains(string(workspaceBytes), "http_archive") {
		if _, err := workspaceFile.WriteString(httpArchive); err != nil {
			log.Println(err)
		}
	}

	if _, err := workspaceFile.WriteString(buildbuddyToolchain); err != nil {
		log.Println(err)
	}
}
