package autoconfig

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
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

func remoteBazelArgs(besBackend, remoteExecutor string) []string {
	var args []string
	args = append(args, "--remote_default_exec_properties=container-image=docker://gcr.io/flame-public/buildbuddy-ci-runner:latest")
	args = append(args, "--remote_header=x-buildbuddy-api-key="+*apiKey)
	args = append(args, "--bes_backend="+besBackend)
	args = append(args, "--remote_executor="+remoteExecutor)
	return args
}

func defaultIP(ctx context.Context) (net.IP, error) {
	netIface, err := networking.DefaultInterface(ctx)
	if err != nil {
		return nil, err
	}
	addrs, err := netIface.Addrs()
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		if v, ok := a.(*net.IPNet); ok {
			return v.IP, nil
		}
	}
	return nil, fmt.Errorf("could not determine host's default IP")
}

// TODO(vadim): reduce # of return values
func Configure(ctx context.Context, bazelFlags *commandline.BazelFlags, filteredOSArgs []string) (*commandline.BazelFlags, *BazelOpts, []string, error) {
	var grpcEndpoint, webURL string
	switch *env {
	case "prod":
		grpcEndpoint = "grpcs://remote.buildbuddy.io"
		webURL = "https://app.buildbuddy.io"
	case "dev":
		grpcEndpoint = "grpcs://remote.buildbuddy.dev"
		webURL = "https://app.buildbuddy.dev"
	case "local":
		// we need to use an IP that's reachable from within the firecracker VMs
		ip, err := defaultIP(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		grpcEndpoint = fmt.Sprintf("grpc://%s:1985", ip)
		webURL = "http://localhost:8080"
	default:
		return nil, nil, nil, fmt.Errorf("unknown environment %q", *env)
	}

	wsFilePath, err := bazel.FindWorkspaceFile(".")
	if err != nil {
		return nil, nil, nil, err
	}

	if *events && bazelFlags.BESBackend == "" {
		bazelFlags.BESBackend = grpcEndpoint
		filteredOSArgs = append(filteredOSArgs, fmt.Sprintf("--bes_results_url=%s/invocation/", webURL))
	}

	if (*remote == remoteExec || *remote == remoteBazel) && bazelFlags.RemoteExecutor == "" {
		if *remote == remoteExec {
			filteredOSArgs = append(filteredOSArgs, bbToolchainArgs()...)
			addToolchainsToWorkspaceIfNotPresent(wsFilePath)
			bazelFlags.RemoteExecutor = grpcEndpoint
			filteredOSArgs = append(filteredOSArgs, "--remote_download_minimal")
		} else {
			filteredOSArgs = append(filteredOSArgs, remoteBazelArgs(bazelFlags.BESBackend, grpcEndpoint)...)
			// --remote_download_minimal is supposed to be enough for `bazel run` to work, but it's not the case in
			// practice. Use --remote_download_toplevel to make sure the top level binary is available for running.
			filteredOSArgs = append(filteredOSArgs, "--remote_download_toplevel")
		}
		filteredOSArgs = append(filteredOSArgs, "--jobs=200")
	}

	if (*cache || *remote == remoteBazel) && bazelFlags.RemoteCache == "" {
		bazelFlags.RemoteCache = grpcEndpoint
	}

	bazelOpts := &BazelOpts{
		BuildBuddyEndpoint: grpcEndpoint,
		WorkspaceFilePath:  wsFilePath,
		EnableRemoteBazel:  *remote == remoteBazel,
		APIKey:             *apiKey,
	}

	return bazelFlags, bazelOpts, filteredOSArgs, nil
}

func addToolchainsToWorkspaceIfNotPresent(workspaceFilePath string) {
	workspaceBytes, err := ioutil.ReadFile(workspaceFilePath)
	if err != nil {
		log.Println(err)
		return
	}

	if strings.Contains(string(workspaceBytes), "buildbuddy_toolchain") {
		return
	}

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
	if _, err := workspaceFile.WriteString(buildbuddyToolchain); err != nil {
		log.Println(err)
	}
}
