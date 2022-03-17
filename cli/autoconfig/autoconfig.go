package autoconfig

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/commandline"
)

const (
	remoteExec  = "exec"
	remoteBazel = "bazel"
)

var (
	events = flag.Bool("events", true, "If false, disable sending build events to buildbuddy.")
	remote = flag.String("remote", "", "A value of 'exec' enables remote execution of actions and 'bazel' enables running both bazel and actions remotely.")
	cache  = flag.Bool("cache", false, "If true, perform build with remote caching.")
	dev    = flag.Bool("dev", false, "If true, point at buildbuddy dev cluster.")
	apiKey = flag.String("api_key", "", "API key to use for BuildBuddy services.")
)

type BazelOpts struct {
	APIKey             string
	BuildBuddyEndpoint string
	EnableRemoteBazel  bool
}

func Configure(bazelFlags *commandline.BazelFlags, filteredOSArgs []string) (*commandline.BazelFlags, *BazelOpts, []string) {
	serviceDomain := "buildbuddy.io"
	if *dev {
		serviceDomain = "buildbuddy.dev"
	}

	if *events && bazelFlags.BESBackend == "" {
		bazelFlags.BESBackend = fmt.Sprintf("grpcs://remote.%s", serviceDomain)
		filteredOSArgs = append(filteredOSArgs, fmt.Sprintf("--bes_results_url=https://app.%s/invocation/", serviceDomain))
	}

	if (*remote == remoteExec || *remote == remoteBazel) && bazelFlags.RemoteExecutor == "" {
		bazelFlags.RemoteExecutor = fmt.Sprintf("grpcs://remote.%s", serviceDomain)
		if *remote == remoteExec {
			filteredOSArgs = append(filteredOSArgs, "--crosstool_top=@buildbuddy_toolchain//:toolchain")
			filteredOSArgs = append(filteredOSArgs, "--javabase=@buildbuddy_toolchain//:javabase_jdk8")
			filteredOSArgs = append(filteredOSArgs, "--host_javabase=@buildbuddy_toolchain//:javabase_jdk8")
			filteredOSArgs = append(filteredOSArgs, "--java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8")
			filteredOSArgs = append(filteredOSArgs, "--host_java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8")
			filteredOSArgs = append(filteredOSArgs, "--host_platform=@buildbuddy_toolchain//:platform")
			filteredOSArgs = append(filteredOSArgs, "--platforms=@buildbuddy_toolchain//:platform")
			filteredOSArgs = append(filteredOSArgs, "--extra_execution_platforms=@buildbuddy_toolchain//:platform")
		} else {
			filteredOSArgs = append(filteredOSArgs, "--remote_default_exec_properties=container-image=docker://gcr.io/flame-public/buildbuddy-ci-runner:latest")
			filteredOSArgs = append(filteredOSArgs, "--remote_header=x-buildbuddy-api-key="+*apiKey)
			filteredOSArgs = append(filteredOSArgs, "--remote_executor="+bazelFlags.RemoteExecutor)
			filteredOSArgs = append(filteredOSArgs, "--bes_backend="+bazelFlags.BESBackend)
		}
		filteredOSArgs = append(filteredOSArgs, "--remote_download_minimal")
		filteredOSArgs = append(filteredOSArgs, "--jobs=200")

		addToolchainsToWorkspaceIfNotPresent()
	}

	if *cache && bazelFlags.RemoteCache == "" {
		bazelFlags.RemoteCache = fmt.Sprintf("grpcs://remote.%s", serviceDomain)
	}

	bazelOpts := &BazelOpts{
		BuildBuddyEndpoint: fmt.Sprintf("grpcs://remote.%s", serviceDomain),
		EnableRemoteBazel:  *remote == remoteBazel,
		APIKey:             *apiKey,
	}

	return bazelFlags, bazelOpts, filteredOSArgs
}

func addToolchainsToWorkspaceIfNotPresent() {
	workspaceFileName := "WORKSPACE.bazel"
	workspaceBytes, err := ioutil.ReadFile(workspaceFileName)
	if err != nil {
		workspaceBytes, err = ioutil.ReadFile("WORKSPACE")
		workspaceFileName = "WORKSPACE"
		if err != nil {
			log.Println(err)
			return
		}
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

	workspaceFile, err := os.OpenFile(workspaceFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return
	}
	defer workspaceFile.Close()
	if _, err := workspaceFile.WriteString(buildbuddyToolchain); err != nil {
		log.Println(err)
	}
}
