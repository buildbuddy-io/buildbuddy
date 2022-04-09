package autoconfig

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/commandline"
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
	dev    = flag.Bool("dev", false, "If true, point at buildbuddy dev cluster.")
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

func Configure(bazelFlags *commandline.BazelFlags, filteredOSArgs []string) (*commandline.BazelFlags, *BazelOpts, []string, error) {
	serviceDomain := "buildbuddy.io"
	if *dev {
		serviceDomain = "buildbuddy.dev"
	}

	wsFilePath, err := findWorkspaceFile()
	if err != nil {
		return nil, nil, nil, err
	}

	if *events && bazelFlags.BESBackend == "" {
		bazelFlags.BESBackend = fmt.Sprintf("grpcs://remote.%s", serviceDomain)
		filteredOSArgs = append(filteredOSArgs, fmt.Sprintf("--bes_results_url=https://app.%s/invocation/", serviceDomain))
	}

	if (*remote == remoteExec || *remote == remoteBazel) && bazelFlags.RemoteExecutor == "" {
		remoteExecTarget := fmt.Sprintf("grpcs://remote.%s", serviceDomain)
		if *remote == remoteExec {
			filteredOSArgs = append(filteredOSArgs, bbToolchainArgs()...)
			addToolchainsToWorkspaceIfNotPresent(wsFilePath)
			bazelFlags.RemoteExecutor = remoteExecTarget
		} else {
			filteredOSArgs = append(filteredOSArgs, remoteBazelArgs(bazelFlags.BESBackend, remoteExecTarget)...)
		}
		filteredOSArgs = append(filteredOSArgs, "--remote_download_minimal")
		filteredOSArgs = append(filteredOSArgs, "--jobs=200")
	}

	if (*cache || *remote == remoteBazel) && bazelFlags.RemoteCache == "" {
		bazelFlags.RemoteCache = fmt.Sprintf("grpcs://remote.%s", serviceDomain)
	}

	bazelOpts := &BazelOpts{
		BuildBuddyEndpoint: fmt.Sprintf("grpcs://remote.%s", serviceDomain),
		WorkspaceFilePath:  wsFilePath,
		EnableRemoteBazel:  *remote == remoteBazel,
		APIKey:             *apiKey,
	}

	return bazelFlags, bazelOpts, filteredOSArgs, nil
}

func findWorkspaceFile() (string, error) {
	path, err := filepath.Abs(".")
	if err != nil {
		return "", nil
	}
	for path != "/" {
		for _, wsFilename := range []string{"WORKSPACE.bazel", "WORKSPACE"} {
			wsFilePath := filepath.Join(path, wsFilename)
			_, err := os.Stat(wsFilePath)
			if err == nil {
				return wsFilePath, nil
			}
			if err != nil && !os.IsNotExist(err) {
				log.Printf("Could not check existence of workspace file at %q: %s\n", wsFilePath, err)
				continue
			}
		}
		path = filepath.Dir(path)
	}
	return "", fmt.Errorf("could not detect workspace root (are you running bb within a Bazel workspace?)")
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
