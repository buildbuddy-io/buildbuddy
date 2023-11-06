// Utility for running a remote execution side-by-side comparison test to
// evaluate remote execution changes. This tool sequentially runs several
// control then test builds and spits out an SQL query that grabs performance
// metrics (e.g. time and cache performance) about those builds for analysis.
// It may be useful for evaluating the performance / cost impact of
// remote-execution or caching changes, for example.
//
// Example usage:
//
// mkdir /tmp/sxs
// git clone http://github.com/buildbuddy-io/buildbuddy /tmp/sxs/test
// cp -R /tmp/sxs/test /tmp/sxs/control
// bazel run //enterprise/tools/rbesxs -- \
// --iterations=10 \
// --api_key=abcdef123456 \
// --setup \
// --target=//server/util/status \
// --test_repo=/tmp/sxs/test \
// --test_flags=--enable_crazy_feature \
// --control_repo=/tmp/sxs/control
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	iterations  = flag.Int("iterations", 1, "The number of side-by-side iterations to run.")
	environment = flag.String("environment", "dev", "The environment to test against, must be 'dev' or 'prod'.")
	apiKey      = flag.String("api_key", "", "BuildBuddy API key to use.")

	target     = flag.String("target", "", "Target to build")
	doSetup    = flag.Bool("setup", false, "Set to true to perform a setup build. Useful if you want to seed or prepare the remote executors in some way.")
	setupFlags = flag.String("setup_flags", "", "Comma-separated list of extra flags to add to the setup builds.")

	// TODO(iain): add a --repo flag so when local changes aren't needed the
	// script can clone and setup the test and control repos.
	controlRepo  = flag.String("control_repo", "", "Filesystem path of the control repository. May be the same as test if no changed are needed.")
	controlFlags = flag.String("control_flags", "", "Comma-separated list of extra flags to add to the control builds.")
	testRepo     = flag.String("test_repo", "", "Filesystem path of the test repository. May be the same as control if no changed are needed.")
	testFlags    = flag.String("test_flags", "", "Comma-separated list of extra flags to add to the test builds.")
)

func randomString() string {
	name, err := random.RandomString(20)
	if err != nil {
		log.Fatalf("error generating remote instance name: %s", err)
	}
	return name
}

func getBuildbuddyUrlOrDie(stderr string) string {
	if !strings.Contains(stderr, "Streaming build results to: ") {
		log.Fatalf("no buildbuddy url found in stderr: %s", stderr)
		return ""
	}
	split := strings.Split(stderr, "Streaming build results to: ")[1]
	bburl := strings.Split(split, "\n")[0]
	if bburl == "" {
		log.Fatalf("buildbuddy url is empty, stderr: %s", stderr)
	}
	_, err := url.ParseRequestURI(bburl)
	if err != nil {
		log.Fatalf("%s is not a url: %s", bburl, stderr)
	}
	return bburl
}

func newCommand(target, environment, apiKey, remoteInstanceName, tag string, extraFlags []string) *repb.Command {
	cmd := []string{"bazel", "build", target}
	envSuffix := "dev"
	if environment == "prod" {
		envSuffix = "io"
	}
	// Add these first so the buildbuddy flags take precedence.
	for i := 0; i < len(extraFlags); i++ {
		if extraFlags[i] != "" {
			cmd = append(cmd, extraFlags[i])
		}
	}
	cmd = append(cmd, fmt.Sprintf("--remote_executor=remote.buildbuddy.%s", envSuffix))
	cmd = append(cmd, fmt.Sprintf("--remote_cache=remote.buildbuddy.%s", envSuffix))
	cmd = append(cmd, fmt.Sprintf("--bes_backend=remote.buildbuddy.%s", envSuffix))
	cmd = append(cmd, fmt.Sprintf("--bes_results_url=https://app.buildbuddy.%s/invocation/", envSuffix))
	cmd = append(cmd, fmt.Sprintf("--remote_header=x-buildbuddy-api-key=%s", apiKey))
	cmd = append(cmd, fmt.Sprintf("--remote_instance_name=%s", remoteInstanceName))
	if tag != "" {
		cmd = append(cmd, fmt.Sprintf("--build_metadata=TAGS=%s", tag))
	}
	cmd = append(cmd, "--noremote_accept_cached")
	return &repb.Command{Arguments: cmd}
}

func bazelClean(ctx context.Context, workDir string) {
	result := commandutil.Run(ctx, &repb.Command{Arguments: []string{"bazel", "clean", "--async"}}, workDir, nil, nil)
	if result.ExitCode != 0 {
		log.Fatalf("bazel clean failed: %s", string(result.Stderr))
	}
}

func getSQLQuery(bbUrls []string) string {
	if len(bbUrls) == 0 {
		return ""
	}
	invocationIds := make([]string, len(bbUrls))
	for i, bbUrl := range bbUrls {
		split := strings.Split(bbUrl, "/")
		invocationIds[i] = split[len(split)-1]
	}
	return "SELECT invocation_id, duration_usec, action_cache_hits, action_cache_misses, cas_cache_hits, cas_cache_uploads, total_download_size_bytes, total_upload_size_bytes FROM Invocations WHERE (invocation_id='" + strings.Join(invocationIds, "' OR invocation_id='") + "');"
}

func main() {
	flag.Parse()

	if *iterations < 0 {
		log.Fatal("--iterations cannot be < 0")
	}
	if !(*environment == "dev" || *environment == "prod") {
		log.Fatal("--environment must be 'dev' or 'prod'")
	}
	if *target == "" {
		log.Fatal("--target required")
	}
	if *controlRepo == "" || *testRepo == "" {
		log.Fatal("--test_repo and --control_repo must be specified")
	}

	ctx := context.Background()

	control := make([]string, *iterations)
	test := make([]string, *iterations)

	failures := 0
	controlTag := randomString()
	testTag := randomString()

	for i := 0; i < *iterations; i++ {
		if failures > *iterations/2 {
			log.Fatal("Failure rate is too high, aborting!")
		}

		remoteInstanceName := randomString()
		if *doSetup {
			bazelClean(ctx, *controlRepo)
			setupCmd := newCommand(*target, *environment, *apiKey, remoteInstanceName, "", strings.Split(*setupFlags, ","))
			log.Infof("iteration %d: running setup command: %s", i, strings.Join(setupCmd.Arguments, " "))
			result := commandutil.Run(ctx, setupCmd, *controlRepo, nil, nil)
			if result.ExitCode != 0 {
				failures++
				log.Infof("setup command failed, retrying: %s", string(result.Stderr))
				i--
				continue
			}
		}

		bazelClean(ctx, *controlRepo)
		controlCmd := newCommand(*target, *environment, *apiKey, remoteInstanceName, controlTag, strings.Split(*controlFlags, ","))
		log.Infof("iteration %d: running control command: %s", i, strings.Join(controlCmd.Arguments, " "))
		result := commandutil.Run(ctx, controlCmd, *controlRepo, nil, nil)
		if result.ExitCode != 0 {
			failures++
			log.Infof("control command failed, retrying: %s", string(result.Stderr))
			i--
			continue
		}
		control[i] = getBuildbuddyUrlOrDie(string(result.Stderr))
		log.Infof("iteration %d: control finished: %s", i, control[i])

		// TODO(iain): it might be interesting/useful to support running test
		// and control in parallel to isolate time effects.
		remoteInstanceName = randomString()
		if *doSetup {
			bazelClean(ctx, *testRepo)
			setupCmd := newCommand(*target, *environment, *apiKey, remoteInstanceName, "", strings.Split(*setupFlags, ","))
			log.Infof("iteration %d: running setup command: %s", i, strings.Join(setupCmd.Arguments, " "))
			result = commandutil.Run(ctx, setupCmd, *controlRepo, nil, nil)
			if result.ExitCode != 0 {
				failures++
				log.Infof("setup command failed, retrying: %s", string(result.Stderr))
				i--
				continue
			}
		}

		bazelClean(ctx, *testRepo)
		testCmd := newCommand(*target, *environment, *apiKey, remoteInstanceName, testTag, strings.Split(*testFlags, ","))
		log.Infof("iteration %d: running test command: %s", i, strings.Join(testCmd.Arguments, " "))
		result = commandutil.Run(ctx, testCmd, *testRepo, nil, nil)
		if result.ExitCode != 0 {
			failures++
			log.Infof("test command failed, retrying: %s", string(result.Stderr))
			i--
			continue
		}
		test[i] = getBuildbuddyUrlOrDie(string(result.Stderr))
		log.Infof("iteration %d: test finished: %s", i, test[i])
	}

	// TODO(iain): would be neat to pull stats directly from web here, however
	// the page is rendered with js, so just wgetting it doesn't work.
	log.Infof("control runs")
	for i := 0; i < *iterations; i++ {
		log.Info(control[i])
	}
	log.Infof("trends: https://app.buildbuddy.dev/trends/?tag=%s", controlTag)
	log.Infof("test runs (tag: %s)", testTag)
	for i := 0; i < *iterations; i++ {
		log.Info(test[i])
	}
	log.Infof("trends: https://app.buildbuddy.dev/trends/?tag=%s", testTag)

	log.Info("MySQL query to get control stats:")
	log.Info(getSQLQuery(control))
	log.Info("MySQL query to get test stats:")
	log.Info(getSQLQuery(test))
}
