package setup

import (
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
)

// Setup prepares us to run a bazel command. It loads plugins, handles auth,
// configures the sidecar, and runs pre-bazel handlers.
func Setup(args []string, tempDir string) (plugins []*plugin.Plugin, bazelArgs []string, execArgs []string, err error) {
	// Load plugins
	plugins, err = plugin.LoadAll(tempDir)
	if err != nil {
		return nil, nil, nil, err
	}

	// Parse args.
	bazelArgs, execArgs = arg.SplitExecutableArgs(args)
	// TODO: Expanding configs results in a long explicit command line in the BB
	// UI. Need to find a way to override the explicit command line in the UI so
	// that it reflects the args passed to the CLI, not the wrapped Bazel
	// process.
	bazelArgs, err = parser.ExpandConfigs(bazelArgs)
	if err != nil {
		return nil, nil, nil, err
	}

	// Fiddle with Bazel args
	// TODO(bduffany): model these as "built-in" plugins
	bazelArgs = tooltag.ConfigureToolTag(bazelArgs)
	bazelArgs, err = login.ConfigureAPIKey(bazelArgs)
	if err != nil {
		return nil, nil, nil, err
	}

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return nil, nil, nil, err
	}

	// Run plugin pre-bazel hooks
	bazelArgs, err = parser.CanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, p := range plugins {
		bazelArgs, execArgs, err = p.PreBazel(bazelArgs, execArgs)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	bazelArgs = sidecar.ConfigureSidecar(bazelArgs)

	return plugins, bazelArgs, execArgs, nil
}
