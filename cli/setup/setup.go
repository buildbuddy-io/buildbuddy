package setup

import (
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/flaghistory"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
)

type Result struct {
	// Plugins are the loaded CLI plugins that should run with this invocation.
	Plugins []*plugin.Plugin

	// BazelArgs contains the forwarded args passed to Bazelisk and the resolved
	// args used internally for setup decisions.
	BazelArgs *arg.BazelArgs

	// ExecArgs are args to pass to the executable produced by `bazel run`.
	ExecArgs []string

	// Sidecar is the sidecar instance started for this invocation, if any.
	Sidecar *sidecar.Instance

	// OriginalBESBackend is the resolved BES backend before sidecar rewrites.
	OriginalBESBackend string
}

// Setup prepares us to run a bazel command. It loads plugins, handles auth,
// configures the sidecar, and runs pre-bazel handlers.
func Setup(args []string, tempDir string) (*Result, error) {
	// Load plugins
	plugins, err := plugin.LoadAll(tempDir)
	if err != nil {
		return nil, err
	}

	bazelArgsStr, execArgs := arg.SplitExecutableArgs(args)
	bazelArgs, err := arg.NewBazelArgs(bazelArgsStr)
	if err != nil {
		return nil, err
	}

	// Save some flags from the current invocation for non-Bazel commands such
	// as `bb ask`. Flags are saved before the sidecar rewrites them, since API
	// requests require the original resolved values.
	bazelArgs, err = flaghistory.SaveFlags(bazelArgs)
	if err != nil {
		return nil, err
	}

	// Fiddle with Bazel args
	// TODO(bduffany): model these as "built-in" plugins
	if err := tooltag.ConfigureToolTag(bazelArgs); err != nil {
		return nil, err
	}
	if err := login.ConfigureAPIKey(bazelArgs); err != nil {
		return nil, err
	}

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return nil, err
	}

	// Run plugin pre-bazel hooks.
	for _, p := range plugins {
		bazelArgs, execArgs, err = p.PreBazel(bazelArgs, execArgs)
		if err != nil {
			return nil, err
		}
	}

	// Save the original BES backend value before it is rewritten by the sidecar.
	originalBESBackend := bazelArgs.Get("bes_backend")

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	sidecarInstance, err := sidecar.ConfigureSidecar(bazelArgs)
	if err != nil {
		return nil, err
	}

	return &Result{
		Plugins:            plugins,
		BazelArgs:          bazelArgs,
		ExecArgs:           execArgs,
		Sidecar:            sidecarInstance,
		OriginalBESBackend: originalBESBackend,
	}, nil
}
