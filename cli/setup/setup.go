package setup

import (
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/flaghistory"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
)

type Result struct {
	Plugins            []*plugin.Plugin
	BazelArgs          []string
	EffectiveBazelArgs []string
	ExecArgs           []string
	Sidecar            *sidecar.Instance
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

	bazelArgs, execArgs := arg.SplitExecutableArgs(args)
	bazelArgs, err = parser.CanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, err
	}

	effectiveBazelArgs, err := parser.ResolveAndCanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, err
	}

	// Save some flags from the current invocation for non-Bazel commands such
	// as `bb ask`.
	// Flags are saved before the sidecar rewrites them, since API requests
	// require the original effective values.
	bazelArgs = flaghistory.SaveFlags(bazelArgs, effectiveBazelArgs)
	effectiveBazelArgs, err = parser.ResolveAndCanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, err
	}

	// Fiddle with Bazel args
	// TODO(bduffany): model these as "built-in" plugins
	bazelArgs = tooltag.ConfigureToolTag(bazelArgs, effectiveBazelArgs)
	effectiveBazelArgs, err = parser.ResolveAndCanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, err
	}
	bazelArgs, err = login.ConfigureAPIKey(bazelArgs, effectiveBazelArgs)
	if err != nil {
		return nil, err
	}
	effectiveBazelArgs, err = parser.ResolveAndCanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, err
	}

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return nil, err
	}

	// Run plugin pre-bazel hooks
	for _, p := range plugins {
		bazelArgs, execArgs, err = p.PreBazel(bazelArgs, effectiveBazelArgs, execArgs)
		if err != nil {
			return nil, err
		}
		effectiveBazelArgs, err = parser.ResolveAndCanonicalizeArgs(bazelArgs)
		if err != nil {
			return nil, err
		}
	}

	// Save the original BES backend value before it is rewritten by the sidecar.
	originalBESBackend := arg.Get(effectiveBazelArgs, "bes_backend")

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	bazelArgs, sidecarInstance := sidecar.ConfigureSidecar(bazelArgs, effectiveBazelArgs)
	effectiveBazelArgs, err = parser.ResolveAndCanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, err
	}

	return &Result{
		Plugins:            plugins,
		BazelArgs:          bazelArgs,
		EffectiveBazelArgs: effectiveBazelArgs,
		ExecArgs:           execArgs,
		Sidecar:            sidecarInstance,
		OriginalBESBackend: originalBESBackend,
	}, nil
}
