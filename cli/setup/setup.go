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
	Args               *arg.ArgPair
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
	pair := &arg.ArgPair{Raw: bazelArgs, Effective: effectiveBazelArgs}

	// Save some flags from the current invocation for non-Bazel commands such
	// as `bb ask`. Flags are saved before the sidecar rewrites them, since API
	// requests require the original effective values.
	// SaveFlags may append --invocation_id via pair.Append (no re-resolve needed).
	flaghistory.SaveFlags(pair)

	// Fiddle with Bazel args
	// TODO(bduffany): model these as "built-in" plugins
	// These use pair.Append for explicit flags — no re-resolve needed.
	tooltag.ConfigureToolTag(pair)
	if err := login.ConfigureAPIKey(pair); err != nil {
		return nil, err
	}

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return nil, err
	}

	// Run plugin pre-bazel hooks. Re-resolve after each plugin since it may
	// have added a --bazelrc flag that changes the effective view.
	for _, p := range plugins {
		execArgs, err = p.PreBazel(pair, execArgs)
		if err != nil {
			return nil, err
		}
		pair.Effective, err = parser.ResolveAndCanonicalizeArgs(pair.Raw)
		if err != nil {
			return nil, err
		}
	}

	// Save the original BES backend value before it is rewritten by the sidecar.
	originalBESBackend := arg.Get(pair.Effective, "bes_backend")

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	// ConfigureSidecar uses pair.Append for explicit flags — no re-resolve needed.
	sidecarInstance := sidecar.ConfigureSidecar(pair)

	return &Result{
		Plugins:            plugins,
		Args:               pair,
		ExecArgs:           execArgs,
		Sidecar:            sidecarInstance,
		OriginalBESBackend: originalBESBackend,
	}, nil
}
