// Package exptest provides experiment flag setup for tests.
package exptest

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/require"
)

// Setup installs an experiment flag provider in env and expflag. Configure the
// OpenFeature provider with openfeature.SetProviderAndWait before or after
// calling Setup.
//
// Cleanup clears the provider from env and expflag and resets OpenFeature to
// its no-op provider. Call Setup before starting background workers so their
// cleanup runs before the provider is removed. Tests using Setup must not run
// in parallel because these providers are process-wide.
func Setup(t testing.TB, env *real_environment.RealEnv) {
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
	expflag.SetFlagProvider(fp)
	t.Cleanup(func() {
		expflag.SetFlagProvider(nil)
		env.SetExperimentFlagProvider(nil)
		require.NoError(t, openfeature.SetProviderAndWait(openfeature.NoopProvider{}))
	})
}
