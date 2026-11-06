package exptest_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/exptest"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"
)

var (
	testExperiment = expflag.String("exptest_test.value", "default", "Value returned by the test experiment.")
)

func TestSetup(t *testing.T) {
	env := &real_environment.RealEnv{}
	// Check after Setup's cleanup that experiment values cannot leak into the
	// next test through either expflag or OpenFeature.
	t.Cleanup(func() {
		require.Nil(t, env.GetExperimentFlagProvider())
		require.Equal(t, "default", testExperiment.Get(t.Context()))
		value, err := openfeature.NewClient("test").StringValue(t.Context(), testExperiment.Name(), "sdk-default", openfeature.NewEvaluationContext("", nil))
		require.NoError(t, err)
		require.Equal(t, "sdk-default", value)
	})
	exptest.Setup(t, env)

	// Workflow tests configure experiments after creating the environment.
	provider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		testExperiment.Name(): {
			State:          memprovider.Enabled,
			DefaultVariant: "treatment",
			Variants:       map[string]any{"treatment": "configured"},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(provider))

	value, details := testExperiment.GetWithDetails(t.Context())
	require.Equal(t, "configured", value)
	require.Equal(t, "treatment", details.GetVariant())
	value, envDetails := env.GetExperimentFlagProvider().StringDetails(t.Context(), testExperiment.Name(), "env-default")
	require.Equal(t, "configured", value)
	require.Equal(t, "treatment", envDetails.Variant())
}
