package expflag_test

import (
	"context"
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/expflag"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

var (
	boolExperiment    = expflag.Bool("expflag_test.bool", true, "A boolean experiment.")
	stringExperiment  = expflag.String("expflag_test.string", "default", "A string experiment.")
	int64Experiment   = expflag.Int64("expflag_test.int64", 1, "An integer experiment.")
	float64Experiment = expflag.Float64("expflag_test.float64", 1, "A floating point experiment.")
	objectExperiment  = expflag.Object("expflag_test.object", map[string]any{"enabled": true}, "An object experiment.")
)

type fakeProvider struct {
	expflag.FlagProvider

	name         string
	defaultValue bool
	opts         []any

	value   bool
	variant string
}

func (p *fakeProvider) BooleanDetails(ctx context.Context, name string, defaultValue bool, opts ...any) (bool, interfaces.ExperimentFlagDetails) {
	p.name, p.defaultValue, p.opts = name, defaultValue, opts
	return p.value, fakeDetails(p.variant)
}

type fakeDetails string

func (d fakeDetails) Variant() string {
	return string(d)
}

func TestDefaults(t *testing.T) {
	require.True(t, boolExperiment.Get(t.Context()))
	require.Equal(t, "default", stringExperiment.Get(t.Context()))
	require.Equal(t, int64(1), int64Experiment.Get(t.Context()))
	require.Equal(t, float64(1), float64Experiment.Get(t.Context()))
	require.Equal(t, map[string]any{"enabled": true}, objectExperiment.Get(t.Context()))

	evaluated := boolExperiment.GetProto(t.Context())
	diff := cmp.Diff(&expb.EvaluatedFlag{
		Name:  boolExperiment.Name(),
		Value: &expb.EvaluatedFlag_BoolValue{BoolValue: true},
	}, evaluated, protocmp.Transform())
	require.Empty(t, diff)
}

func TestConfigOverrides(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		configure func(*testing.T)
	}{
		{
			name: "command_line",
			configure: func(t *testing.T) {
				for name, value := range map[string]string{
					boolExperiment.Name():    "false",
					stringExperiment.Name():  "",
					int64Experiment.Name():   "9007199254740993",
					float64Experiment.Name(): "1.25",
					objectExperiment.Name():  `{"enabled":false}`,
				} {
					f := flag.Lookup(name)
					require.NotNil(t, f, "flag %q should be declared", name)
					require.NoError(t, f.Value.Set(value))
				}
			},
		},
		{
			name: "yaml",
			configure: func(t *testing.T) {
				flags.PopulateFlagsFromData(t, `
expflag_test:
  bool: false
  string: ""
  int64: 9007199254740993
  float64: 1.25
  object:
    enabled: false
`)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			flags.Set(t, boolExperiment.Name(), true)
			flags.Set(t, stringExperiment.Name(), "default")
			flags.Set(t, int64Experiment.Name(), int64(1))
			flags.Set(t, float64Experiment.Name(), float64(1))
			flags.Set(t, objectExperiment.Name(), map[string]any{"enabled": true})

			testCase.configure(t)

			require.False(t, boolExperiment.Get(t.Context()))
			require.Empty(t, stringExperiment.Get(t.Context()))
			require.Equal(t, int64(9007199254740993), int64Experiment.Get(t.Context()))
			require.Equal(t, 1.25, float64Experiment.Get(t.Context()))
			require.Equal(t, map[string]any{"enabled": false}, objectExperiment.Get(t.Context()))
		})
	}
}

func TestFlagProvider(t *testing.T) {
	provider := &fakeProvider{value: true, variant: "treatment"}
	expflag.SetFlagProvider(provider)
	t.Cleanup(func() { expflag.SetFlagProvider(nil) })
	flags.Set(t, boolExperiment.Name(), false)

	value, details := boolExperiment.GetWithDetails(t.Context(), "option")
	require.Equal(t, boolExperiment.Name(), provider.name)
	require.False(t, provider.defaultValue)
	require.Equal(t, []any{"option"}, provider.opts)

	require.True(t, value)
	require.Equal(t, "treatment", details.GetVariant())
	evaluated := boolExperiment.GetProto(t.Context())
	diff := cmp.Diff(&expb.EvaluatedFlag{
		Name:    boolExperiment.Name(),
		Variant: "treatment",
		Value:   &expb.EvaluatedFlag_BoolValue{BoolValue: true},
	}, evaluated, protocmp.Transform())
	require.Empty(t, diff)
}

func TestContextProvider(t *testing.T) {
	expflag.SetFlagProvider(expflag.NewContextProvider())
	t.Cleanup(func() { expflag.SetFlagProvider(nil) })

	object, err := structpb.NewStruct(map[string]any{"enabled": false})
	require.NoError(t, err)
	ctx := expflag.ContextWithEvaluatedFlags(t.Context(), []*expb.EvaluatedFlag{
		{Name: boolExperiment.Name(), Variant: "off", Value: &expb.EvaluatedFlag_BoolValue{BoolValue: false}},
		{Name: stringExperiment.Name(), Value: &expb.EvaluatedFlag_StringValue{StringValue: ""}},
		{Name: int64Experiment.Name(), Value: &expb.EvaluatedFlag_Int64Value{Int64Value: 9007199254740993}},
		{Name: float64Experiment.Name(), Value: &expb.EvaluatedFlag_Float64Value{Float64Value: 0}},
		{Name: objectExperiment.Name(), Value: &expb.EvaluatedFlag_ObjectValue{ObjectValue: object}},
	})

	value, details := boolExperiment.GetWithDetails(ctx)
	require.False(t, value)
	require.Equal(t, "off", details.GetVariant())
	require.Empty(t, stringExperiment.Get(ctx))
	require.Equal(t, int64(9007199254740993), int64Experiment.Get(ctx))
	require.Zero(t, float64Experiment.Get(ctx))
	require.Equal(t, map[string]any{"enabled": false}, objectExperiment.Get(ctx))

	require.True(t, boolExperiment.Get(t.Context()))
	require.Equal(t, map[string]any{"enabled": true}, objectExperiment.Get(t.Context()))
}

func TestContextProvider_UnusableValues(t *testing.T) {
	expflag.SetFlagProvider(expflag.NewContextProvider())
	t.Cleanup(func() { expflag.SetFlagProvider(nil) })

	for _, testCase := range []struct {
		name  string
		flags []*expb.EvaluatedFlag
	}{
		{
			name:  "missing",
			flags: nil,
		},
		{
			name: "unset_values",
			flags: []*expb.EvaluatedFlag{
				{Name: boolExperiment.Name(), Variant: "off"},
				{Name: stringExperiment.Name()},
				{Name: int64Experiment.Name()},
				{Name: float64Experiment.Name()},
				{Name: objectExperiment.Name()},
			},
		},
		{
			name: "wrong_types",
			flags: []*expb.EvaluatedFlag{
				{Name: boolExperiment.Name(), Variant: "off", Value: &expb.EvaluatedFlag_StringValue{StringValue: "false"}},
				{Name: stringExperiment.Name(), Value: &expb.EvaluatedFlag_BoolValue{}},
				{Name: int64Experiment.Name(), Value: &expb.EvaluatedFlag_Float64Value{}},
				{Name: float64Experiment.Name(), Value: &expb.EvaluatedFlag_Int64Value{}},
				{Name: objectExperiment.Name(), Value: &expb.EvaluatedFlag_StringValue{StringValue: "{}"}},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := expflag.ContextWithEvaluatedFlags(t.Context(), testCase.flags)
			value, details := boolExperiment.GetWithDetails(ctx)
			require.True(t, value)
			require.Empty(t, details.GetVariant())
			require.Equal(t, "default", stringExperiment.Get(ctx))
			require.Equal(t, int64(1), int64Experiment.Get(ctx))
			require.Equal(t, float64(1), float64Experiment.Get(ctx))
			require.Equal(t, map[string]any{"enabled": true}, objectExperiment.Get(ctx))
		})
	}
}

func TestContextWithEvaluatedFlags_ReplacesFlags(t *testing.T) {
	expflag.SetFlagProvider(expflag.NewContextProvider())
	t.Cleanup(func() { expflag.SetFlagProvider(nil) })

	ctx := expflag.ContextWithEvaluatedFlags(t.Context(), []*expb.EvaluatedFlag{
		{Name: int64Experiment.Name(), Value: &expb.EvaluatedFlag_Int64Value{Int64Value: 42}},
	})
	require.Equal(t, int64(42), int64Experiment.Get(ctx))
	ctx = expflag.ContextWithEvaluatedFlags(ctx, nil)
	require.Equal(t, int64(1), int64Experiment.Get(ctx))
}
