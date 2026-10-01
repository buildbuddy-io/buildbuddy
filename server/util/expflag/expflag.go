// Package expflag allows declaring configuration options that can be controlled
// via flagd as well as normal server flags.
//
// # Declarations
//
// Declare experiments at the package level using Bool, String, Int64, Float64,
// or Object. Follow normal flag naming conventions, with a subsystem namespace
// and a snake_case feature name, such as app.widget_color. Adding a blank line
// between normal flags and experiment flags can help delineate them more
// clearly. Example:
//
//	var (
//	    widgetSize = flag.Int("app.widget_size", 10, "Widget size.")
//
//	    widgetColor = expflag.String("app.widget_color", "blue", "Widget color.")
//	)
//
// Help strings should describe flag behavior, including special values that
// disable a feature. Mention experiment-specific behavior only when it matters
// to someone configuring the flag. Do not repeat the default value in help
// strings, since it's already generated in help output.
//
// Each experiment also declares a command-line flag with the same name, so
// declaring an experiment with the same name as another experiment or flag
// panics. The flag's configured value is the default used when no experiment
// flag provider is installed or the provider cannot evaluate the experiment.
// Defaults can be changed from the command line or YAML config, just like
// normal flags.
//
// # Evaluation and defaults
//
// Evaluating an experiment requires a context, because the value depends on
// request attributes such as the authenticated group ID:
//
//	frontendConfig.WidgetColor = widgetColor.Get(ctx)
//
// Pass custom targeting attributes using experiments.WithContext from
// enterprise/server/experiments. Pass an option for each attribute:
//
//	color := widgetColor.Get(ctx,
//	    experiments.WithContext("widget_kind", "warning"),
//	    experiments.WithContext("page", "settings"),
//	)
//
// These options are forwarded to the provider for this evaluation and can also
// be passed to GetWithDetails or GetProto. Multi-arm experiments can use
// GetWithDetails to get the selected variant:
//
//	color, details := widgetColor.GetWithDetails(ctx)
//	successMetric.WithLabelValues(details.GetVariant()).Inc()
//
// Callers should generally rely on configured defaults instead of checking
// whether the environment's experiment provider is nil. It's also a good idea
// to short-circuit on values that disable the feature before doing expensive
// work such as database queries or RPCs.
//
// # Executors
//
// This package is not yet ready for use with executors.
// For now, only use expflag on the apps and cache proxies.
// TODO: update documentation here once executors support expflag.
//
// # Options and renamed flags
//
// Constructors accept expflag-specific options and normal flag tags in any
// order. Expflag consumes its own options and forwards the remaining tags to
// the underlying flag constructor:
//
//	var widgetColor = expflag.String("app.widget_color", "blue", "Widget color.", flag.Internal, expflag.DeprecatedExperimentName("widget-color"))
//
// When renaming an experiment flag, use DeprecatedExperimentName to keep existing
// flagd configurations working. The new name is evaluated first. The old name
// is read only if the provider reports that the new name is missing. A value of
// false, zero, an empty string, or the configured default under the new name
// still takes precedence. Other evaluation errors do not trigger fallback. The
// command-line flag, YAML key, and evaluated proto all use the new name; this
// option does not declare an alias for the old command-line flag or YAML key.
// Fallback adds a second provider evaluation while the new name is missing.
//
// # Testing
//
// For simple experiments that do not require custom targeting, prefer using
// flags.Set from server/util/testing/flags to configure experiments in tests.
// It sets the flag's default and restores the previous value during cleanup:
//
//	flags.Set(t, "app.widget_color", "red")
//	require.Equal(t, "red", widgetColor.Get(t.Context()))
//
// When testing custom targeting attributes, call exptest.Setup(t, env) from
// enterprise/server/testutil/exptest and configure OpenFeature targeting rules
// for the attributes being tested. Setup installs the provider in env and
// expflag and registers cleanup. Call it before starting background workers so
// they finish before the provider is removed. These tests must not run in
// parallel because the providers are process-wide.
//
// This example uses experiments, exptest, and the OpenFeature openfeature and
// memprovider packages. The evaluator sets Variant so the test can check which
// variant was selected using GetWithDetails:
//
//	exptest.Setup(t, env)
//
//	evaluator := func(_ memprovider.InMemoryFlag, attrs openfeature.FlattenedContext) (any, openfeature.ProviderResolutionDetail) {
//	    if attrs["widget_kind"] == "warning" {
//	        return "red", openfeature.ProviderResolutionDetail{Reason: openfeature.TargetingMatchReason, Variant: "warning"}
//	    }
//	    return "blue", openfeature.ProviderResolutionDetail{Reason: openfeature.DefaultReason, Variant: "default"}
//	}
//	provider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
//	    widgetColor.Name(): {
//	        State:            memprovider.Enabled,
//	        ContextEvaluator: &evaluator,
//	    },
//	})
//	require.NoError(t, openfeature.SetProviderAndWait(provider))
//
//	color, details := widgetColor.GetWithDetails(t.Context(), experiments.WithContext("widget_kind", "warning"))
//	require.Equal(t, "red", color)
//	require.Equal(t, "warning", details.GetVariant())
//	require.Equal(t, "blue", widgetColor.Get(t.Context(), experiments.WithContext("widget_kind", "info")))
package expflag

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/protobuf/types/known/structpb"

	expb "github.com/buildbuddy-io/buildbuddy/proto/experiments"
)

var (
	// provider evaluates all flags. It is nil until SetFlagProvider is called.
	provider atomic.Pointer[FlagProvider]
)

// FlagProvider is the subset of interfaces.ExperimentFlagProvider used to
// evaluate experiment flags.
type FlagProvider interface {
	BooleanDetails(ctx context.Context, name string, defaultValue bool, opts ...any) (bool, interfaces.ExperimentFlagDetails)
	StringDetails(ctx context.Context, name string, defaultValue string, opts ...any) (string, interfaces.ExperimentFlagDetails)
	Int64Details(ctx context.Context, name string, defaultValue int64, opts ...any) (int64, interfaces.ExperimentFlagDetails)
	Float64Details(ctx context.Context, name string, defaultValue float64, opts ...any) (float64, interfaces.ExperimentFlagDetails)
	ObjectDetails(ctx context.Context, name string, defaultValue map[string]any, opts ...any) (map[string]any, interfaces.ExperimentFlagDetails)
}

// SetFlagProvider installs the provider used to evaluate all experiment
// flags. Binaries call this once during initialization. Passing nil removes
// the provider, so that flags return their configured defaults.
func SetFlagProvider(p FlagProvider) {
	if p == nil {
		provider.Store(nil)
		return
	}
	provider.Store(&p)
}

// flagNotFoundReporter is implemented by evaluation details that can report
// whether the provider found the flag. Fallback to a deprecated experiment name
// happens only when the new name is missing. Other evaluation errors return
// false, so a disabled or misconfigured flag under the new name does not fall
// back to the old one.
type flagNotFoundReporter interface {
	FlagNotFound() bool
}

type deprecatedExperimentName string

// DeprecatedExperimentName keeps existing flagd configurations working after a
// flag is renamed. The new name takes precedence; the old name is read only
// when the provider reports that the new name is missing. This option does
// not declare a command-line flag or YAML alias for the old name.
func DeprecatedExperimentName(name string) any {
	return deprecatedExperimentName(name)
}

func parseOptions(opts []any) (string, []tags.Taggable) {
	var deprecatedName string
	var flagTags []tags.Taggable
	for _, opt := range opts {
		switch opt := opt.(type) {
		case deprecatedExperimentName:
			deprecatedName = string(opt)
		case tags.Taggable:
			flagTags = append(flagTags, opt)
		default:
			panic(fmt.Sprintf("unsupported expflag option %T", opt))
		}
	}
	return deprecatedName, flagTags
}

// Flag is an experiment flag with a value of type T. Declare flags at the
// package level using Bool, String, Int64, Float64, or Object.
type Flag[T any] struct {
	name                     string
	deprecatedExperimentName string
	// Configured default, backed by the command-line flag of the same name.
	defaultValue *T
	// evaluate is the FlagProvider method that evaluates values of type T.
	evaluate func(p FlagProvider, ctx context.Context, name string, defaultValue T, opts ...any) (T, interfaces.ExperimentFlagDetails)
	// setValue stores a value of type T in the value oneof of an EvaluatedFlag.
	setValue func(f *expb.EvaluatedFlag, value T) error
}

// Bool declares a boolean experiment flag.
func Bool(name string, defaultValue bool, help string, opts ...any) *Flag[bool] {
	deprecatedName, flagTags := parseOptions(opts)
	return &Flag[bool]{
		name:                     name,
		deprecatedExperimentName: deprecatedName,
		defaultValue:             flag.Bool(name, defaultValue, help, flagTags...),
		evaluate:                 FlagProvider.BooleanDetails,
		setValue: func(f *expb.EvaluatedFlag, value bool) error {
			f.Value = &expb.EvaluatedFlag_BoolValue{BoolValue: value}
			return nil
		},
	}
}

// String declares a string experiment flag.
func String(name string, defaultValue string, help string, opts ...any) *Flag[string] {
	deprecatedName, flagTags := parseOptions(opts)
	return &Flag[string]{
		name:                     name,
		deprecatedExperimentName: deprecatedName,
		defaultValue:             flag.String(name, defaultValue, help, flagTags...),
		evaluate:                 FlagProvider.StringDetails,
		setValue: func(f *expb.EvaluatedFlag, value string) error {
			f.Value = &expb.EvaluatedFlag_StringValue{StringValue: value}
			return nil
		},
	}
}

// Int64 declares an int64 experiment flag.
func Int64(name string, defaultValue int64, help string, opts ...any) *Flag[int64] {
	deprecatedName, flagTags := parseOptions(opts)
	return &Flag[int64]{
		name:                     name,
		deprecatedExperimentName: deprecatedName,
		defaultValue:             flag.Int64(name, defaultValue, help, flagTags...),
		evaluate:                 FlagProvider.Int64Details,
		setValue: func(f *expb.EvaluatedFlag, value int64) error {
			f.Value = &expb.EvaluatedFlag_Int64Value{Int64Value: value}
			return nil
		},
	}
}

// Float64 declares a float64 experiment flag.
func Float64(name string, defaultValue float64, help string, opts ...any) *Flag[float64] {
	deprecatedName, flagTags := parseOptions(opts)
	return &Flag[float64]{
		name:                     name,
		deprecatedExperimentName: deprecatedName,
		defaultValue:             flag.Float64(name, defaultValue, help, flagTags...),
		evaluate:                 FlagProvider.Float64Details,
		setValue: func(f *expb.EvaluatedFlag, value float64) error {
			f.Value = &expb.EvaluatedFlag_Float64Value{Float64Value: value}
			return nil
		},
	}
}

// objectValue is the command-line flag type for object experiment flags. It
// is a named map type because the YAML config loader walks map[string]any
// values as nested config sections. An unnamed map[string]any flag value would
// have its keys interpreted as flag names nested under the object flag's name.
type objectValue map[string]any

// Object declares an experiment flag whose value is a JSON object. The map
// returned by Get may be shared with other callers, so callers must not modify
// it.
func Object(name string, defaultValue map[string]any, help string, opts ...any) *Flag[map[string]any] {
	deprecatedName, flagTags := parseOptions(opts)
	value := flag.New(flag.CommandLine, name, objectValue(defaultValue), help, flagTags...)
	return &Flag[map[string]any]{
		name:                     name,
		deprecatedExperimentName: deprecatedName,
		defaultValue:             (*map[string]any)(value),
		evaluate:                 FlagProvider.ObjectDetails,
		setValue: func(f *expb.EvaluatedFlag, value map[string]any) error {
			object, err := structpb.NewStruct(value)
			if err != nil {
				return err
			}
			f.Value = &expb.EvaluatedFlag_ObjectValue{ObjectValue: object}
			return nil
		},
	}
}

// Name returns the flag name.
func (f *Flag[T]) Name() string {
	return f.name
}

// Get evaluates the flag and returns its value. Options are passed through to
// the provider, for example experiments.WithContext.
func (f *Flag[T]) Get(ctx context.Context, opts ...any) T {
	value, _ := f.get(ctx, opts...)
	return value
}

// GetWithDetails evaluates the flag and returns its value along with the
// evaluation details, which include the selected variant.
func (f *Flag[T]) GetWithDetails(ctx context.Context, opts ...any) (T, *expb.EvaluatedFlag) {
	value, details := f.get(ctx, opts...)
	evaluated := &expb.EvaluatedFlag{Name: f.name}
	if details != nil {
		evaluated.Variant = details.Variant()
	}
	if err := f.setValue(evaluated, value); err != nil {
		log.CtxWarningf(ctx, "Experiment flag %q value could not be converted to a proto: %s", f.name, err)
	}
	return value, evaluated
}

// GetProto evaluates the flag and returns the result as a proto, so that
// another process can use the same value without re-evaluating the targeting
// rules. For example, the app sends these to executors in the ExecutionTask.
func (f *Flag[T]) GetProto(ctx context.Context, opts ...any) *expb.EvaluatedFlag {
	_, evaluated := f.GetWithDetails(ctx, opts...)
	return evaluated
}

func (f *Flag[T]) get(ctx context.Context, opts ...any) (T, interfaces.ExperimentFlagDetails) {
	p := provider.Load()
	if p == nil {
		return *f.defaultValue, nil
	}
	value, details := f.evaluate(*p, ctx, f.name, *f.defaultValue, opts...)
	if nf, ok := details.(flagNotFoundReporter); ok && nf.FlagNotFound() && f.deprecatedExperimentName != "" {
		value, details = f.evaluate(*p, ctx, f.deprecatedExperimentName, *f.defaultValue, opts...)
	}
	return value, details
}

type evaluatedFlagsKey struct{}

// contextFlag is a flag attached to a context by ContextWithEvaluatedFlags.
type contextFlag struct {
	proto *expb.EvaluatedFlag
	// object is set if the value is an object.
	object *lazyObject
}

// lazyObject converts an object flag's value to a map on the first read and
// reuses it afterwards, so that tasks pay for the conversion only if they read
// the flag, and only once.
type lazyObject struct {
	once  sync.Once
	proto *structpb.Struct
	value map[string]any
}

// ContextWithEvaluatedFlags attaches flags evaluated by another process to
// ctx, for use by the provider returned by NewContextProvider. Flags attached
// to ctx previously are replaced, so an empty list leaves ctx with no flags.
func ContextWithEvaluatedFlags(ctx context.Context, flags []*expb.EvaluatedFlag) context.Context {
	byName := make(map[string]contextFlag, len(flags))
	for _, f := range flags {
		cf := contextFlag{proto: f}
		if v, ok := f.GetValue().(*expb.EvaluatedFlag_ObjectValue); ok {
			cf.object = &lazyObject{proto: v.ObjectValue}
		}
		byName[f.GetName()] = cf
	}
	return context.WithValue(ctx, evaluatedFlagsKey{}, byName)
}

// contextProvider reads flag values attached to the context by
// ContextWithEvaluatedFlags.
type contextProvider struct{}

// NewContextProvider returns a provider that reads flag values attached to the
// context by ContextWithEvaluatedFlags. Executors use this so that each
// experiment is evaluated once, by the app, and the executor sees the same
// result. Flags missing from the context, or whose value is a different type
// than requested, return the default value.
func NewContextProvider() FlagProvider {
	return contextProvider{}
}

func (contextProvider) BooleanDetails(ctx context.Context, name string, defaultValue bool, _ ...any) (bool, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name).proto
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_BoolValue); ok {
		return v.BoolValue, evaluatedFlagDetails{f}
	}
	return defaultValue, unusableFlagDetails(f)
}

func (contextProvider) StringDetails(ctx context.Context, name string, defaultValue string, _ ...any) (string, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name).proto
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_StringValue); ok {
		return v.StringValue, evaluatedFlagDetails{f}
	}
	return defaultValue, unusableFlagDetails(f)
}

func (contextProvider) Int64Details(ctx context.Context, name string, defaultValue int64, _ ...any) (int64, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name).proto
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_Int64Value); ok {
		return v.Int64Value, evaluatedFlagDetails{f}
	}
	return defaultValue, unusableFlagDetails(f)
}

func (contextProvider) Float64Details(ctx context.Context, name string, defaultValue float64, _ ...any) (float64, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name).proto
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_Float64Value); ok {
		return v.Float64Value, evaluatedFlagDetails{f}
	}
	return defaultValue, unusableFlagDetails(f)
}

func (contextProvider) ObjectDetails(ctx context.Context, name string, defaultValue map[string]any, _ ...any) (map[string]any, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name)
	if o := f.object; o != nil {
		o.once.Do(func() { o.value = o.proto.AsMap() })
		return o.value, evaluatedFlagDetails{f.proto}
	}
	return defaultValue, unusableFlagDetails(f.proto)
}

func evaluatedFlagFromContext(ctx context.Context, name string) contextFlag {
	flags, _ := ctx.Value(evaluatedFlagsKey{}).(map[string]contextFlag)
	return flags[name]
}

// evaluatedFlagDetails implements interfaces.ExperimentFlagDetails for the
// context provider. It holds the proto pointer rather than the variant string,
// because returning a non-empty string as an interface allocates. A nil flag
// means that the flag is not attached to the context.
type evaluatedFlagDetails struct {
	flag *expb.EvaluatedFlag
}

func (d evaluatedFlagDetails) Variant() string {
	return d.flag.GetVariant()
}

func (d evaluatedFlagDetails) FlagNotFound() bool {
	return d.flag == nil
}

// unusableFlagDetails returns details for a flag whose value the context
// provider cannot use. Only a flag missing from the context is reported as not
// found, so that an unset value or a value of the wrong type does not fall back
// to a deprecated experiment name.
func unusableFlagDetails(f *expb.EvaluatedFlag) interfaces.ExperimentFlagDetails {
	if f == nil {
		return evaluatedFlagDetails{}
	}
	return nil
}
