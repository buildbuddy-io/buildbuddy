// Package expflag provides a flag-like API for experiments.
//
// Experiments are declared at the package level, like command-line flags:
//
//	var widgetColor = expflag.String("app.widget_color", "blue", "Widget color.")
//
// Evaluating an experiment requires a context, because the value depends on
// request attributes such as the authenticated group ID:
//
//	frontendConfig.WidgetColor = widgetColor.Get(ctx)
//
// Multi-arm experiments can use GetWithDetails to also get the selected
// variant:
//
//	color, details := widgetColor.GetWithDetails(ctx)
//	successMetric.WithLabelValues(details.GetVariant()).Inc()
//
// Binaries call SetFlagProvider during initialization to install the
// interfaces.ExperimentFlagProvider that evaluates flags. If no provider is
// set, which is the case for the OSS app, every flag returns its default
// value.
//
// Each experiment flag also declares a command-line flag with the same name,
// so the default can be changed without a rebuild, from the command line or
// from the YAML config, for example -app.widget_color=red. Tests can override
// defaults using the normal flag test helpers, such as flags.Set. Because
// each experiment declares a command-line flag, declaring two experiments with
// the same name panics.
//
// Executors do not evaluate experiments themselves. Instead, the app evaluates
// executor-visible experiments using GetProto and sends the results in the
// ExecutionTask. The executor attaches these to the task's context using
// ContextWithEvaluatedFlags and installs NewContextProvider as its flag
// provider, so the same Get calls work in both processes.
package expflag

import (
	"context"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
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

// Flag is an experiment flag with a value of type T. Declare flags at the
// package level using Bool, String, Int64, Float64, or Object.
type Flag[T any] struct {
	name string
	// Configured default, backed by the command-line flag of the same name.
	defaultValue *T
	// evaluate is the FlagProvider method that evaluates values of type T.
	evaluate func(p FlagProvider, ctx context.Context, name string, defaultValue T, opts ...any) (T, interfaces.ExperimentFlagDetails)
	// setValue stores a value of type T in the value oneof of an EvaluatedFlag.
	setValue func(f *expb.EvaluatedFlag, value T) error
}

// Bool declares a boolean experiment flag.
func Bool(name string, defaultValue bool, help string) *Flag[bool] {
	return &Flag[bool]{
		name:         name,
		defaultValue: flag.Bool(name, defaultValue, help),
		evaluate:     FlagProvider.BooleanDetails,
		setValue: func(f *expb.EvaluatedFlag, value bool) error {
			f.Value = &expb.EvaluatedFlag_BoolValue{BoolValue: value}
			return nil
		},
	}
}

// String declares a string experiment flag.
func String(name string, defaultValue string, help string) *Flag[string] {
	return &Flag[string]{
		name:         name,
		defaultValue: flag.String(name, defaultValue, help),
		evaluate:     FlagProvider.StringDetails,
		setValue: func(f *expb.EvaluatedFlag, value string) error {
			f.Value = &expb.EvaluatedFlag_StringValue{StringValue: value}
			return nil
		},
	}
}

// Int64 declares an int64 experiment flag.
func Int64(name string, defaultValue int64, help string) *Flag[int64] {
	return &Flag[int64]{
		name:         name,
		defaultValue: flag.Int64(name, defaultValue, help),
		evaluate:     FlagProvider.Int64Details,
		setValue: func(f *expb.EvaluatedFlag, value int64) error {
			f.Value = &expb.EvaluatedFlag_Int64Value{Int64Value: value}
			return nil
		},
	}
}

// Float64 declares a float64 experiment flag.
func Float64(name string, defaultValue float64, help string) *Flag[float64] {
	return &Flag[float64]{
		name:         name,
		defaultValue: flag.Float64(name, defaultValue, help),
		evaluate:     FlagProvider.Float64Details,
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

// Object declares an experiment flag whose value is a JSON object.
func Object(name string, defaultValue map[string]any, help string) *Flag[map[string]any] {
	value := flag.New(flag.CommandLine, name, objectValue(defaultValue), help)
	return &Flag[map[string]any]{
		name:         name,
		defaultValue: (*map[string]any)(value),
		evaluate:     FlagProvider.ObjectDetails,
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
	value, _ := f.GetWithDetails(ctx, opts...)
	return value
}

// GetWithDetails evaluates the flag and returns its value along with the
// evaluation details, which include the selected variant.
func (f *Flag[T]) GetWithDetails(ctx context.Context, opts ...any) (T, *expb.EvaluatedFlag) {
	value := *f.defaultValue
	evaluated := &expb.EvaluatedFlag{Name: f.name}
	if p := provider.Load(); p != nil {
		var details interfaces.ExperimentFlagDetails
		value, details = f.evaluate(*p, ctx, f.name, value, opts...)
		if details != nil {
			evaluated.Variant = details.Variant()
		}
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

type evaluatedFlagsKey struct{}

// ContextWithEvaluatedFlags attaches flags evaluated by another process to
// ctx, for use by the provider returned by NewContextProvider. Flags attached
// to ctx previously are replaced, so an empty list leaves ctx with no flags.
func ContextWithEvaluatedFlags(ctx context.Context, flags []*expb.EvaluatedFlag) context.Context {
	byName := make(map[string]*expb.EvaluatedFlag, len(flags))
	for _, f := range flags {
		byName[f.GetName()] = f
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
	f := evaluatedFlagFromContext(ctx, name)
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_BoolValue); ok {
		return v.BoolValue, variant(f.GetVariant())
	}
	return defaultValue, variant("")
}

func (contextProvider) StringDetails(ctx context.Context, name string, defaultValue string, _ ...any) (string, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name)
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_StringValue); ok {
		return v.StringValue, variant(f.GetVariant())
	}
	return defaultValue, variant("")
}

func (contextProvider) Int64Details(ctx context.Context, name string, defaultValue int64, _ ...any) (int64, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name)
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_Int64Value); ok {
		return v.Int64Value, variant(f.GetVariant())
	}
	return defaultValue, variant("")
}

func (contextProvider) Float64Details(ctx context.Context, name string, defaultValue float64, _ ...any) (float64, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name)
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_Float64Value); ok {
		return v.Float64Value, variant(f.GetVariant())
	}
	return defaultValue, variant("")
}

func (contextProvider) ObjectDetails(ctx context.Context, name string, defaultValue map[string]any, _ ...any) (map[string]any, interfaces.ExperimentFlagDetails) {
	f := evaluatedFlagFromContext(ctx, name)
	if v, ok := f.GetValue().(*expb.EvaluatedFlag_ObjectValue); ok {
		return v.ObjectValue.AsMap(), variant(f.GetVariant())
	}
	return defaultValue, variant("")
}

func evaluatedFlagFromContext(ctx context.Context, name string) *expb.EvaluatedFlag {
	flags, _ := ctx.Value(evaluatedFlagsKey{}).(map[string]*expb.EvaluatedFlag)
	return flags[name]
}

// variant implements interfaces.ExperimentFlagDetails for the context
// provider.
type variant string

func (v variant) Variant() string {
	return string(v)
}
