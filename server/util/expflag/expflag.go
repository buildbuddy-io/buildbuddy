// expflag provides a flag-like API for experiments, in which the schema for
// every experiment must be fully specified at initialization time. It adds
// additional type safety, validation, documentation, and testing features on
// top of the lower-level ExperimentFlagProvider interface.
//
// Experiments are defined at the package level. The declaration includes the
// name, default value, help string, and any variables that are expected to be
// available in the evaluation context.
//
// Each expflag is added to a global registry when it is declared, similar to
// normal flags. The app will panic if the same flag name is registered more
// registered more than once. All registered flags, including their full schema,
// can also be printed by running the app with "-experiments.help". This is
// useful as a reference when reading or writing flagd configs, while also
// functioning as a catalog of available experiments.
//
// Evaluation context vars follow a pre-declared schema, including the names,
// types, and help strings of all variables included in the evaluation context.
// When running the app with "-experiments.strict_vars" enabled, expflag will
// panic if a flag is evaluated with undeclared variables, missing variables, or
// variable values with the wrong type. This adds a small amount of overhead to
// every evaluation, but is useful for unit tests and for local development.
// Config tests can also use this schema to check for any "var" references that
// are not pre-declared, which is useful for catching mistakes such as typos in
// variable names or accidentally referencing variables that might be available
// for other experiments, but not the current one.
//
// Each experiment flag also declares a normal Go flag with the same name, but
// prefixed with "experiment_flags.". This allows overriding the default
// experiment flag values using normal app config YAML files or command-line
// flags. This may be useful for changing experiment defaults in a pinch,
// without needing to rebuild or redeploy new binaries. It may also be useful
// for users who aren't running experiments, but still want control over
// experiment flag values. It may also be useful for testing (either ad-hoc or
// in unit tests), allowing experiment default values to be set on the fly
// without depending on flagd.
//
// Example usage:
//
//	// Declare a flag at the package level.
//	// This declaration includes the name, default value, help string, and
//	// any variables that are expected to be available in the evaluation context.
//	var widgetEnabled = expflag.Bool("app.settings.widget_enabled", false, "Enables a cool widget.", expflag.StringVar("locale", "The user's locale, e.g. en-US."))
//
//	// Evaluate the flag, passing a ctx and env.
//	func (s *Server) renderPage(ctx context.Context, w io.Writer) {
//	    if widgetEnabled.Get(ctx, s.env, expflag.Var("locale", getUserLocale(ctx))) {
//	        widget.WriteTo(w) // render the widget
//	    }
//	}
package expflag

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

var (
	// Keeps track of all flag values. Panics if the same flag name is
	// referenced multiple times.
	registry = make(map[string]registeredFlag)
)

// NilDetails is returned by Flag.GetWithDetails when flag details are not
// available, which can happen if experiments are disabled, a flag value is not
// configured or is disabled, or flag evaluation fails for any reason.
var NilDetails = zeroDetails{}

// Flag is the interface implemented by all experiment flags.
type Flag[T any] interface {
	// Name returns the name of the flag.
	Name() string

	// Get evaluates the flag and returns its value.
	Get(ctx context.Context, env environment.Env, opts ...any) T

	// GetWithDetails evaluates the flag and returns its value as well as
	// evaluation details, which includes things like the variant name.
	GetWithDetails(ctx context.Context, env environment.Env, opts ...any) (T, interfaces.ExperimentFlagDetails)
}

// Bool declares a boolean experiment flag.
func Bool(name string, defaultVal bool, help string, vars ...Var) Flag[bool] {
	register(name, help, defaultVal, vars...)
	return &boolFlag{name, defaultVal}
}

// String declares a string experiment flag.
func String(name string, defaultVal string, help string, vars ...Var) Flag[string] {
	register(name, help, defaultVal, vars...)
	return &stringFlag{name, defaultVal}
}

// Int64 declares an int64 experiment flag.
func Int64(name string, defaultVal int64, help string, vars ...Var) Flag[int64] {
	register(name, help, defaultVal, vars...)
	return &int64Flag{name, defaultVal}
}

// Float64 declares a float64 experiment flag.
func Float64(name string, defaultVal float64, help string, vars ...Var) Flag[float64] {
	register(name, help, defaultVal, vars...)
	return &float64Flag{name, defaultVal}
}

// Object declares an object experiment flag.
func Object(name string, defaultVal map[string]any, help string, vars ...Var) Flag[map[string]any] {
	register(name, help, defaultVal, vars...)
	return &objectFlag{name, defaultVal}
}

type Var struct {
	name string
	help string
}

var _ interfaces.ExperimentFlagDetails = NilDetails

type zeroDetails struct{}

func (zeroDetails) Variant() string {
	return ""
}

type boolFlag struct {
	name       string
	defaultVal bool
}

func (g *boolFlag) Name() string {
	return g.name
}

func (g *boolFlag) Get(ctx context.Context, env environment.Env, opts ...any) bool {
	v, _ := g.GetWithDetails(ctx, env, opts...)
	return v
}

func (g *boolFlag) GetWithDetails(ctx context.Context, env environment.Env, opts ...any) (bool, interfaces.ExperimentFlagDetails) {
	fp := env.GetExperimentFlagProvider()
	if fp == nil {
		return g.defaultVal, NilDetails
	}
	return fp.BooleanDetails(ctx, g.name, g.defaultVal, opts...)
}

type stringFlag struct {
	name       string
	defaultVal string
}

func (g *stringFlag) Name() string {
	return g.name
}

func (g *stringFlag) Get(ctx context.Context, env environment.Env, opts ...any) string {
	v, _ := g.GetWithDetails(ctx, env, opts...)
	return v
}

func (g *stringFlag) GetWithDetails(ctx context.Context, env environment.Env, opts ...any) (string, interfaces.ExperimentFlagDetails) {
	fp := env.GetExperimentFlagProvider()
	if fp == nil {
		return g.defaultVal, NilDetails
	}
	return fp.StringDetails(ctx, g.name, g.defaultVal, opts...)
}

type int64Flag struct {
	name       string
	defaultVal int64
}

func (g *int64Flag) Name() string {
	return g.name
}

func (g *int64Flag) Get(ctx context.Context, env environment.Env, opts ...any) int64 {
	v, _ := g.GetWithDetails(ctx, env, opts...)
	return v
}

func (g *int64Flag) GetWithDetails(ctx context.Context, env environment.Env, opts ...any) (int64, interfaces.ExperimentFlagDetails) {
	fp := env.GetExperimentFlagProvider()
	if fp == nil {
		return g.defaultVal, NilDetails
	}
	return fp.Int64Details(ctx, g.name, g.defaultVal, opts...)
}

type float64Flag struct {
	name       string
	defaultVal float64
}

func (g *float64Flag) Name() string {
	return g.name
}

func (g *float64Flag) Get(ctx context.Context, env environment.Env, opts ...any) float64 {
	v, _ := g.GetWithDetails(ctx, env, opts...)
	return v
}

func (g *float64Flag) GetWithDetails(ctx context.Context, env environment.Env, opts ...any) (float64, interfaces.ExperimentFlagDetails) {
	fp := env.GetExperimentFlagProvider()
	if fp == nil {
		return g.defaultVal, NilDetails
	}
	return fp.Float64Details(ctx, g.name, g.defaultVal, opts...)
}

type objectFlag struct {
	name       string
	defaultVal map[string]any
}

func (g *objectFlag) Name() string {
	return g.name
}

func (g *objectFlag) Get(ctx context.Context, env environment.Env, opts ...any) map[string]any {
	v, _ := g.GetWithDetails(ctx, env, opts...)
	return v
}

func (g *objectFlag) GetWithDetails(ctx context.Context, env environment.Env, opts ...any) (map[string]any, interfaces.ExperimentFlagDetails) {
	fp := env.GetExperimentFlagProvider()
	if fp == nil {
		return g.defaultVal, NilDetails
	}
	return fp.ObjectDetails(ctx, g.name, g.defaultVal, opts...)
}

type registeredFlag struct {
	help           string
	sourceLocation string
	defaultValue   any
}

func register(name, help string, defaultValue any, vars ...Var) {
	// TODO: enforce a consistent naming convention, e.g. lower_snake_case.
	var sourceLocation string
	_, file, line, ok := runtime.Caller(2)
	if ok {
		sourceLocation = fmt.Sprintf("%s:%d", file, line)
	} else {
		sourceLocation = "<failed to get caller>"
	}
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("flag %q already registered in %s", name, sourceLocation))
	}
	registry[name] = registeredFlag{
		help:           help,
		sourceLocation: sourceLocation,
		defaultValue:   defaultValue,
	}
}

func PrintHelp() {
	fmt.Printf("\nExperiment flags:\n\n")
	names := slices.Collect(maps.Keys(registry))
	sort.Strings(names)
	for _, name := range names {
		flag := registry[name]
		typeStr := "string"
		switch flag.defaultValue.(type) {
		case bool:
			typeStr = "bool"
		case string:
			typeStr = "string"
		case map[string]any:
			typeStr = "object"
		// TODO: in the JSON spec, floats and ints are typed "number".
		// Can flagd distinguish between them?
		case float64:
			typeStr = "float64"
		case int64:
			typeStr = "int64"
		default:
			panic(fmt.Sprintf("unexpected flag type %T", flag.defaultValue))
		}
		defaultStr, err := json.Marshal(flag.defaultValue)
		if err != nil {
			panic(fmt.Sprintf("failed to marshal default value for flag %s: %v", name, err))
		}
		fmt.Printf("-%s (%s, default %s)\n", name, typeStr, defaultStr)
		fmt.Printf("    %s\n", flag.help)
		fmt.Printf("    Source: %s\n", flag.sourceLocation)
	}
}
