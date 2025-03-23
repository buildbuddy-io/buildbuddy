package experiments

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/open-feature/go-sdk/openfeature"

	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
)

var (
	appName      = flag.String("experiments.app_name", "buildbuddy-app", "Client name to use for experiments")
	flagdBackend = flag.String("experiments.flagd_backend", "", "Flagd backend to use for evaluating flags")
)

// Register adds a new interfaces.ExperimentFlagProvider to the env. If the
// `experiments.flagd_backend` is set, this will be a real experiment provider,
// otherwise it will be a noop-provider. This function will not return until
// the configured provider is ready.
func Register(env *real_environment.RealEnv) error {
	var provider openfeature.FeatureProvider = openfeature.NoopProvider{}
	if *flagdBackend != "" {
		provider = flagd.NewProvider(flagd.WithTargetUri(*flagdBackend))
	}

	if err := openfeature.SetProviderAndWait(provider); err != nil {
		return err
	}
	fp, err := NewFlagProvider(*appName)
	if err != nil {
		return err
	}
	env.SetExperimentFlagProvider(fp)
	statusz.AddSection("experiments", "Configured experiments config", fp)
	return nil
}

// NewFlagProvider creates a new ExperimentFlagProvider, it will use whatever
// flag provider is installed in openfeature.
func NewFlagProvider(clientName string) (*FlagProvider, error) {
	return &FlagProvider{
		client: openfeature.NewClient(clientName),
	}, nil
}

// FlagProvider implements the interface.ExperimentFlagProvider interface.
type FlagProvider struct {
	client *openfeature.Client
}

// Statusz reports a simple statusz page so it's clear on a running app which
// experiments are configured.
func (fp *FlagProvider) Statusz(ctx context.Context) string {
	var b strings.Builder
	fmt.Fprintf(&b, "<div>Flagd backend: %q</div>", *flagdBackend)
	fmt.Fprintf(&b, "<div>Configured Provider: %v</div>", openfeature.GetApiInstance().GetProviderMetadata())
	fmt.Fprintf(&b, "<div>example-experiment-enabled: %t</div>", fp.Boolean(ctx, "example-experiment-enabled", false))
	return b.String()
}

// Options is evaluated by a set of Option functions, which configure it.
type Options struct {
	targetingKey string
	attributes   map[string]interface{}
}
type Option func(*Options)

// getEvaluationContext returns EvaluationContext which is basically a map of
// keys (strings) and values (any type) that can be evaluated by a configured
// flag. By default, the following flags are extracted from the provided
// context:
//   - group_id: this as used as the target key (default ID) and also provided
//     as an attribute. Parsed from claims.
//   - user_id: Parsed from claims.
//   - invocation_id: Parsed from the bazel request metadata, if set.
//   - action_id: Parsed from the bazel request metadata, if set.
//
// The fields allow enabling features at the group level (default), or by
// user, invocation, or action. Care should be taken to not enable experiments
// in a way that would be confusing to users.
func (fp *FlagProvider) getEvaluationContext(ctx context.Context, opts ...any) openfeature.EvaluationContext {
	options := &Options{
		targetingKey: interfaces.AuthAnonymousUser,
		attributes:   make(map[string]interface{}, 0),
	}
	for _, optI := range opts {
		if opt, ok := optI.(Option); ok {
			opt(options)
		}
	}

	if claims, err := claims.ClaimsFromContext(ctx); err == nil {
		options.targetingKey = claims.GetGroupID()
		options.attributes["group_id"] = claims.GetGroupID()
		options.attributes["user_id"] = claims.GetUserID()
	}
	if iid := bazel_request.GetInvocationID(ctx); len(iid) > 0 {
		options.attributes["invocation_id"] = iid
	}
	if aid := bazel_request.GetActionID(ctx); len(aid) > 0 {
		options.attributes["action_id"] = aid
	}

	evalContext := openfeature.NewEvaluationContext(options.targetingKey, options.attributes)
	return evalContext
}

// WithContext adds the provided key and value into the experiment context when
// the flag is evaluated. This allows selectively enabling flags only when they
// make sense. For example, you might want to only enable a certain performance
// optimization if the platform is linux, etc.
func WithContext(key string, value interface{}) Option {
	return func(o *Options) {
		o.attributes[key] = value
	}
}

// Boolean extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Boolean value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) Boolean(ctx context.Context, flagName string, defaultValue bool, opts ...any) bool {
	return fp.client.Boolean(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}

// String extracts the evaluationContext from ctx, applies any option
// overrides, and returns the String value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) String(ctx context.Context, flagName string, defaultValue string, opts ...any) string {
	return fp.client.String(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}

// Float64 extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Float64 value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) Float64(ctx context.Context, flagName string, defaultValue float64, opts ...any) float64 {
	return fp.client.Float(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}

// Int64 extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Int64 value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) Int64(ctx context.Context, flagName string, defaultValue int64, opts ...any) int64 {
	return fp.client.Int(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}
