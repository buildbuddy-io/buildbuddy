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
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/open-feature/go-sdk/openfeature"

	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
)

var (
	appName      = flag.String("experiments.app_name", "buildbuddy-app", "Client name to use for experiments")
	flagdBackend = flag.String("experiments.flagd_backend", "", "Flagd backend to use for evaluating flags")
)

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

func NewFlagProvider(clientName string) (*FlagProvider, error) {
	return &FlagProvider{
		client: openfeature.NewClient(clientName),
	}, nil
}

type FlagProvider struct {
	client *openfeature.Client
}

func (fp *FlagProvider) Statusz(ctx context.Context) string {
	var b strings.Builder
	fmt.Fprintf(&b, "<div>Flagd backend: %q</div>", *flagdBackend)
	fmt.Fprintf(&b, "<div>Configured Provider: %v</div>", openfeature.GetApiInstance().GetProviderMetadata())
	fmt.Fprintf(&b, "<div>example-experiment-enabled: %t</div>", fp.Boolean(ctx, "example-experiment-enabled", false))
	return b.String()
}

type Options struct {
	targetingKey string
	attributes   map[string]interface{}
}
type Option func(*Options)

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
	log.Printf("eval context: %+v", evalContext)
	return evalContext
}

func WithContext(key string, value interface{}) Option {
	return func(o *Options) {
		o.attributes[key] = value
	}
}

func (fp *FlagProvider) Boolean(ctx context.Context, flagName string, defaultValue bool, opts ...any) bool {
	return fp.client.Boolean(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}
func (fp *FlagProvider) String(ctx context.Context, flagName string, defaultValue string, opts ...any) string {
	return fp.client.String(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}
func (fp *FlagProvider) Float64(ctx context.Context, flagName string, defaultValue float64, opts ...any) float64 {
	return fp.client.Float(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}
func (fp *FlagProvider) Int64(ctx context.Context, flagName string, defaultValue int64, opts ...any) int64 {
	return fp.client.Int(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
}
