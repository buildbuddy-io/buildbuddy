package experiments

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/open-feature/go-sdk/openfeature"
	"google.golang.org/protobuf/encoding/protojson"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
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
		host, port, err := net.SplitHostPort(*flagdBackend)
		if err != nil {
			return err
		}
		intPort, err := strconv.Atoi(port)
		if err != nil {
			return err
		}
		provider = flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithHost(host), flagd.WithPort(uint16(intPort)))
	}

	if err := openfeature.SetProviderAndWait(provider); err != nil {
		return err
	}
	// TODO: register shutdown function to close the provider?
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

// details implements the interfaces.ExperimentFlagDetails interface,
// providing details about flag evaluation.
type details struct {
	variant string
}

// noDetails are returned when flag evaluation fails.
var noDetails = (*details)(nil)

func (d *details) Variant() string {
	if d == nil {
		return ""
	}
	return d.variant
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
//   - group_status: The group's status as a string (e.g., "FREE_TIER_GROUP_STATUS",
//     "ENTERPRISE_GROUP_STATUS"). Parsed from claims.
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

	if claims, err := claims.ClaimsFromContext(ctx); err == nil {
		options.targetingKey = claims.GetExperimentTargetingGroupID()
		options.attributes["group_id"] = claims.GetExperimentTargetingGroupID()
		options.attributes["user_id"] = claims.GetUserID()
		if status := claims.GetGroupStatus(); status != grpb.Group_UNKNOWN_GROUP_STATUS {
			options.attributes["group_status"] = grpb.Group_GroupStatus_name[int32(status)]
		}
	}
	rmd := bazel_request.GetRequestMetadata(ctx)
	if iid := rmd.GetToolInvocationId(); len(iid) > 0 {
		options.attributes["invocation_id"] = iid
	}
	if aid := rmd.GetActionId(); len(aid) > 0 {
		options.attributes["action_id"] = aid
	}
	if mnemonic := rmd.GetActionMnemonic(); mnemonic != "" {
		options.attributes["action_mnemonic"] = mnemonic
	}
	if targetID := rmd.GetTargetId(); targetID != "" {
		options.attributes["target_id"] = targetID
	}
	for _, optI := range opts {
		if opt, ok := optI.(Option); ok {
			opt(options)
		}
	}

	evalContext := openfeature.NewEvaluationContext(options.targetingKey, options.attributes)
	return evalContext
}

// ObjectToStruct is a utility function to get a Go struct from an object
// returned by [interfaces.ExperimentFlagProvider.Object] or
// [interfaces.ExperimentFlagProvider.ObjectDetails].
//
// It uses an intermediate JSON conversion, so any `json` tags on struct fields
// can be used to control how the object fields are unmarshaled into struct
// fields.
func ObjectToStruct(object map[string]any, dest any) error {
	b, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := json.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

// ObjectToProto is a utility function to unmarshal a proto from an object
// returned by [interfaces.ExperimentFlagProvider.Object] or
// [interfaces.ExperimentFlagProvider.ObjectDetails].
//
// For this to work, the originally specified JSON config must follow the
// ProtoJSON format. See https://protobuf.dev/programming-guides/json/
func ObjectToProto(object map[string]any, dest proto.Message) error {
	b, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := protojson.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
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

func (fp *FlagProvider) Subscribe(ch chan<- struct{}) (stop func()) {
	f := func(details openfeature.EventDetails) {
		log.Debugf("Update: %+v", details)
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	h := openfeature.EventCallback(&f)
	// Subscribe to config change events.
	openfeature.AddHandler(openfeature.ProviderConfigChange, h)
	// Transitioning from not-ready to ready may cause experiment state to
	// change; subscribe to that event too.
	openfeature.AddHandler(openfeature.ProviderReady, h)
	return func() {
		openfeature.RemoveHandler(openfeature.ProviderConfigChange, h)
		openfeature.RemoveHandler(openfeature.ProviderReady, h)
	}
}

// Boolean extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Boolean value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) Boolean(ctx context.Context, flagName string, defaultValue bool, opts ...any) bool {
	v, _ := fp.BooleanDetails(ctx, flagName, defaultValue, opts...)
	return v
}

// BooleanDetails extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Boolean value for flagName, or defaultValue if no
// experiment provider is configured. It also returns the details about the flag
// evaluation.
func (fp *FlagProvider) BooleanDetails(ctx context.Context, flagName string, defaultValue bool, opts ...any) (bool, interfaces.ExperimentFlagDetails) {
	d, err := fp.client.BooleanValueDetails(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
	if err != nil {
		log.CtxDebugf(ctx, "Experiment flag %q could not be evaluated: %v", flagName, err)
		return defaultValue, noDetails
	}
	return d.Value, &details{d.Variant}
}

// String extracts the evaluationContext from ctx, applies any option
// overrides, and returns the String value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) String(ctx context.Context, flagName string, defaultValue string, opts ...any) string {
	v, _ := fp.StringDetails(ctx, flagName, defaultValue, opts...)
	return v
}

// StringDetails extracts the evaluationContext from ctx, applies any option
// overrides, and returns the String value for flagName, or defaultValue if no
// experiment provider is configured. It also returns the details about the flag
// evaluation.
func (fp *FlagProvider) StringDetails(ctx context.Context, flagName string, defaultValue string, opts ...any) (string, interfaces.ExperimentFlagDetails) {
	d, err := fp.client.StringValueDetails(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
	if err != nil {
		log.CtxDebugf(ctx, "Experiment flag %q could not be evaluated: %v", flagName, err)
		return defaultValue, noDetails
	}
	return d.Value, &details{d.Variant}
}

// Float64 extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Float64 value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) Float64(ctx context.Context, flagName string, defaultValue float64, opts ...any) float64 {
	v, _ := fp.Float64Details(ctx, flagName, defaultValue, opts...)
	return v
}

// Float64Details extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Float64 value for flagName, or defaultValue if no
// experiment provider is configured. It also returns the details about the flag
// evaluation.
func (fp *FlagProvider) Float64Details(ctx context.Context, flagName string, defaultValue float64, opts ...any) (float64, interfaces.ExperimentFlagDetails) {
	d, err := fp.client.FloatValueDetails(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
	if err != nil {
		log.CtxDebugf(ctx, "Experiment flag %q could not be evaluated: %v", flagName, err)
		return defaultValue, noDetails
	}
	return d.Value, &details{d.Variant}
}

// Int64 extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Int64 value for flagName, or defaultValue if no
// experiment provider is configured.
func (fp *FlagProvider) Int64(ctx context.Context, flagName string, defaultValue int64, opts ...any) int64 {
	v, _ := fp.Int64Details(ctx, flagName, defaultValue, opts...)
	return v
}

// Int64Details extracts the evaluationContext from ctx, applies any option
// overrides, and returns the Int64 value for flagName, or defaultValue if no
// experiment provider is configured. It also returns the details about the flag
// evaluation.
func (fp *FlagProvider) Int64Details(ctx context.Context, flagName string, defaultValue int64, opts ...any) (int64, interfaces.ExperimentFlagDetails) {
	d, err := fp.client.IntValueDetails(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
	if err != nil {
		log.CtxDebugf(ctx, "Experiment flag %q could not be evaluated: %v", flagName, err)
		return defaultValue, noDetails
	}
	return d.Value, &details{d.Variant}
}

// Object extracts the evaluationContext from ctx, applies any option
// overrides, and returns the map[string]any value for flagName, or defaultValue
// if no experiment provider is configured.
func (fp *FlagProvider) Object(ctx context.Context, flagName string, defaultValue map[string]any, opts ...any) map[string]any {
	m, _ := fp.ObjectDetails(ctx, flagName, defaultValue, opts...)
	return m
}

// ObjectDetails extracts the evaluationContext from ctx, applies any option
// overrides, and returns the map[string]any value for flagName, or defaultValue
// if no experiment provider is configured. It also returns the details about
// the flag evaluation.
func (fp *FlagProvider) ObjectDetails(ctx context.Context, flagName string, defaultValue map[string]any, opts ...any) (map[string]any, interfaces.ExperimentFlagDetails) {
	d, err := fp.client.ObjectValueDetails(ctx, flagName, defaultValue, fp.getEvaluationContext(ctx, opts...))
	if err != nil {
		log.CtxDebugf(ctx, "Experiment flag %q could not be evaluated: %v", flagName, err)
		return defaultValue, noDetails
	}
	v := d.Value
	if m, ok := d.Value.(map[string]any); ok {
		return m, &details{d.Variant}
	} else {
		log.CtxWarningf(ctx, "Experiment flag %q expected value of type map[string]any, but the value is %T (%v)", flagName, v, v)
	}
	return defaultValue, noDetails
}
