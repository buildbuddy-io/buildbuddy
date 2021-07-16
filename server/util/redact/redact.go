package redact

import (
	"context"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/golang/protobuf/proto"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
)

const (
	RedactionFlagAPIKey = 1 << iota
)

func RedactAPIKey(ctx context.Context, env environment.Env, event *bespb.BuildEvent) error {
	proto.DiscardUnknown(event)

	apiKey, ok := ctx.Value("x-buildbuddy-api-key").(string)
	if !ok {
		return nil
	}

	txt := proto.MarshalTextString(event)
	// TODO: Show the display label of the API key that was redacted, if we can
	// get that info efficiently.
	txt = strings.ReplaceAll(txt, apiKey, "<REDACTED>")

	return proto.UnmarshalText(txt, event)
}

func RedactAPIKeysWithSlowRegexp(ctx context.Context, env environment.Env, event *bespb.BuildEvent) error {
	proto.DiscardUnknown(event)
	txt := proto.MarshalTextString(event)

	// NB: this implementation depends on the way we generate API keys
	// (20 alphanumeric characters).

	// Replace x-buildbuddy-api-key header.
	pat := regexp.MustCompile("x-buildbuddy-api-key=[[:alnum:]]{20}")
	txt = pat.ReplaceAllLiteralString(txt, "x-buildbuddy-api-key=<REDACTED>")

	// Replace sequences that look like API keys immediately followed by '@',
	// to account for patterns like "grpc://$API_KEY@app.buildbuddy.io"
	// or "bes_backend=$API_KEY@domain.com".

	// Here we match 20 alphanum chars occurring at the start of a line.
	pat = regexp.MustCompile("^[[:alnum:]]{20}@")
	txt = pat.ReplaceAllLiteralString(txt, "<REDACTED>@")
	// Here we match 20 alphanum chars anywhere in the line, preceded by a non-
	// alphanum char (to ensure the match is exactly 20 alphanum chars long).
	pat = regexp.MustCompile("([^[:alnum:]])[[:alnum:]]{20}@")
	txt = pat.ReplaceAllString(txt, "$1<REDACTED>@")

	// Replace the literal API key in the configuration, which does not need
	// to conform to the way we generate API keys.
	configuredKey := getConfiguredAPIKey(env)
	if configuredKey != "" {
		txt = strings.ReplaceAll(txt, configuredKey, "<REDACTED>")
	}

	contextKey, ok := ctx.Value("x-buildbuddy-api-key").(string)
	if ok {
		txt = strings.ReplaceAll(txt, contextKey, "<REDACTED>")
	}

	return proto.UnmarshalText(txt, event)
}

func getConfiguredAPIKey(env environment.Env) string {
	if apiConfig := env.GetConfigurator().GetAPIConfig(); apiConfig != nil {
		return apiConfig.APIKey
	}
	return ""
}
