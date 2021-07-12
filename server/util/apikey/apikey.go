package apikey

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

const (
	// GeneratedAPIKeyLength is the fixed length of generated API key tokens.
	GeneratedAPIKeyLength = 20

	apiKeyRedactionMarker = "<REDACTED>"
)

func RedactAll(env environment.Env, text string) string {
	// NB: this implementation depends on the way we generate API keys
	// (20 alphanumeric characters).

	// Replace x-buildbuddy-api-key header.
	pat := regexp.MustCompile(fmt.Sprintf("x-buildbuddy-api-key=[[:alnum:]]{%d}", GeneratedAPIKeyLength))
	text = pat.ReplaceAllLiteralString(text, fmt.Sprintf("x-buildbuddy-api-key=%s", apiKeyRedactionMarker))

	// Replace sequences that look like API keys immediately followed by '@',
	// to account for patterns like "grpc://$API_KEY@app.buildbuddy.io"
	// or "bes_backend=$API_KEY@domain.com".

	text = redactAPIKeyBasicAuth(text, apiKeyRedactionMarker)

	// Replace the literal API key in the configuration, which does not need
	// to conform to the way we generate API keys.
	configuredKey := getConfiguredAPIKey(env)
	if configuredKey != "" {
		text = strings.ReplaceAll(text, configuredKey, apiKeyRedactionMarker)
	}
	return text
}

// Replace the API key as it appears in patterns like `{key}@buildbuddy.io`,
// `grpc://{key}@buildbuddy.io`, etc. Specifically, look for *exactly* 20
// alphanumeric characters followed by "@" and replace the 20 chars with
// the replacement string.
func redactAPIKeyBasicAuth(text, replacement string) string {
	out := make([]byte, 0, len(text))
	alnumCount := 0
	for _, b := range []byte(text) {
		// NOTE: in UTF-8, all non-ASCII characters start with 1 in the highest bit
		// position, so it's OK to iterate over bytes instead of runes here.
		// (Iterating over runes is significantly slower.)
		if (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') {
			alnumCount++
		} else {
			if b == '@' && alnumCount == GeneratedAPIKeyLength {
				out = out[:len(out)-GeneratedAPIKeyLength]
				out = append(out, []byte(replacement)...)
			}
			alnumCount = 0
		}
		out = append(out, b)
	}
	return string(out)
}

func getConfiguredAPIKey(env environment.Env) string {
	if apiConfig := env.GetConfigurator().GetAPIConfig(); apiConfig != nil {
		return apiConfig.APIKey
	}
	return ""
}
