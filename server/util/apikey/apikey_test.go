package apikey_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/apikey"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
)

const (
	generatedKey  = "aBcDe54321AbCdE12345"
	configuredKey = "password"
)

func TestRedactAll(t *testing.T) {
	flags.Set(t, "api.api_key", configuredKey)
	flags.Set(t, "api.enable_api", "true")
	env := testenv.GetTestEnv(t)

	// Test that it redacts when it should
	for _, testCase := range []struct {
		text     string
		redacted string
	}{
		{generatedKey + "@", "<REDACTED>@"},
		{"x-buildbuddy-api-key=" + generatedKey, "x-buildbuddy-api-key=<REDACTED>"},
		{"//" + generatedKey + "@", "//<REDACTED>@"},
		{configuredKey, "<REDACTED>"},
		{"x-buildbuddy-api-key=" + configuredKey, "x-buildbuddy-api-key=<REDACTED>"},
		{"A" + configuredKey + "B", "A<REDACTED>B"},
		{"//" + configuredKey + "@", "//<REDACTED>@"},
	} {
		assert.Equal(t, testCase.redacted, apikey.RedactAll(env, testCase.text))
	}
	// Test that it does not redact when it shouldn't
	for _, text := range []string{
		generatedKey,
		"//" + generatedKey[:len(generatedKey)-1] + "@",
		"A" + generatedKey + "@",
	} {
		assert.Equal(t, text, apikey.RedactAll(env, text))
	}
}
