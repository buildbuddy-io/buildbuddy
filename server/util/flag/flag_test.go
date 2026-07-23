package flag_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type credentials struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password" config:"secret"`
}

type registry struct {
	Address     string      `yaml:"address" json:"address"`
	Credentials credentials `yaml:"credentials" json:"credentials"`
}

var (
	plainFlag        = flag.String("test.is_secret.plain", "", "")
	secretFlag       = flag.String("test.is_secret.secret", "", "", flag.Secret)
	secretFieldFlag  = flag.Slice("test.is_secret.credentials", []credentials{}, "")
	nestedSecretFlag = flag.Struct("test.is_secret.registry", registry{}, "")
)

func TestIsSecret(t *testing.T) {
	for name, want := range map[string]bool{
		// A plain flag has no secrets.
		"test.is_secret.plain": false,
		// A flag declared with the Secret tag.
		"test.is_secret.secret": true,
		// A flag whose type has a config:"secret" field.
		"test.is_secret.credentials": true,
		// A flag whose type reaches a config:"secret" field via nesting.
		"test.is_secret.registry": true,
	} {
		flg := flag.CommandLine.Lookup(name)
		require.NotNil(t, flg, name)
		assert.Equal(t, want, flag.IsSecret(flg), name)
	}
}

func TestIsSecret_SetValuesNotExposed(t *testing.T) {
	// Sanity-check the scenario IsSecret exists for: a set flag whose
	// String() serialization would include a secret struct field.
	flags.Set(t, "test.is_secret.credentials", []credentials{{Username: "user", Password: "SECRET_PASSWORD"}})
	flg := flag.CommandLine.Lookup("test.is_secret.credentials")
	require.NotNil(t, flg)
	assert.Contains(t, flg.Value.String(), "SECRET_PASSWORD")
	assert.True(t, flag.IsSecret(flg))
}
