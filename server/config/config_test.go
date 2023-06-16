package config_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

func replaceFlagsForTesting(t *testing.T) *flag.FlagSet {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	common.DefaultFlagSet = flags

	t.Cleanup(func() {
		common.DefaultFlagSet = flag.CommandLine
	})

	return flags
}

type fakeSecretProvider struct {
	secrets map[string]string
}

func (f *fakeSecretProvider) GetSecret(ctx context.Context, name string) ([]byte, error) {
	secret, ok := f.secrets[name]
	if !ok {
		return nil, status.NotFoundErrorf("secret %q not found", name)
	}
	return []byte(secret), nil
}

type secretHolder struct {
	Secret string
}

func TestExpansion(t *testing.T) {
	defer func() {
		config.SecretProvider = nil
	}()

	// Basic environment variable expansion.
	{
		flags := replaceFlagsForTesting(t)
		os.Setenv("SOMEENV", "foo")
		envFlag := flags.String("env_flag", "", "")
		err := config.LoadFromData(strings.TrimSpace(`
			env_flag: ${SOMEENV}
		`))
		require.NoError(t, err)
		require.Equal(t, "foo", *envFlag)
	}

	// Basic environment variable expansion via flag.
	{
		flags := replaceFlagsForTesting(t)
		os.Setenv("SOMEENV", "foo2")
		envFlag := flags.String("env_flag", "", "")
		flags.Set("env_flag", "${SOMEENV}")
		err := config.LoadFromData("")
		require.NoError(t, err)
		require.Equal(t, "foo2", *envFlag)
	}

	// Multiline environment variable expansion.
	{
		flags := replaceFlagsForTesting(t)
		envFlag := flags.String("env_flag", "", "")
		os.Setenv("SOMEENV", "foo\nbar")
		err := config.LoadFromData(strings.TrimSpace(`
			env_flag: ${SOMEENV}
		`))
		require.NoError(t, err)
		require.Equal(t, "foo\nbar", *envFlag)
	}

	// Multiline environment variable expansion via flag.
	{
		flags := replaceFlagsForTesting(t)
		envFlag := flags.String("env_flag", "", "")
		os.Setenv("SOMEENV", "foo\nbar")
		flags.Set("env_flag", "${SOMEENV}")
		err := config.LoadFromData("")
		require.NoError(t, err)
		require.Equal(t, "foo\nbar", *envFlag)
	}

	// Secret reference but no secret provider.
	{
		flags := replaceFlagsForTesting(t)
		flags.String("secret_flag", "", "")
		err := config.LoadFromData(strings.TrimSpace(`
			secret_flag: ${SECRET:FOO}
		`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no secret provider")
	}

	// Secret reference to non-existent secret.
	{
		config.SecretProvider = &fakeSecretProvider{}
		err := config.LoadFromData(strings.TrimSpace(`
			secret_flag: ${SECRET:FOO}
		`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	}

	// Basic secret expansion.
	{
		replaceFlagsForTesting(t)
		secretFlag := flag.String("secret_flag", "", "", flag.Secret)
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO": "BAR"},
		}
		err := config.LoadFromData(strings.TrimSpace(`
			secret_flag: ${SECRET:FOO}
		`))
		require.NoError(t, err)
		require.Equal(t, "BAR", *secretFlag)
	}

	// Basic secret expansion via flag.
	{
		flags := replaceFlagsForTesting(t)
		secretFlag := flags.String("secret_flag", "", "")
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO": "BAR"},
		}
		flags.Set("secret_flag", "${SECRET:FOO}")
		err := config.LoadFromData("")
		require.NoError(t, err)
		require.Equal(t, "BAR", *secretFlag)
	}

	// Multiline secret expansion.
	{
		flags := replaceFlagsForTesting(t)
		secretFlag := flags.String("secret_flag", "", "")
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO": "BAR\nBAZ"},
		}
		err := config.LoadFromData(strings.TrimSpace(`
			secret_flag: ${SECRET:FOO}
		`))
		require.NoError(t, err)
		require.Equal(t, "BAR\nBAZ", *secretFlag)
	}

	// Multiline secret expansion via flag.
	{
		flags := replaceFlagsForTesting(t)
		secretFlag := flags.String("secret_flag", "", "")
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO": "BAR\nBAZ"},
		}
		flags.Set("secret_flag", "${SECRET:FOO}")
		err := config.LoadFromData("")
		require.NoError(t, err)
		require.Equal(t, "BAR\nBAZ", *secretFlag)
	}

	// Secrets inside a list.
	{
		replaceFlagsForTesting(t)
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO1": "FIRST\nSECRET", "FOO2": "SECOND\nSECRET"},
		}
		secretSliceFlag := flag.Slice("secret_slice_flag", []string{}, "")
		err := config.LoadFromData(strings.TrimSpace(`
secret_slice_flag: 
  - ${SECRET:FOO1}
  - ${SECRET:FOO2}
	`))
		require.NoError(t, err)
		require.Equal(t, []string{"FIRST\nSECRET", "SECOND\nSECRET"}, *secretSliceFlag)
	}

	// Secrets inside a list, via flag.
	{
		flags := replaceFlagsForTesting(t)
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO1": "FIRST\nSECRET", "FOO2": "SECOND\nSECRET"},
		}
		secretSliceFlag := flag.Slice("secret_slice_flag", []string{}, "")
		flags.Set("secret_slice_flag", "${SECRET:FOO1},${SECRET:FOO2}")
		err := config.LoadFromData("")
		require.NoError(t, err)
		require.Equal(t, []string{"FIRST\nSECRET", "SECOND\nSECRET"}, *secretSliceFlag)
	}

	// Secrets inside a struct slice.
	{
		replaceFlagsForTesting(t)
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO1": "FIRST\nSECRET", "FOO2": "SECOND\nSECRET"},
		}
		secretStructSliceFlag := flag.Slice("secret_struct_slice_flag", []secretHolder{}, "")
		err := config.LoadFromData(strings.TrimSpace(`
secret_struct_slice_flag: 
  - secret: ${SECRET:FOO1}
  - secret: ${SECRET:FOO2}
	`))
		require.NoError(t, err)
		require.Equal(t, []secretHolder{{Secret: "FIRST\nSECRET"}, {Secret: "SECOND\nSECRET"}}, *secretStructSliceFlag)
	}

	// Secrets inside a struct slice, via flag.
	{
		flags := replaceFlagsForTesting(t)
		config.SecretProvider = &fakeSecretProvider{
			secrets: map[string]string{"FOO1": "FIRST\nSECRET", "FOO2": "SECOND\nSECRET"},
		}
		secretStructSliceFlag := flag.Slice("secret_struct_slice_flag", []secretHolder{}, "")
		err := flags.Set("secret_struct_slice_flag", `[{"Secret":"${SECRET:FOO1}"},{"Secret":"${SECRET:FOO2}"}]`)
		require.NoError(t, err)
		err = config.LoadFromData("")
		require.NoError(t, err)
		require.Equal(t, []secretHolder{{Secret: "FIRST\nSECRET"}, {Secret: "SECOND\nSECRET"}}, *secretStructSliceFlag)
	}

}

func TestLoadFromData(t *testing.T) {
	// Can successfully parse int config item
	{
		flags := replaceFlagsForTesting(t)
		_ = flags.Int("must_be_a_number", 0, "")
		err := config.LoadFromData(strings.TrimSpace(`
must_be_a_number: 4
	`))
		require.NoError(t, err)
	}

	// Parse error when config value causes type mismatch
	{
		flags := replaceFlagsForTesting(t)
		_ = flags.Int("must_be_a_number", 0, "")
		err := config.LoadFromData(strings.TrimSpace(`
must_be_a_number: "not a string, like this"
	`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "retyping YAML map")
		require.Contains(t, err.Error(), "into int")
	}
}
