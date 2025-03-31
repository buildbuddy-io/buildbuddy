package experiments_test

import (
	"context"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"

	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	openfeatureTesting "github.com/open-feature/go-sdk/openfeature/testing"
)

func TestNoopProviderProvidesDefaults(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	err := experiments.Register(te)
	require.NoError(t, err)

	fp := te.GetExperimentFlagProvider()
	require.False(t, fp.Boolean(ctx, "my_flag", false))
	require.Equal(t, "default", fp.String(ctx, "my_flag", "default"))
	require.Equal(t, int64(0), fp.Int64(ctx, "my_flag", 0))
	require.Equal(t, 1.0, fp.Float64(ctx, "my_flag", 1.0))
}

func TestPrimitiveFlags(t *testing.T) {
	ctx := context.Background()

	testProvider := openfeatureTesting.NewTestProvider()
	openfeature.SetProviderAndWait(testProvider)
	defer testProvider.Cleanup()

	testProvider.UsingFlags(t, map[string]memprovider.InMemoryFlag{
		"bool_flag": {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants: map[string]any{
				"on":  true,
				"off": false,
			},
		},
		"string_flag": {
			State:          memprovider.Enabled,
			DefaultVariant: "foo",
			Variants: map[string]any{
				"foo": "value-is-foo",
				"bar": "value-is-bar",
			},
		},
		"int_flag": {
			State:          memprovider.Enabled,
			DefaultVariant: "small",
			Variants: map[string]any{
				"small": 1,
				"big":   100,
			},
		},
		"float_flag": {
			State:          memprovider.Enabled,
			DefaultVariant: "big",
			Variants: map[string]any{
				"small": 1.1,
				"big":   99.9999999,
			},
		},
	})

	fp, err := experiments.NewFlagProvider("test-name")
	require.NoError(t, err)

	require.True(t, fp.Boolean(ctx, "bool_flag", false))
	require.Equal(t, "value-is-foo", fp.String(ctx, "string_flag", "default"))
	require.Equal(t, int64(1), fp.Int64(ctx, "int_flag", 0))
	require.Equal(t, 99.9999999, fp.Float64(ctx, "float_flag", 1.0))
}

func writeFlagConfig(t testing.TB, data string) string {
	t.Helper()
	f, err := os.CreateTemp(os.Getenv("TEST_TMPDIR"), "buildbuddy-test-file-*.flagd.json")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = f.Close()
		os.RemoveAll(path)
	})
	return path
}

func TestSelection(t *testing.T) {
	t.Skip() // fix flagd race
	ctx := context.Background()

	// generate configs at https://flagd.dev/playground/
	var testFlags = `{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "enable-mainframe-access": {
      "state": "ENABLED",
      "defaultVariant": "false",
      "variants": {
        "true": true,
        "false": false
      },
      "targeting": {
        "if": [
          {
            "ends_with": [
              {
                "var": "email"
              },
              "@buildbuddy.io"
            ]
          },
          "true"
        ]
      }
    }
  }
}
`
	offlineFlagPath := writeFlagConfig(t, testFlags)
	provider := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
	openfeature.SetProviderAndWait(provider)

	fp, err := experiments.NewFlagProvider("test-name")
	require.NoError(t, err)

	// A user without the targeting var 'email' should not get the flag, but
	// a user with it should.
	require.False(t, fp.Boolean(ctx, "enable-mainframe-access", false))
	require.False(t, fp.Boolean(ctx, "enable-mainframe-access", false, experiments.WithContext("email", "joeblow@google.com")))
	require.True(t, fp.Boolean(ctx, "enable-mainframe-access", false, experiments.WithContext("email", "tyler@buildbuddy.io")))
}

func contextWithUnverifiedJWT(c *claims.Claims) context.Context {
	authCtx := claims.AuthContextFromClaims(context.Background(), c, nil)
	jwt := authCtx.Value(authutil.ContextTokenStringKey).(string)
	return context.WithValue(context.Background(), authutil.ContextTokenStringKey, jwt)
}

func TestMultiVariant(t *testing.T) {
	t.Skip() // fix flagd race
	// generate configs at https://flagd.dev/playground/
	var testFlags = `{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "color-palette-experiment": {
      "state": "ENABLED",
      "defaultVariant": "grey",
      "variants": {
        "red": "#b91c1c",
        "blue": "#0284c7",
        "green": "#16a34a",
        "grey": "#4b5563"
      },
      "targeting": {
        "fractional": [
          [
            "red",
            25
          ],
          [
            "blue",
            25
          ],
          [
            "green",
            25
          ],
          [
            "grey",
            25
          ]
        ]
      }
    }
  }
}
`
	offlineFlagPath := writeFlagConfig(t, testFlags)
	provider := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
	openfeature.SetProviderAndWait(provider)

	fp, err := experiments.NewFlagProvider("test-name")
	require.NoError(t, err)

	counts := map[string]int{}
	tables.RegisterTables()
	// Roughly a quarter of users should get put into each group.
	for range 1000 {
		// GroupID is used as targeting key, so generate a claims and
		// attach it to the context with a random group ID.
		gid, err := tables.PrimaryKeyForTable("Groups")
		require.NoError(t, err)
		ctx := contextWithUnverifiedJWT(&claims.Claims{GroupID: gid})

		color := fp.String(ctx, "color-palette-experiment", "no-color")
		counts[color]++
	}
	for color, count := range counts {
		require.GreaterOrEqual(t, count, 200, color)
	}
}
