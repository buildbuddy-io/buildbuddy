// TODO: figure out how to properly shut down flagd in these tests.
// https://github.com/open-feature/go-sdk/issues/397

package experiments_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
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
	require.Nil(t, fp.Object(ctx, "my_flag", nil))
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
		"object_flag": {
			State:          memprovider.Enabled,
			DefaultVariant: "foo",
			Variants: map[string]any{
				"foo": map[string]any{"foo": "foo value"},
				"bar": nil,
			},
		},
	})

	fp, err := experiments.NewFlagProvider("test-name")
	require.NoError(t, err)

	require.True(t, fp.Boolean(ctx, "bool_flag", false))
	require.Equal(t, "value-is-foo", fp.String(ctx, "string_flag", "default"))
	require.Equal(t, int64(1), fp.Int64(ctx, "int_flag", 0))
	require.Equal(t, 99.9999999, fp.Float64(ctx, "float_flag", 1.0))
	require.Equal(t, map[string]any{"foo": "foo value"}, fp.Object(ctx, "object_flag", nil))
	type ObjStruct struct {
		Foo string `json:"foo"`
	}
	var obj ObjStruct
	err = experiments.ObjectToStruct(fp.Object(ctx, "object_flag", nil), &obj)
	require.NoError(t, err)
	require.Equal(t, ObjStruct{Foo: "foo value"}, obj)

	b, d := fp.BooleanDetails(ctx, "bool_flag", false)
	require.True(t, b)
	require.Equal(t, "on", d.Variant())
	s, d := fp.StringDetails(ctx, "string_flag", "default")
	require.Equal(t, "value-is-foo", s)
	require.Equal(t, "foo", d.Variant())
	i, d := fp.Int64Details(ctx, "int_flag", 0)
	require.Equal(t, int64(1), i)
	require.Equal(t, "small", d.Variant())
	f, d := fp.Float64Details(ctx, "float_flag", 0)
	require.Equal(t, 99.9999999, f)
	require.Equal(t, "big", d.Variant())
	m, d := fp.ObjectDetails(ctx, "object_flag", nil)
	require.Equal(t, map[string]any{"foo": "foo value"}, m)
	require.Equal(t, "foo", d.Variant())
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

// TestStablePercentage ensures that once a parameter is included in an
// experiment, it remains included even as the percentage increases.
func TestStablePercentage(t *testing.T) {
	var testFlags = `{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "percent-experiment": {
      "state": "ENABLED",
      "defaultVariant": "false",
      "variants": {
        "true": true,
        "false": false
      },
      "targeting": {
        "fractional": [
			{
				"cat": [{ "var": "$flagd.flagKey" }, { "var": "experiment-param" }]
			},
			["true", %d],
			["false", %d]
		]
      }
    }
  }
}
`
	offlineFlagPath := writeFlagConfig(t, fmt.Sprintf(testFlags, 0, 100))
	writePercentConfig := func(percent int) {
		os.WriteFile(offlineFlagPath, []byte(fmt.Sprintf(testFlags, percent, 100-percent)), os.ModePerm)
	}

	ctx := context.Background()
	params := make([]string, 100)
	values := make([]bool, 100)
	for i := range params {
		p, err := random.RandomString(10)
		require.NoError(t, err)
		params[i] = p
	}
	for percent := 0; percent <= 100; percent++ {
		writePercentConfig(percent)

		// Create a new provider after each write. Otherwise the test can race
		// with the provider's internal goroutine that reads the file.
		provider := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
		openfeature.SetProviderAndWait(provider)
		fp, err := experiments.NewFlagProvider("test-name")
		require.NoError(t, err)

		for j, param := range params {
			previous := values[j]
			actual := fp.Boolean(ctx, "percent-experiment", false, experiments.WithContext("experiment-param", param))
			if percent == 0 {
				require.False(t, actual, "no params should be in experiment, but param %q is", param)
			} else if percent == 100 {
				require.True(t, actual, "all params should be in experiment, but param %q is not", param)
			} else if previous == true {
				require.True(t, actual, "param %q should remain in experiment, but went backward during %d%% rollout", param, percent)
			}
			values[j] = actual
		}
	}
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
	authCtx := claims.AuthContextWithJWT(context.Background(), c, nil)
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
	defer provider.Shutdown()

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

func TestTargetingGroupID(t *testing.T) {
	ctx := context.Background()

	const testFlags = `{
	  "$schema": "https://flagd.dev/schema/v0/flags.json",
	  "flags": {
	    "test_flag": {
	      "state": "ENABLED",
	      "variants": {
	        "override": "override",
	        "default": "default"
	      },
	      "defaultVariant": "default",
	      "targeting": {
	        "if": [
	          { "==": [{ "var": "group_id" }, "GR2"] },
	          "override",
	          "default"
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

	t.Run("should use ExperimentTargetingGroupID as group_id var if set", func(t *testing.T) {
		ctx := testauth.WithAuthenticatedUserInfo(ctx, &claims.Claims{
			GroupID:                    "GR1",
			ExperimentTargetingGroupID: "GR2",
		})
		s := fp.String(ctx, "test_flag", "")
		require.Equal(t, "override", s)
	})
	t.Run("should use GroupID as group_id var if no targeting group ID is set", func(t *testing.T) {
		ctx := testauth.WithAuthenticatedUserInfo(ctx, &claims.Claims{
			GroupID: "GR1",
		})
		s := fp.String(ctx, "test_flag", "")
		require.Equal(t, "default", s)
	})
}
