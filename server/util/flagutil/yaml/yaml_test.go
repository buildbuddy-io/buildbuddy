package yaml_test

import (
	"context"
	"flag"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

type unsupportedFlagValue struct{}

func (f *unsupportedFlagValue) Set(string) error { return nil }
func (f *unsupportedFlagValue) String() string   { return "" }

type testStruct struct {
	Field  int    `json:"field"`
	Meadow string `json:"meadow"`
}

func replaceFlagsForTesting(t *testing.T) *flag.FlagSet {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	common.DefaultFlagSet = flags

	t.Cleanup(func() {
		common.DefaultFlagSet = flag.CommandLine
	})

	return flags
}

func TestGenerateYAMLTypeMapFromFlags(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flags.Bool("bool", true, "")
	flags.Int("one.two.int", 10, "")
	flagtypes.StringSlice("one.two.string_slice", []string{"hi", "hello"}, "")
	flags.Float64("one.two.two_and_a_half.float64", 5.2, "")
	flagtypes.JSONSlice("one.two.three.struct_slice", []testStruct{{Field: 4, Meadow: "Great"}}, "")
	flags.String("a.b.string", "xxx", "")
	flagtypes.URLFromString("a.b.url", "https://www.example.com", "")
	actual, err := flagyaml.GenerateYAMLMapWithValuesFromFlags(
		func(flg *flag.Flag) (reflect.Type, error) {
			return flagyaml.GetYAMLTypeForFlagValue(flg.Value)
		},
		flagyaml.IgnoreFilter,
	)
	require.NoError(t, err)
	expected := map[string]any{
		"bool": reflect.TypeOf((*bool)(nil)),
		"one": map[string]any{
			"two": map[string]any{
				"int":          reflect.TypeOf((*int)(nil)),
				"string_slice": reflect.TypeOf((*[]string)(nil)),
				"two_and_a_half": map[string]any{
					"float64": reflect.TypeOf((*float64)(nil)),
				},
				"three": map[string]any{
					"struct_slice": reflect.TypeOf((*[]testStruct)(nil)),
				},
			},
		},
		"a": map[string]any{
			"b": map[string]any{
				"string": reflect.TypeOf((*string)(nil)),
				"url":    reflect.TypeOf((*flagtypes.URLFlag)(nil)),
			},
		},
	}
	if diff := cmp.Diff(expected, actual, cmp.Comparer(func(x, y reflect.Type) bool { return x == y })); diff != "" {
		t.Error(diff)
	}
}

func TestBadGenerateYAMLTypeMapFromFlags(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flags.Int("one.two.int", 10, "")
	flags.Int("one.two", 10, "")
	_, err := flagyaml.GenerateYAMLMapWithValuesFromFlags(
		func(flg *flag.Flag) (reflect.Type, error) {
			return flagyaml.GetYAMLTypeForFlagValue(flg.Value)
		},
		flagyaml.IgnoreFilter,
	)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)

	flags.Int("one.two", 10, "")
	flags.Int("one.two.int", 10, "")
	_, err = flagyaml.GenerateYAMLMapWithValuesFromFlags(
		func(flg *flag.Flag) (reflect.Type, error) {
			return flagyaml.GetYAMLTypeForFlagValue(flg.Value)
		},
		flagyaml.IgnoreFilter,
	)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)

	flags.Var(&unsupportedFlagValue{}, "unsupported", "")
	_, err = flagyaml.GenerateYAMLMapWithValuesFromFlags(
		func(flg *flag.Flag) (reflect.Type, error) {
			return flagyaml.GetYAMLTypeForFlagValue(flg.Value)
		},
		flagyaml.IgnoreFilter,
	)
	require.Error(t, err)

}

func TestRetypeAndFilterYAMLMap(t *testing.T) {
	typeMap := map[string]any{
		"bool": reflect.TypeOf((*bool)(nil)),
		"one": map[string]any{
			"two": map[string]any{
				"int":          reflect.TypeOf((*int)(nil)),
				"string_slice": reflect.TypeOf((*[]string)(nil)),
				"two_and_a_half": map[string]any{
					"float64": reflect.TypeOf((*float64)(nil)),
				},
				"three": map[string]any{
					"struct_slice": reflect.TypeOf((*[]testStruct)(nil)),
				},
			},
		},
		"a": map[string]any{
			"b": map[string]any{
				"string": reflect.TypeOf((*string)(nil)),
				"url":    reflect.TypeOf((*flagtypes.URLFlag)(nil)),
			},
		},
		"foo": map[string]any{
			"bar": reflect.TypeOf((*int64)(nil)),
		},
	}
	yamlData := `
bool: true
one:
  two:
    int: 1
    string_slice:
      - "string1"
      - "string2"
    two_and_a_half:
      float64: 9.4
    three:
      struct_slice:
        - field: 9
          meadow: "Eternal"
        - field: 5
a:
  b:
    url: "http://www.example.com"
foo: 7
first:
  second:
    unknown: 9009
    no: "definitely not"
`
	yamlMap := make(map[string]any)
	err := yaml.Unmarshal([]byte(yamlData), yamlMap)
	require.NoError(t, err)
	err = flagyaml.RetypeAndFilterYAMLMap(yamlMap, typeMap, []string{})
	require.NoError(t, err)
	expected := map[string]any{
		"bool": true,
		"one": map[string]any{
			"two": map[string]any{
				"int":          int(1),
				"string_slice": []string{"string1", "string2"},
				"two_and_a_half": map[string]any{
					"float64": float64(9.4),
				},
				"three": map[string]any{
					"struct_slice": []testStruct{{Field: 9, Meadow: "Eternal"}, {Field: 5}},
				},
			},
		},
		"a": map[string]any{
			"b": map[string]any{
				"url": flagtypes.URLFlag(url.URL{Scheme: "http", Host: "www.example.com"}),
			},
		},
	}
	if diff := cmp.Diff(expected, yamlMap); diff != "" {
		t.Error(diff)
	}
}

func TestBadRetypeAndFilterYAMLMap(t *testing.T) {
	typeMap := map[string]any{
		"bool": reflect.TypeOf((*bool)(nil)),
	}
	yamlData := `
bool: 7
`
	yamlMap := make(map[string]any)
	err := yaml.Unmarshal([]byte(yamlData), yamlMap)
	require.NoError(t, err)
	err = flagyaml.RetypeAndFilterYAMLMap(yamlMap, typeMap, []string{})
	require.Error(t, err)

	typeMap = map[string]any{
		"bool": false,
	}
	yamlData = `
bool: true
`
	yamlMap = make(map[string]any)
	err = yaml.Unmarshal([]byte(yamlData), yamlMap)
	require.NoError(t, err)
	err = flagyaml.RetypeAndFilterYAMLMap(yamlMap, typeMap, []string{})
	require.Error(t, err)
}

func TestPopulateFlagsFromData(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flagBool := flags.Bool("bool", true, "")
	flagOneTwoInt := flags.Int("one.two.int", 10, "")
	flagOneTwoStringSlice := flagtypes.StringSlice("one.two.string_slice", []string{"hi", "hello"}, "")
	flagOneTwoTwoAndAHalfFloat := flags.Float64("one.two.two_and_a_half.float64", 5.2, "")
	flagOneTwoThreeStructSlice := []testStruct{{Field: 4, Meadow: "Great"}}
	flagtypes.JSONSliceVar(&flagOneTwoThreeStructSlice, "one.two.three.struct_slice", flagOneTwoThreeStructSlice, "")
	flagABString := flags.String("a.b.string", "xxx", "")
	flagABStructSlice := []testStruct{{Field: 7, Meadow: "Chimney"}}
	flagtypes.JSONSliceVar(&flagABStructSlice, "a.b.struct_slice", flagABStructSlice, "")
	flagABURL := flagtypes.URLFromString("a.b.url", "https://www.example.com", "")
	yamlData := `
bool: true
one:
  two:
    int: 1
    string_slice:
      - "string1"
      - "string2"
    two_and_a_half:
      float64: 9.4
    three:
      struct_slice:
        - field: 9
          meadow: "Eternal"
        - field: 5
a:
  b:
    url: "http://www.example.com:8080"
foo: 7
first:
  second:
    unknown: 9009
    no: "definitely not"
`
	err := flagyaml.PopulateFlagsFromData(yamlData)
	require.NoError(t, err)
	assert.Equal(t, true, *flagBool)
	assert.Equal(t, int(1), *flagOneTwoInt)
	assert.Equal(t, []string{"hi", "hello", "string1", "string2"}, *flagOneTwoStringSlice)
	assert.Equal(t, float64(9.4), *flagOneTwoTwoAndAHalfFloat)
	assert.Equal(t, []testStruct{{Field: 4, Meadow: "Great"}, {Field: 9, Meadow: "Eternal"}, {Field: 5}}, flagOneTwoThreeStructSlice)
	assert.Equal(t, "xxx", *flagABString)
	assert.Equal(t, []testStruct{{Field: 7, Meadow: "Chimney"}}, flagABStructSlice)
	assert.Equal(t, url.URL{Scheme: "http", Host: "www.example.com:8080"}, *flagABURL)
}

func TestBadPopulateFlagsFromData(t *testing.T) {
	_ = replaceFlagsForTesting(t)

	yamlData := `
	bool: true
`
	err := flagyaml.PopulateFlagsFromData(yamlData)
	require.Error(t, err)

	flags := replaceFlagsForTesting(t)

	flags.Var(&unsupportedFlagValue{}, "bad", "")
	err = flagyaml.PopulateFlagsFromData("")
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)

	_ = flags.Bool("bool", false, "")
	yamlData = `
bool: 7
`
	err = flagyaml.PopulateFlagsFromData(yamlData)
	require.Error(t, err)
}

func TestPopulateFlagsFromYAML(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flagBool := flags.Bool("bool", true, "")
	flagOneTwoInt := flags.Int("one.two.int", 10, "")
	flagOneTwoStringSlice := flagtypes.JSONSlice("one.two.string_slice", []string{"hi", "hello"}, "")
	flagOneTwoTwoAndAHalfFloat := flags.Float64("one.two.two_and_a_half.float64", 5.2, "")
	flagOneTwoThreeStructSlice := []testStruct{{Field: 4, Meadow: "Great"}}
	flagtypes.JSONSliceVar(&flagOneTwoThreeStructSlice, "one.two.three.struct_slice", flagOneTwoThreeStructSlice, "")
	flagABString := flags.String("a.b.string", "xxx", "")
	flagABStructSlice := []testStruct{{Field: 7, Meadow: "Chimney"}}
	flagtypes.JSONSliceVar(&flagABStructSlice, "a.b.struct_slice", flagABStructSlice, "")
	flagABURL := flagtypes.URLFromString("a.b.url", "https://www.example.com", "")
	input := map[string]any{
		"bool": false,
		"one": map[string]any{
			"two": map[string]any{
				"string_slice": []string{"meow", "woof"},
				"two_and_a_half": map[string]any{
					"float64": float64(7),
				},
				"three": map[string]any{
					"struct_slice": ([]testStruct)(nil),
				},
			},
		},
		"a": map[string]any{
			"b": map[string]any{
				"string":       "",
				"struct_slice": []testStruct{{Field: 9}},
				"url":          flagtypes.URLFlag(url.URL{Scheme: "https", Host: "www.example.com:8080"}),
			},
		},
		"undefined": struct{}{}, // keys without with no corresponding flag name should be ignored.
	}
	node := &yaml.Node{}
	err := node.Encode(input)
	require.NoError(t, err)
	err = flagyaml.PopulateFlagsFromYAMLMap(input, node)
	require.NoError(t, err)

	assert.Equal(t, false, *flagBool)
	assert.Equal(t, 10, *flagOneTwoInt)
	assert.Equal(t, []string{"hi", "hello", "meow", "woof"}, *flagOneTwoStringSlice)
	assert.Equal(t, float64(7), *flagOneTwoTwoAndAHalfFloat)
	assert.Equal(t, []testStruct{{Field: 4, Meadow: "Great"}}, flagOneTwoThreeStructSlice)
	assert.Equal(t, "", *flagABString)
	assert.Equal(t, []testStruct{{Field: 7, Meadow: "Chimney"}, {Field: 9}}, flagABStructSlice)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com:8080"}, *flagABURL)
}

func TestBadPopulateFlagsFromYAML(t *testing.T) {
	_ = replaceFlagsForTesting(t)

	flags := replaceFlagsForTesting(t)
	flags.Var(&unsupportedFlagValue{}, "unsupported", "")
	input := map[string]any{
		"unsupported": 0,
	}
	node := &yaml.Node{}
	err := node.Encode(input)
	require.NoError(t, err)
	err = flagyaml.PopulateFlagsFromYAMLMap(input, node)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flags.Bool("bool", false, "")
	input = map[string]any{
		"bool": 0,
	}
	node = &yaml.Node{}
	err = node.Encode(input)
	require.NoError(t, err)
	err = flagyaml.PopulateFlagsFromYAMLMap(input, node)
	require.Error(t, err)
}

func TestOverrideFlagsFromData(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flagBool := flags.Bool("bool", true, "")
	flagOneTwoInt := flags.Int("one.two.int", 10, "")
	flagOneTwoStringSlice := flagtypes.StringSlice("one.two.string_slice", []string{"hi", "hello"}, "")
	flagOneTwoTwoAndAHalfFloat := flags.Float64("one.two.two_and_a_half.float64", 5.2, "")
	flagOneTwoThreeStructSlice := []testStruct{{Field: 4, Meadow: "Great"}}
	flagtypes.JSONSliceVar(&flagOneTwoThreeStructSlice, "one.two.three.struct_slice", flagOneTwoThreeStructSlice, "")
	flagABString := flags.String("a.b.string", "xxx", "")
	flagABStructSlice := []testStruct{{Field: 7, Meadow: "Chimney"}}
	flagtypes.JSONSliceVar(&flagABStructSlice, "a.b.struct_slice", flagABStructSlice, "")
	flagABURL := flagtypes.URLFromString("a.b.url", "https://www.example.com", "")
	yamlData := `
bool: true
one:
  two:
    int: 1
    string_slice:
      - "string1"
      - "string2"
    two_and_a_half:
      float64: 9.4
    three:
      struct_slice:
        - field: 9
          meadow: "Eternal"
        - field: 5
a:
  b:
    url: "http://www.example.com:8080"
foo: 7
first:
  second:
    unknown: 9009
    no: "definitely not"
`
	err := flagyaml.OverrideFlagsFromData(yamlData)
	require.NoError(t, err)
	assert.Equal(t, true, *flagBool)
	assert.Equal(t, int(1), *flagOneTwoInt)
	assert.Equal(t, []string{"string1", "string2"}, *flagOneTwoStringSlice)
	assert.Equal(t, float64(9.4), *flagOneTwoTwoAndAHalfFloat)
	assert.Equal(t, []testStruct{{Field: 9, Meadow: "Eternal"}, {Field: 5}}, flagOneTwoThreeStructSlice)
	assert.Equal(t, "xxx", *flagABString)
	assert.Equal(t, []testStruct{{Field: 7, Meadow: "Chimney"}}, flagABStructSlice)
	assert.Equal(t, url.URL{Scheme: "http", Host: "www.example.com:8080"}, *flagABURL)
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

func TestSecretExpansion(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	os.Setenv("SOMEENV", "foo")
	envFlag := flags.String("env_flag", "", "")
	err := flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
		env_flag: ${SOMEENV}
	`))
	require.NoError(t, err)
	require.Equal(t, "foo", *envFlag)

	// Multiline env variable. Uncommon, but should work.
	os.Setenv("SOMEENV", "foo\nbar")
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
		env_flag: ${SOMEENV}
	`))
	require.NoError(t, err)
	require.Equal(t, "foo\nbar", *envFlag)

	secretFlag := flags.String("secret_flag", "", "")
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
		secret_flag: ${SECRET:FOO}
	`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no secret provider")

	flagyaml.SecretProvider = &fakeSecretProvider{}
	defer func() {
		flagyaml.SecretProvider = nil
	}()
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
		secret_flag: ${SECRET:FOO}
	`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	flagyaml.SecretProvider = &fakeSecretProvider{
		secrets: map[string]string{"FOO": "BAR"},
	}
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
		secret_flag: ${SECRET:FOO}
	`))
	require.NoError(t, err)
	require.Equal(t, "BAR", *secretFlag)

	// Multiline secret.
	flagyaml.SecretProvider = &fakeSecretProvider{
		secrets: map[string]string{"FOO": "BAR\nBAZ"},
	}
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
		secret_flag: ${SECRET:FOO}
	`))
	require.NoError(t, err)
	require.Equal(t, "BAR\nBAZ", *secretFlag)

	// Secrets inside a list.
	flagyaml.SecretProvider = &fakeSecretProvider{
		secrets: map[string]string{"FOO1": "FIRST\nSECRET", "FOO2": "SECOND\nSECRET"},
	}
	secretSliceFlag := flagutil.New("secret_slice_flag", []string{}, "")
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
secret_slice_flag: 
  - ${SECRET:FOO1}
  - ${SECRET:FOO2}
	`))
	require.NoError(t, err)
	require.Equal(t, []string{"FIRST\nSECRET", "SECOND\nSECRET"}, *secretSliceFlag)

	secretStructSliceFlag := flagutil.New("secret_struct_slice_flag", []secretHolder{}, "")
	err = flagyaml.PopulateFlagsFromData(strings.TrimSpace(`
secret_struct_slice_flag: 
  - secret: ${SECRET:FOO1}
  - secret: ${SECRET:FOO2}
	`))
	require.NoError(t, err)
	require.Equal(t, []secretHolder{{Secret: "FIRST\nSECRET"}, {Secret: "SECOND\nSECRET"}}, *secretStructSliceFlag)
}
