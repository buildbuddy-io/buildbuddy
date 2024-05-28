package tags

import (
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

func replaceFlagsForTesting(t *testing.T) *flag.FlagSet {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	common.DefaultFlagSet = flags

	t.Cleanup(func() {
		common.DefaultFlagSet = flag.CommandLine
	})

	return flags
}

func replaceIgnoreSetForTesting(t *testing.T) map[string]struct{} {
	oldIgnoreSet := flagyaml.IgnoreSet
	flagyaml.IgnoreSet = make(map[string]struct{})

	t.Cleanup(func() {
		flagyaml.IgnoreSet = oldIgnoreSet
	})

	return flagyaml.IgnoreSet
}

func TestAlias(t *testing.T) {
	flagset := flag.NewFlagSet("test", flag.ContinueOnError)

	v := flagset.String("t", "default", "test usage")
	Tag[string, flag.Value](flagset, "t", AliasTag("t1", "t2"))

	f := flagset.Lookup("t")
	require.NotNil(t, f)

	assert.EqualValues(t, v, f.Value)
	assert.Equal(t, "test usage", f.Usage)

	f1 := flagset.Lookup("t1")
	require.NotNil(t, f1)

	f2 := flagset.Lookup("t2")
	require.NotNil(t, f2)

	assert.EqualValues(t, v, common.UnwrapFlagValue(f1.Value))
	assert.EqualValues(t, v, common.UnwrapFlagValue(f2.Value))

	assert.Equal(t, "Alias for t", f1.Usage)
	assert.Equal(t, "Alias for t", f2.Usage)

}

func TestDeprecated(t *testing.T) {
	flagset := flag.NewFlagSet("test", flag.ContinueOnError)

	v := flagset.String("t", "default", "test usage")
	Tag[string, flag.Value](flagset, "t", DeprecatedTag("migrate pls"))

	f := flagset.Lookup("t")
	require.NotNil(t, f)

	assert.EqualValues(t, v, common.UnwrapFlagValue(f.Value))
	assert.Equal(t, "test usage **DEPRECATED** migrate pls", f.Usage)
}

func TestSecret(t *testing.T) {
	flagset := replaceFlagsForTesting(t)

	v1 := flagset.String("t1", "default", "test usage")
	Tag[string, flag.Value](flagset, "t1", SecretTag)

	f1 := flagset.Lookup("t1")
	require.NotNil(t, f1)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1.Value))

	v2 := flagset.String("structured.t", "default2", "test usage")
	Tag[string, flag.Value](flagset, "structured.t", SecretTag)

	f2 := flagset.Lookup("structured.t")
	require.NotNil(t, f2)

	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2.Value))

	common.DefaultFlagSet = flagset
	text, err := flagyaml.SplitDocumentedYAMLFromFlags(flagyaml.RedactSecrets)
	require.NoError(t, err)

	m := make(map[string]any)
	require.NoError(t, yaml.Unmarshal(text, m))
	assert.Empty(t, len(m))
}

func TestInternal(t *testing.T) {
	flagset := replaceFlagsForTesting(t)

	v1 := flagset.String("t", "default", "test usage")
	Tag[string, flag.Value](flagset, "t", InternalTag)

	f1 := flagset.Lookup("t")
	require.NotNil(t, f1)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1.Value))

	v2 := flagset.String("structured.t", "default2", "test usage")
	Tag[string, flag.Value](flagset, "structured.t", InternalTag)

	f2 := flagset.Lookup("structured.t")
	require.NotNil(t, f2)

	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2.Value))

	_ = flagset.String("not_internal", "", "test usage")

	common.DefaultFlagSet = flagset
	text, err := flagyaml.SplitDocumentedYAMLFromFlags()
	require.NoError(t, err)

	m := make(map[string]any)
	require.NoError(t, yaml.Unmarshal(text, m))
	assert.Len(t, m, 1)

	assert.NotNil(t, flagset.Usage)
}

func TestYAMLIgnoreTag(t *testing.T) {
	flagset := replaceFlagsForTesting(t)
	_ = replaceIgnoreSetForTesting(t)

	v := flagset.String("t", "default", "test usage")
	Tag[string, flag.Value](flagset, "t", YAMLIgnoreTag)

	f := flagset.Lookup("t")
	require.NotNil(t, f)

	assert.EqualValues(t, v, common.UnwrapFlagValue(f.Value))

	yamlData := `
	t: newValue
`
	flagyaml.PopulateFlagsFromData(yamlData)
	assert.Equal(t, "default", *v)
}

func TestInternalAndSecret(t *testing.T) {
	flagset := replaceFlagsForTesting(t)

	v1 := flagset.String("t1", "default", "test usage")
	Tag[string, flag.Value](flagset, "t1", InternalTag)
	Tag[string, flag.Value](flagset, "t1", SecretTag)

	f1 := flagset.Lookup("t1")
	require.NotNil(t, f1)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1.Value))

	v2 := flagset.String("structured.t", "default2", "test usage")
	Tag[string, flag.Value](flagset, "structured.t", SecretTag)
	Tag[string, flag.Value](flagset, "structured.t", InternalTag)

	f2 := flagset.Lookup("structured.t")
	require.NotNil(t, f2)

	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2.Value))

	_ = flagset.String("not_internal_or_secret", "", "test usage")

	common.DefaultFlagSet = flagset
	text, err := flagyaml.SplitDocumentedYAMLFromFlags()
	require.NoError(t, err)

	m := make(map[string]any)
	require.NoError(t, yaml.Unmarshal(text, m))
	assert.Len(t, m, 1)

	assert.NotNil(t, flagset.Usage)
}

func TestAliasAndSecret(t *testing.T) {
	flagset := replaceFlagsForTesting(t)

	v1 := flagset.String("t", "default", "test usage")
	Tag[string, flag.Value](flagset, "t", AliasTag("t_alias"))
	Tag[string, flag.Value](flagset, "t", SecretTag)

	f1 := flagset.Lookup("t")
	require.NotNil(t, f1)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1.Value))
	assert.Equal(t, "test usage", f1.Usage)

	v2 := flagset.String("structured.t", "default2", "test usage")
	Tag[string, flag.Value](flagset, "structured.t", SecretTag)
	Tag[string, flag.Value](flagset, "structured.t", AliasTag("structured.t_alias"))

	f2 := flagset.Lookup("structured.t")
	require.NotNil(t, f2)

	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2.Value))
	assert.Equal(t, "test usage", f2.Usage)

	common.DefaultFlagSet = flagset
	text, err := flagyaml.SplitDocumentedYAMLFromFlags(flagyaml.RedactSecrets)
	require.NoError(t, err)

	m := make(map[string]any)
	require.NoError(t, yaml.Unmarshal(text, m))
	assert.Empty(t, m)

	assert.NotNil(t, flagset.Usage)

	f1Alias := flagset.Lookup("t_alias")
	require.NotNil(t, f1Alias)

	f2Alias := flagset.Lookup("structured.t_alias")
	require.NotNil(t, f2Alias)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1Alias.Value))
	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2Alias.Value))

	assert.Equal(t, "Alias for t", f1Alias.Usage)
	assert.Equal(t, "Alias for structured.t", f2Alias.Usage)
}

func TestDeprecatedAndSecret(t *testing.T) {
	flagset := replaceFlagsForTesting(t)

	v1 := flagset.String("t1", "default", "test usage")
	Tag[string, flag.Value](flagset, "t1", SecretTag)
	Tag[string, flag.Value](flagset, "t1", DeprecatedTag("migrate pls"))

	f1 := flagset.Lookup("t1")
	require.NotNil(t, f1)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1.Value))
	assert.Equal(t, "test usage **DEPRECATED** migrate pls", f1.Usage)

	v2 := flagset.String("structured.t", "default2", "test usage")
	Tag[string, flag.Value](flagset, "structured.t", DeprecatedTag("migrate pls"))
	Tag[string, flag.Value](flagset, "structured.t", SecretTag)

	f2 := flagset.Lookup("structured.t")
	require.NotNil(t, f2)

	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2.Value))
	assert.Equal(t, "test usage **DEPRECATED** migrate pls", f2.Usage)

	common.DefaultFlagSet = flagset
	text, err := flagyaml.SplitDocumentedYAMLFromFlags(flagyaml.RedactSecrets)
	require.NoError(t, err)

	m := make(map[string]any)
	require.NoError(t, yaml.Unmarshal(text, m))
	assert.Empty(t, len(m))
}

func TestYAMLIgnoreAndSecret(t *testing.T) {
	flagset := replaceFlagsForTesting(t)

	v1 := flagset.String("t1", "default", "test usage")
	Tag[string, flag.Value](flagset, "t1", SecretTag)
	Tag[string, flag.Value](flagset, "t1", YAMLIgnoreTag)

	f1 := flagset.Lookup("t1")
	require.NotNil(t, f1)

	assert.EqualValues(t, v1, common.UnwrapFlagValue(f1.Value))

	v2 := flagset.String("structured.t", "default2", "test usage")
	Tag[string, flag.Value](flagset, "structured.t", YAMLIgnoreTag)
	Tag[string, flag.Value](flagset, "structured.t", SecretTag)

	f2 := flagset.Lookup("structured.t")
	require.NotNil(t, f2)

	assert.EqualValues(t, v2, common.UnwrapFlagValue(f2.Value))

	common.DefaultFlagSet = flagset
	text, err := flagyaml.SplitDocumentedYAMLFromFlags(flagyaml.RedactSecrets)
	require.NoError(t, err)

	m := make(map[string]any)
	require.NoError(t, yaml.Unmarshal(text, m))
	assert.Empty(t, len(m))

	_ = replaceIgnoreSetForTesting(t)

	yamlData := `
	t: newValue
	structured:
	  t: newValue2
`
	flagyaml.PopulateFlagsFromData(yamlData)
	assert.Equal(t, "default", *v1)
	assert.Equal(t, "default2", *v2)
}
