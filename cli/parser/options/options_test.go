package options_test

import (
	"flag"
	"slices"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options/flag_form"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/seq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
)

func TestDefinitionsFromFlagSet(t *testing.T) {
	testFlags := flag.NewFlagSet("test_flagset", flag.ContinueOnError)
	testFlags.Bool("enabled", false, "")
	testFlags.String("name", "", "")
	flagtypes.StringSlice(testFlags, "tags", nil, "")
	flagtypes.JSONMap(testFlags, "labels", map[string]string{}, "")

	definitions, err := options.DefinitionsFromFlagSet(testFlags, "test_command")
	require.NoError(t, err)
	byName := make(map[string]*options.Definition, len(definitions))
	for _, definition := range definitions {
		byName[definition.Name()] = definition
	}

	require.True(t, byName["enabled"].HasNegative())
	require.False(t, byName["enabled"].RequiresValue())
	require.True(t, byName["name"].RequiresValue())
	require.False(t, byName["name"].Multi())
	// Slice- and map-valued flags are repeatable, so they should receive the
	// Multi() option.
	require.True(t, byName["tags"].Multi())
	require.True(t, byName["labels"].Multi())
}

func RequiredValueDefinition(name, oldname, shortname string, opts ...options.DefinitionOpt) *options.Definition {
	return options.NewDefinition(
		name,
		append(
			[]options.DefinitionOpt{
				options.WithOldName(oldname),
				options.WithShortName(shortname),
				options.WithRequiresValue(),
			},
			opts...,
		)...,
	)
}

func BoolOrEnumDefinition(name, oldname, shortname string, opts ...options.DefinitionOpt) *options.Definition {
	return options.NewDefinition(
		name,
		append(
			[]options.DefinitionOpt{
				options.WithOldName(oldname),
				options.WithShortName(shortname),
				options.WithNegative(),
			},
			opts...,
		)...,
	)
}

func ExpansionDefinition(name, oldname, shortname string, opts ...options.DefinitionOpt) *options.Definition {
	return options.NewDefinition(
		name,
		append(
			[]options.DefinitionOpt{
				options.WithOldName(oldname),
				options.WithShortName(shortname),
			},
			opts...,
		)...,
	)
}

func TestRequiredValueOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	oldName := "experimental_name"
	noOldName := "noexperimental_name"
	shortName := "n"
	badName := "badname"
	badShortName := "x"

	t.Run("Required value option base with name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			RequiredValueDefinition(name, "", ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Required value option base with name and old name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			RequiredValueDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Required value option base with name and short name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			RequiredValueDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Required value option base with name, old name, and short name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			RequiredValueDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Required value option base with name and old name from old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			oldName,
			RequiredValueDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Required value option base with name, old name, and short name from old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			oldName,
			RequiredValueDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Required value option base with name and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			RequiredValueDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
	})

	t.Run("Required value option base with name, old name, and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			RequiredValueDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
	})

	t.Run("Required value option base with name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			RequiredValueDefinition(name, "", ""),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name and old name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			RequiredValueDefinition(name, oldName, ""),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name and short name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			RequiredValueDefinition(name, "", shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name, old name, and short name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			RequiredValueDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name and old name from negative old name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noOldName,
			RequiredValueDefinition(name, oldName, ""),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name, old name, and short name from negative old name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noOldName,
			RequiredValueDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name, old name, and short name from invalid name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badName,
			RequiredValueDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name, old name, and short name from invalid short name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badShortName,
			RequiredValueDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})
}

func TestBoolOrEnumOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	oldName := "experimental_name"
	noOldName := "noexperimental_name"
	shortName := "n"
	badName := "badname"
	badShortName := "x"

	t.Run("Bool or enum option base with name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			BoolOrEnumDefinition(name, "", ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name and old name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			BoolOrEnumDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name and short name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			BoolOrEnumDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name, old name, and short name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			BoolOrEnumDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name and old name from old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			oldName,
			BoolOrEnumDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.Equal(t, "--"+noOldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Bool or enum option base with name, old name, and short name from old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			oldName,
			BoolOrEnumDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.Equal(t, "--"+noOldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Bool or enum option base with name and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			BoolOrEnumDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name, old name, and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			BoolOrEnumDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name from negative name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noName,
			BoolOrEnumDefinition(name, "", ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())

		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
	})

	t.Run("Bool or enum option base with name and old name from negative name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noName,
			BoolOrEnumDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name and short name from negative name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noName,
			BoolOrEnumDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())

		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
	})

	t.Run("Bool or enum option base with name, old name, and short name from negative name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noName,
			BoolOrEnumDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.Equal(t, "--"+noName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Bool or enum option base with name and old name from negative old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noOldName,
			BoolOrEnumDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())

		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.Equal(t, "--"+noOldName, base.Format())
	})

	t.Run("Bool or enum option base with name, old name, and short name from negative old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noOldName,
			BoolOrEnumDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noOldName, base.Format())
		assert.True(t, base.Negative())

		base.SetNegative()
		assert.True(t, base.Negative())
		assert.Equal(t, flag_form.NegativeOldName, base.Form)
		assert.Equal(t, "--"+noOldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Bool or enum option base with name, old name, and short name from invalid name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badName,
			BoolOrEnumDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Bool or enum option base with name, old name, and short name from invalid short name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badShortName,
			BoolOrEnumDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})
}

func TestExpansionOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	oldName := "experimental_name"
	noOldName := "noexperimental_name"
	shortName := "n"
	badName := "badname"
	badShortName := "x"

	t.Run("Expansion option base with name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			ExpansionDefinition(name, "", ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Expansion option base with name and old name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			ExpansionDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Expansion option base with name and short name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			ExpansionDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Expansion option base with name, old name, and short name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			ExpansionDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.Name, base.Form)
		assert.Equal(t, "--"+name, base.Format())
	})

	t.Run("Expansion option base with name and old name from old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			oldName,
			ExpansionDefinition(name, oldName, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Expansion option base with name, old name, and short name from old name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			oldName,
			ExpansionDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.Equal(t, "--"+oldName, base.Format())
	})

	t.Run("Expansion option base with name and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			ExpansionDefinition(name, "", shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
	})

	t.Run("Expansion option base with name, old name, and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			ExpansionDefinition(name, oldName, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseOldName()
		assert.Equal(t, flag_form.OldName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesOldName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+oldName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.False(t, base.UsesOldName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())

		base.SetNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
		base.ClearNegative()
		assert.False(t, base.Negative())
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.Equal(t, "-"+shortName, base.Format())
	})

	t.Run("Expansion option base with name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			ExpansionDefinition(name, "", ""),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name and old name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			ExpansionDefinition(name, oldName, ""),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name and short name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			ExpansionDefinition(name, "", shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name, old name, and short name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			ExpansionDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name and old name from negative old name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noOldName,
			ExpansionDefinition(name, oldName, ""),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name, old name, and short name from negative old name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noOldName,
			ExpansionDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name, old name, and short name from invalid name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badName,
			ExpansionDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name, old name, and short name from invalid short name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badShortName,
			ExpansionDefinition(name, oldName, shortName),
		)
		assert.Error(t, err)
	})
}

func TestCloneRequiredValueOption(t *testing.T) {
	value := "value"
	otherValue := "other"

	name := "name"
	oldName := "experimental_name"
	shortName := "n"
	definition := RequiredValueDefinition(name, oldName, shortName)

	otherName := "other_name"
	otherOldName := "experimental_other_name"
	otherShortName := "o"
	otherDefinition := RequiredValueDefinition(otherName, otherOldName, otherShortName)

	opt, err := options.NewOption(name, nil, definition)
	opt.SetValue(value)
	opt = opt.Normalized()
	require.NoError(t, err)
	require.NotNil(t, opt)

	requiredOpt, ok := opt.(*options.RequiredValueOption)
	require.True(t, ok)
	require.NotNil(t, requiredOpt)

	clone := opt.Clone()
	require.NotNil(t, clone)

	requiredClone, ok := clone.(*options.RequiredValueOption)
	require.True(t, ok)
	require.NotNil(t, requiredClone)

	assert.Equal(t, value, clone.GetValue())
	assert.Equal(t, definition, requiredClone.Defined)
	assert.True(t, clone.UsesName())
	assert.True(t, requiredClone.Joined)

	clone.SetValue(otherValue)
	assert.Equal(t, value, opt.GetValue())
	assert.Equal(t, otherValue, clone.GetValue())

	requiredClone.Defined = otherDefinition
	assert.Equal(t, definition, requiredOpt.Defined)
	assert.Equal(t, otherDefinition, requiredClone.Defined)

	clone.UseOldName()
	assert.True(t, opt.UsesName())
	assert.True(t, clone.UsesOldName())

	requiredClone.Joined = false
	assert.True(t, requiredOpt.Joined)
}

func TestExpansionOption(t *testing.T) {
	e1e1b1Def := BoolOrEnumDefinition("e1e1b1", "", "")
	e1e1b1Opt, err := options.NewOption(
		e1e1b1Def.Name(), nil, e1e1b1Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e1e1b1Opt)

	e1e1b2Def := BoolOrEnumDefinition("e1e1b2", "", "")
	e1e1b2Opt, err := options.NewOption(
		e1e1b2Def.Name(), nil, e1e1b2Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e1e1b2Opt)

	e1e1Def := ExpansionDefinition("e1e1", "", "", options.WithExpansion(
		e1e1b1Opt,
		e1e1b2Opt,
	))
	e1e1Opt, err := options.NewOption(
		e1e1Def.Name(), nil, e1e1Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e1e1Opt)

	e1b1Def := BoolOrEnumDefinition("e1b1", "", "")
	e1b1Opt, err := options.NewOption(
		e1b1Def.Name(), nil, e1b1Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e1b1Opt)

	e1b2Def := BoolOrEnumDefinition("e1b2", "", "")
	e1b2Opt, err := options.NewOption(
		e1b2Def.Name(), nil, e1b2Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e1b2Opt)

	e1Def := ExpansionDefinition("e1", "", "", options.WithExpansion(
		e1b1Opt,
		e1e1Opt,
		e1b2Opt,
	))
	e1Opt, err := options.NewOption(
		e1Def.Name(), nil, e1Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e1Opt)

	e2b1Def := BoolOrEnumDefinition("e2b1", "", "")
	e2b1Opt, err := options.NewOption(
		e2b1Def.Name(), nil, e2b1Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e2b1Opt)

	e2Def := ExpansionDefinition("e2", "", "", options.WithExpansion(
		e2b1Opt,
	))
	e2Opt, err := options.NewOption(
		e2Def.Name(), nil, e2Def,
	)
	require.NoError(t, err)
	require.NotNil(t, e2Opt)

	eb3Def := BoolOrEnumDefinition("eb3", "", "")
	eb3Opt, err := options.NewOption(
		eb3Def.Name(), nil, eb3Def,
	)
	require.NoError(t, err)
	require.NotNil(t, eb3Opt)

	eb2Def := BoolOrEnumDefinition("eb2", "", "")
	eb2Opt, err := options.NewOption(
		eb2Def.Name(), nil, eb2Def,
	)
	require.NoError(t, err)
	require.NotNil(t, eb2Opt)

	eb1Def := BoolOrEnumDefinition("eb1", "", "")
	eb1Opt, err := options.NewOption(
		eb1Def.Name(), nil, eb1Def,
	)
	require.NoError(t, err)
	require.NotNil(t, eb1Opt)

	eDef := ExpansionDefinition("e", "", "", options.WithExpansion(
		eb1Opt,
		e1Opt,
		eb2Opt,
		eb3Opt,
		e2Opt,
		eb2Opt,
	))
	eOpt, err := options.NewOption(
		eDef.Name(), nil, eDef,
	)
	require.NoError(t, err)
	require.NotNil(t, eOpt)

	assert.Equal(
		t,
		[]options.Option{
			eb1Opt,
			e1b1Opt,
			e1e1b1Opt,
			e1e1b2Opt,
			e1b2Opt,
			eb2Opt,
			eb3Opt,
			e2b1Opt,
			eb2Opt,
		},
		slices.Collect(options.ExpandAll(slices.Values([]options.Option{eOpt}))),
	)

	var truncated []options.Option
	for o := range options.ExpandAll([]options.Option{eOpt}) {
		if seq.Equal[string](o.Format(), e1e1b2Opt.Format()) {
			break
		}
		truncated = append(truncated, o)
	}

	expected := slices.Collect(
		seq.Fmap(
			[]options.Option{
				eb1Opt,
				e1b1Opt,
				e1e1b1Opt,
			},
			func(o options.Option) string {
				return strings.Join(o.Format(), " ")
			},
		),
	)
	actual := slices.Collect(
		seq.Fmap(
			truncated,
			func(o options.Option) string {
				return strings.Join(o.Format(), " ")
			},
		),
	)
	assert.Equal(
		t,
		expected,
		actual,
	)
}
