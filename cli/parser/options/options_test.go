package options_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options/flag_form"
	"github.com/stretchr/testify/assert"
)

func RequiredValueDefinition(name, shortname string, opts ...options.DefinitionOpt) *options.Definition {
	return options.NewDefinition(
		name,
		append(
			[]options.DefinitionOpt{
				options.WithShortName(shortname),
				options.WithRequiresValue(),
			},
			opts...,
		)...,
	)
}

func BoolOrEnumDefinition(name, shortname string, opts ...options.DefinitionOpt) *options.Definition {
	return options.NewDefinition(
		name,
		append(
			[]options.DefinitionOpt{
				options.WithShortName(shortname),
				options.WithNegative(),
			},
			opts...,
		)...,
	)
}

func ExpansionDefinition(name, shortname string, opts ...options.DefinitionOpt) *options.Definition {
	return options.NewDefinition(
		name,
		append(
			[]options.DefinitionOpt{
				options.WithShortName(shortname),
			},
			opts...,
		)...,
	)
}

func TestRequiredValueOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	shortName := "n"
	badName := "badname"
	badShortName := "x"

	t.Run("Required value option base with name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			RequiredValueDefinition(name, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
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
			RequiredValueDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
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

	t.Run("Required value option base with name and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			RequiredValueDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
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
			RequiredValueDefinition(name, ""),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name and short name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			RequiredValueDefinition(name, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name and short name from invalid name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badName,
			RequiredValueDefinition(name, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Required value option base with name and short name from invalid short name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badShortName,
			RequiredValueDefinition(name, shortName),
		)
		assert.Error(t, err)
	})
}

func TestBoolOrEnumOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	shortName := "n"
	badName := "badname"
	badShortName := "x"

	t.Run("Bool or enum option base with name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			BoolOrEnumDefinition(name, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
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
			BoolOrEnumDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
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

	t.Run("Bool or enum option base with name and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			BoolOrEnumDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
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
			BoolOrEnumDefinition(name, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
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

	t.Run("Bool or enum option base with name and short name from negative name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			noName,
			BoolOrEnumDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+noName, base.Format())
		assert.True(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.NegativeName, base.Form)
		assert.True(t, base.UsesName())
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

	t.Run("Bool or enum option base with name and short name from invalid name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badName,
			BoolOrEnumDefinition(name, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Bool or enum option base with name and short name from invalid short name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badShortName,
			BoolOrEnumDefinition(name, shortName),
		)
		assert.Error(t, err)
	})
}

func TestExpansionOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	shortName := "n"
	badName := "badname"
	badShortName := "x"

	t.Run("Expansion option base with name from name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			name,
			ExpansionDefinition(name, ""),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
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
			ExpansionDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
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

	t.Run("Expansion option base with name and short name from short name", func(t *testing.T) {
		base, err := options.NewOptionBase(
			shortName,
			ExpansionDefinition(name, shortName),
		)

		assert.NoError(t, err)
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
		assert.True(t, base.UsesShortName())
		assert.Equal(t, "-"+shortName, base.Format())
		assert.False(t, base.Negative())
		base.UseName()
		assert.Equal(t, flag_form.Name, base.Form)
		assert.True(t, base.UsesName())
		assert.False(t, base.UsesShortName())
		assert.Equal(t, "--"+name, base.Format())
		assert.False(t, base.Negative())
		base.UseShortName()
		assert.Equal(t, flag_form.ShortName, base.Form)
		assert.False(t, base.UsesName())
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
			ExpansionDefinition(name, ""),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name and short name from negative name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			noName,
			ExpansionDefinition(name, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name and short name from invalid name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badName,
			ExpansionDefinition(name, shortName),
		)
		assert.Error(t, err)
	})

	t.Run("Expansion option base with name and short name from invalid short name", func(t *testing.T) {
		_, err := options.NewOptionBase(
			badShortName,
			ExpansionDefinition(name, shortName),
		)
		assert.Error(t, err)
	})
}
