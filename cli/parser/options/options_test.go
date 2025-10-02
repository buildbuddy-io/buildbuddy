
package options_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options/flag_form"
	"github.com/stretchr/testify/assert"
)

func RequiredValueDefinition(name, shortname string, opts... options.DefinitionOpt) *options.Definition {
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

func BoolOrEnumDefinition(name, shortname string, opts... options.DefinitionOpt) *options.Definition {
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

func ExpansionDefinition(name, shortname string, opts... options.DefinitionOpt) *options.Definition {
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
	badName := "badname"
	shortName := "n"

	base, err := options.NewOptionBase(
		name,
		RequiredValueDefinition(name, ""),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())

	assert.False(t, base.Negative())
	base.SetNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		name,
		RequiredValueDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	
	assert.False(t, base.Negative())
	base.SetNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		shortName,
		RequiredValueDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())

	assert.False(t, base.Negative())
	base.SetNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Short, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Short, base.Form)
	
	_, err = options.NewOptionBase(
		noName,
		RequiredValueDefinition(name, shortName),
	)
	assert.Error(t, err)
	
	_, err = options.NewOptionBase(
		badName,
		RequiredValueDefinition(name, shortName),
	)
	assert.Error(t, err)
}

func TestBoolOrEnumOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	badName := "badname"
	shortName := "n"

	base, err := options.NewOptionBase(
		name,
		BoolOrEnumDefinition(name, ""),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())

	assert.False(t, base.Negative())
	base.SetNegative()
	assert.True(t, base.Negative())
	assert.Equal(t, flag_form.Negative, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		name,
		BoolOrEnumDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	
	assert.False(t, base.Negative())
	base.SetNegative()
	assert.True(t, base.Negative())
	assert.Equal(t, flag_form.Negative, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		shortName,
		BoolOrEnumDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())

	assert.False(t, base.Negative())
	base.SetNegative()
	assert.True(t, base.Negative())
	assert.Equal(t, flag_form.Negative, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		noName,
		BoolOrEnumDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Negative, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + noName, base.Format())
	assert.True(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Negative, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + noName, base.Format())
	assert.True(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Negative, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + noName, base.Format())
	assert.True(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Negative, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + noName, base.Format())
	assert.True(t, base.Negative())
	
	assert.True(t, base.Negative())
	base.SetNegative()
	assert.True(t, base.Negative())
	assert.Equal(t, flag_form.Negative, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	
	_, err = options.NewOptionBase(
		badName,
		BoolOrEnumDefinition(name, shortName),
	)
	assert.Error(t, err)
}

func TestExpansionOptionBase(t *testing.T) {
	name := "name"
	noName := "noname"
	badName := "badname"
	shortName := "n"

	base, err := options.NewOptionBase(
		name,
		ExpansionDefinition(name, ""),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())

	assert.False(t, base.Negative())
	base.SetNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		name,
		ExpansionDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	
	assert.False(t, base.Negative())
	base.SetNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Standard, base.Form)
	
	base, err = options.NewOptionBase(
		shortName,
		ExpansionDefinition(name, shortName),
	)
	assert.NoError(t, err)
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())
	base.UseName()
	assert.Equal(t, flag_form.Standard, base.Form)
	assert.True(t, base.UsesName())
	assert.False(t, base.UsesShortName())
	assert.Equal(t, "--" + name, base.Format())
	assert.False(t, base.Negative())
	base.UseShortName()
	assert.Equal(t, flag_form.Short, base.Form)
	assert.False(t, base.UsesName())
	assert.True(t, base.UsesShortName())
	assert.Equal(t, "-" + shortName, base.Format())
	assert.False(t, base.Negative())

	assert.False(t, base.Negative())
	base.SetNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Short, base.Form)
	base.ClearNegative()
	assert.False(t, base.Negative())
	assert.Equal(t, flag_form.Short, base.Form)
	
	_, err = options.NewOptionBase(
		noName,
		ExpansionDefinition(name, shortName),
	)
	assert.Error(t, err)
	
	_, err = options.NewOptionBase(
		badName,
		ExpansionDefinition(name, shortName),
	)
	assert.Error(t, err)
}
