package flag_form_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/options/flag_form"
	"github.com/stretchr/testify/assert"
)

func TestNegative(t *testing.T) {
	assert.False(t, flag_form.Standard.Negative())
	assert.True(t, flag_form.Negative.Negative())
	assert.False(t, flag_form.Short.Negative())
	assert.False(t, flag_form.Unknown.Negative())
	assert.False(t, (flag_form.Unknown + 1).Negative())
	assert.False(t, (flag_form.Unknown + 2).Negative())
}

func TestSetNegative(t *testing.T) {
	assert.Equal(t, flag_form.Negative, flag_form.Standard.SetNegative())
	assert.Equal(t, flag_form.Negative, flag_form.Negative.SetNegative())
	assert.Equal(t, flag_form.Unknown, flag_form.Short.SetNegative())
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.SetNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 1).SetNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 2).SetNegative())
}

func TestClearNegative(t *testing.T) {
	assert.Equal(t, flag_form.Standard, flag_form.Standard.ClearNegative())
	assert.Equal(t, flag_form.Standard, flag_form.Negative.ClearNegative())
	assert.Equal(t, flag_form.Short, flag_form.Short.ClearNegative())
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.ClearNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 1).ClearNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 2).ClearNegative())
}

func TestCompareNameTypes(t *testing.T) {
	// Compare against Standard
	assert.True(t, flag_form.Standard.CompareNameType(flag_form.Standard))
	assert.True(t, flag_form.Standard.CompareNameType(flag_form.Negative))
	assert.False(t, flag_form.Standard.CompareNameType(flag_form.Short))
	assert.False(t, flag_form.Standard.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.Standard.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.Standard.CompareNameType(flag_form.Unknown+2))

	// Compare against Negative
	assert.True(t, flag_form.Negative.CompareNameType(flag_form.Standard))
	assert.True(t, flag_form.Negative.CompareNameType(flag_form.Negative))
	assert.False(t, flag_form.Negative.CompareNameType(flag_form.Short))
	assert.False(t, flag_form.Negative.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.Negative.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.Negative.CompareNameType(flag_form.Unknown+2))

	// Compare against Short
	assert.False(t, flag_form.Short.CompareNameType(flag_form.Standard))
	assert.False(t, flag_form.Short.CompareNameType(flag_form.Negative))
	assert.True(t, flag_form.Short.CompareNameType(flag_form.Short))
	assert.False(t, flag_form.Short.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.Short.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.Short.CompareNameType(flag_form.Unknown+2))

	// Compare against Unknown
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Standard))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Negative))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Short))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Unknown+2))
}

func TestUnknown(t *testing.T) {
	assert.False(t, flag_form.Standard.Unknown())
	assert.False(t, flag_form.Negative.Unknown())
	assert.False(t, flag_form.Short.Unknown())
	assert.True(t, flag_form.Unknown.Unknown())
	assert.True(t, (flag_form.Unknown + 1).Unknown())
	assert.True(t, (flag_form.Unknown + 2).Unknown())
}
