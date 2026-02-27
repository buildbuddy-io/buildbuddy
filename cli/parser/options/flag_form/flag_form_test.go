package flag_form_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/options/flag_form"
	"github.com/stretchr/testify/assert"
)

func TestNegative(t *testing.T) {
	assert.False(t, flag_form.Name.Negative())
	assert.True(t, flag_form.NegativeName.Negative())
	assert.False(t, flag_form.ShortName.Negative())
	assert.False(t, flag_form.OldName.Negative())
	assert.True(t, flag_form.NegativeOldName.Negative())
	assert.False(t, flag_form.Unknown.Negative())
	assert.False(t, (flag_form.Unknown + 1).Negative())
	assert.False(t, (flag_form.Unknown + 2).Negative())
}

func TestSetNegative(t *testing.T) {
	assert.Equal(t, flag_form.NegativeName, flag_form.Name.SetNegative())
	assert.Equal(t, flag_form.NegativeName, flag_form.NegativeName.SetNegative())
	assert.Equal(t, flag_form.NegativeOldName, flag_form.OldName.SetNegative())
	assert.Equal(t, flag_form.Unknown, flag_form.ShortName.SetNegative())
	assert.Equal(t, flag_form.NegativeOldName, flag_form.NegativeOldName.SetNegative())
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.SetNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 1).SetNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 2).SetNegative())
}

func TestClearNegative(t *testing.T) {
	assert.Equal(t, flag_form.Name, flag_form.Name.ClearNegative())
	assert.Equal(t, flag_form.Name, flag_form.NegativeName.ClearNegative())
	assert.Equal(t, flag_form.OldName, flag_form.OldName.ClearNegative())
	assert.Equal(t, flag_form.OldName, flag_form.NegativeOldName.ClearNegative())
	assert.Equal(t, flag_form.ShortName, flag_form.ShortName.ClearNegative())
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.ClearNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 1).ClearNegative())
	assert.Equal(t, flag_form.Unknown, (flag_form.Unknown + 2).ClearNegative())
}

func TestCompareNameTypes(t *testing.T) {
	// Compare against Standard
	assert.True(t, flag_form.Name.CompareNameType(flag_form.Name))
	assert.True(t, flag_form.Name.CompareNameType(flag_form.NegativeName))
	assert.False(t, flag_form.Name.CompareNameType(flag_form.OldName))
	assert.False(t, flag_form.Name.CompareNameType(flag_form.NegativeOldName))
	assert.False(t, flag_form.Name.CompareNameType(flag_form.ShortName))
	assert.False(t, flag_form.Name.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.Name.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.Name.CompareNameType(flag_form.Unknown+2))

	// Compare against Negative
	assert.True(t, flag_form.NegativeName.CompareNameType(flag_form.Name))
	assert.True(t, flag_form.NegativeName.CompareNameType(flag_form.NegativeName))
	assert.False(t, flag_form.NegativeName.CompareNameType(flag_form.OldName))
	assert.False(t, flag_form.NegativeName.CompareNameType(flag_form.NegativeOldName))
	assert.False(t, flag_form.NegativeName.CompareNameType(flag_form.ShortName))
	assert.False(t, flag_form.NegativeName.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.NegativeName.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.NegativeName.CompareNameType(flag_form.Unknown+2))

	// Compare against Old
	assert.False(t, flag_form.OldName.CompareNameType(flag_form.Name))
	assert.False(t, flag_form.OldName.CompareNameType(flag_form.NegativeName))
	assert.True(t, flag_form.OldName.CompareNameType(flag_form.OldName))
	assert.True(t, flag_form.OldName.CompareNameType(flag_form.NegativeOldName))
	assert.False(t, flag_form.OldName.CompareNameType(flag_form.ShortName))
	assert.False(t, flag_form.OldName.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.OldName.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.OldName.CompareNameType(flag_form.Unknown+2))

	// Compare against OldNegative
	assert.False(t, flag_form.NegativeOldName.CompareNameType(flag_form.Name))
	assert.False(t, flag_form.NegativeOldName.CompareNameType(flag_form.NegativeName))
	assert.True(t, flag_form.NegativeOldName.CompareNameType(flag_form.OldName))
	assert.True(t, flag_form.NegativeOldName.CompareNameType(flag_form.NegativeOldName))
	assert.False(t, flag_form.NegativeOldName.CompareNameType(flag_form.ShortName))
	assert.False(t, flag_form.NegativeOldName.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.NegativeOldName.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.NegativeOldName.CompareNameType(flag_form.Unknown+2))

	// Compare against Short
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.Name))
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.NegativeName))
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.OldName))
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.NegativeOldName))
	assert.True(t, flag_form.ShortName.CompareNameType(flag_form.ShortName))
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.ShortName.CompareNameType(flag_form.Unknown+2))

	// Compare against Unknown
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Name))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.NegativeName))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.OldName))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.NegativeOldName))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.ShortName))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Unknown))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Unknown+1))
	assert.False(t, flag_form.Unknown.CompareNameType(flag_form.Unknown+2))
}

func TestAsNameType(t *testing.T) {
	// Convert from Standard
	assert.Equal(t, flag_form.Name, flag_form.Name.AsNameType(flag_form.Name))
	assert.Equal(t, flag_form.Name, flag_form.Name.AsNameType(flag_form.NegativeName))
	assert.Equal(t, flag_form.ShortName, flag_form.Name.AsNameType(flag_form.ShortName))
	assert.Equal(t, flag_form.Unknown, flag_form.Name.AsNameType(flag_form.Unknown))
	assert.Equal(t, flag_form.Unknown, flag_form.Name.AsNameType(flag_form.Unknown+1))
	assert.Equal(t, flag_form.Unknown, flag_form.Name.AsNameType(flag_form.Unknown+2))

	// Convert from Negative
	assert.Equal(t, flag_form.NegativeName, flag_form.NegativeName.AsNameType(flag_form.Name))
	assert.Equal(t, flag_form.NegativeName, flag_form.NegativeName.AsNameType(flag_form.NegativeName))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeName.AsNameType(flag_form.ShortName))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeName.AsNameType(flag_form.Unknown))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeName.AsNameType(flag_form.Unknown+1))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeName.AsNameType(flag_form.Unknown+2))

	// Convert from Old
	assert.Equal(t, flag_form.Name, flag_form.OldName.AsNameType(flag_form.Name))
	assert.Equal(t, flag_form.Name, flag_form.OldName.AsNameType(flag_form.NegativeName))
	assert.Equal(t, flag_form.ShortName, flag_form.OldName.AsNameType(flag_form.ShortName))
	assert.Equal(t, flag_form.Unknown, flag_form.OldName.AsNameType(flag_form.Unknown))
	assert.Equal(t, flag_form.Unknown, flag_form.OldName.AsNameType(flag_form.Unknown+1))
	assert.Equal(t, flag_form.Unknown, flag_form.OldName.AsNameType(flag_form.Unknown+2))

	// Convert from OldNegative
	assert.Equal(t, flag_form.NegativeName, flag_form.NegativeOldName.AsNameType(flag_form.Name))
	assert.Equal(t, flag_form.NegativeName, flag_form.NegativeOldName.AsNameType(flag_form.NegativeName))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeOldName.AsNameType(flag_form.ShortName))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeOldName.AsNameType(flag_form.Unknown))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeOldName.AsNameType(flag_form.Unknown+1))
	assert.Equal(t, flag_form.Unknown, flag_form.NegativeOldName.AsNameType(flag_form.Unknown+2))

	// Convert from Short
	assert.Equal(t, flag_form.Name, flag_form.ShortName.AsNameType(flag_form.Name))
	assert.Equal(t, flag_form.Name, flag_form.ShortName.AsNameType(flag_form.NegativeName))
	assert.Equal(t, flag_form.ShortName, flag_form.ShortName.AsNameType(flag_form.ShortName))
	assert.Equal(t, flag_form.Unknown, flag_form.ShortName.AsNameType(flag_form.Unknown))
	assert.Equal(t, flag_form.Unknown, flag_form.ShortName.AsNameType(flag_form.Unknown+1))
	assert.Equal(t, flag_form.Unknown, flag_form.ShortName.AsNameType(flag_form.Unknown+2))

	// Convert from Unknown
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.AsNameType(flag_form.Name))
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.AsNameType(flag_form.NegativeName))
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.AsNameType(flag_form.ShortName))
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.AsNameType(flag_form.Unknown))
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.AsNameType(flag_form.Unknown+1))
	assert.Equal(t, flag_form.Unknown, flag_form.Unknown.AsNameType(flag_form.Unknown+2))
}

func TestUnknown(t *testing.T) {
	assert.False(t, flag_form.Name.Unknown())
	assert.False(t, flag_form.NegativeName.Unknown())
	assert.False(t, flag_form.OldName.Unknown())
	assert.False(t, flag_form.NegativeOldName.Unknown())
	assert.False(t, flag_form.ShortName.Unknown())
	assert.True(t, flag_form.Unknown.Unknown())
	assert.True(t, (flag_form.Unknown + 1).Unknown())
	assert.True(t, (flag_form.Unknown + 2).Unknown())
}
