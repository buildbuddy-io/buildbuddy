package fieldgetter_test

import (
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Parent struct {
	Str       string
	Bool      bool
	Child     *Child
	StrList   []string
	ChildList []*Child
}

type Child struct {
	ChildString string
}

func TestDynamic(t *testing.T) {
	for _, test := range []struct {
		val      *Parent
		path     string
		expected string
	}{
		{&Parent{Str: "hello"}, "Str", "hello"},
		{&Parent{Bool: true}, "Bool", "true"},
		{&Parent{StrList: []string{"hello"}}, "StrList.0", "hello"},
		{&Parent{ChildList: []*Child{{ChildString: "hello"}}}, "ChildList.0.ChildString", "hello"},
	} {
		vals, err := fieldgetter.FieldValues(test.val, test.path)

		assert.NoError(t, err, test.path)
		assert.Equal(t, test.expected, vals[test.path])
	}

	for _, test := range []struct {
		val    *Parent
		path   string
		errMsg string
	}{
		{&Parent{StrList: []string{"hello"}}, "Str.0", `cannot access field "0" of string "Str"`},
		{&Parent{StrList: []string{"hello"}}, "Str.Foo", `cannot access field "Foo" of string "Str"`},
	} {
		vals, err := fieldgetter.FieldValues(test.val, test.path)

		assert.Nil(t, vals)
		assert.Equal(t, test.errMsg, fmt.Sprintf("%s", err))
	}
}
