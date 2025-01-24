package fieldgetter_test

import (
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Parent struct {
	Child     *Child
	Str       string
	StrList   []string
	ChildList []*Child
	Bool      bool
}

type Child struct {
	ChildString string
}

func TestExtractValues(t *testing.T) {
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
		vals, err := fieldgetter.ExtractValues(test.val, test.path)

		assert.NoError(t, err, test.path)
		assert.Equal(t, test.expected, vals[test.path])
	}

	for _, test := range []struct {
		val    *Parent
		path   string
		errMsg string
	}{
		{nil, "<ANY_FIELD_NAME>", `call of ExtractValues on nil value`},
		{&Parent{}, "<INVALID_FIELD_NAME>", `invalid field "<INVALID_FIELD_NAME>" (parent path: "")`},
		{&Parent{Child: &Child{}}, "Child.<INVALID_FIELD_NAME>", `invalid field "<INVALID_FIELD_NAME>" (parent path: "Child")`},
		{&Parent{}, "ChildList.9999999999", `strconv.ParseInt: parsing "9999999999": value out of range`},
		{&Parent{}, "ChildList.Foo", `invalid field access of "Foo" on list (parent path: "ChildList")`},
		{&Parent{}, "ChildList.0", `out of bounds: index 0 of "ChildList"`},
		{&Parent{}, "Child", `nil value of "Child"`},
		{&Parent{StrList: []string{"hello"}}, "Str.0", `cannot access field "0" of string "Str"`},
		{&Parent{StrList: []string{"hello"}}, "Str.Foo", `cannot access field "Foo" of string "Str"`},
	} {
		vals, err := fieldgetter.ExtractValues(test.val, test.path)

		assert.Nil(t, vals)
		assert.Equal(t, test.errMsg, fmt.Sprintf("%s", err))
	}
}
