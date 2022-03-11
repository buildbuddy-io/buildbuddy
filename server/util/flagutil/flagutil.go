package flagutil

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
)

// NOTE: slice flags are *appended* to default values and
// config values, instead of overriding them completely.
type SliceFlag interface {
	flag.Value
	UnderlyingSlice() interface{}
	SetTo(interface{})
}

func NewSliceFlag(slicePtr interface{}) SliceFlag {
	if stringSlicePtr, ok := slicePtr.(*[]string); ok {
		return newStringSliceFlag(stringSlicePtr)
	}
	if reflect.TypeOf(slicePtr).Elem().Elem().Kind() == reflect.Struct {
		return newStructSliceFlag(slicePtr)
	}
	alert.UnexpectedEvent(
		"unrecognized_slice_pointer_for_flags",
		"Unrecognized slice pointer type in NewSliceFlag: %T",
		slicePtr,
	)
	return nil
}

type stringSliceFlag []string

func (f *stringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringSliceFlag) Set(values string) error {
	for _, val := range strings.Split(values, ",") {
		*f = append(*f, val)
	}
	return nil
}

func (f *stringSliceFlag) UnderlyingSlice() interface{} {
	slice := make([]string, len(*f))
	copy(slice, *f)
	return slice
}

func (f *stringSliceFlag) SetTo(stringSlice interface{}) {
	ss, ok := stringSlice.([]string)
	if !ok {
		alert.UnexpectedEvent("string_slice_flag_type_error", "SetTo accepts only []string, but was passed parameter of type %T", stringSlice)
		return
	}
	*f = make([]string, len(ss))
	copy(*f, ss)
}

func newStringSliceFlag(stringSlicePtr *[]string) *stringSliceFlag {
	return (*stringSliceFlag)(stringSlicePtr)
}

type structSliceFlag struct {
	dstSlice reflect.Value
}

func (f *structSliceFlag) String() string {
	b, err := json.Marshal(f.dstSlice.Interface())
	if err != nil {
		alert.UnexpectedEvent("config_cannot_marshal_struct", "err: %s", err)
		return "[]"
	}
	return string(b)
}

func (f *structSliceFlag) Set(value string) error {
	var i interface{}
	if err := json.Unmarshal([]byte(value), &i); err != nil {
		return err
	}
	if _, ok := i.([]interface{}); ok {
		dst := reflect.New(f.dstSlice.Type())
		if err := json.Unmarshal([]byte(value), dst.Interface()); err != nil {
			return err
		}
		f.dstSlice.Set(reflect.AppendSlice(f.dstSlice, dst.Elem()))
		return nil
	}
	if _, ok := i.(map[string]interface{}); ok {
		dst := reflect.New(f.dstSlice.Type().Elem())
		if err := json.Unmarshal([]byte(value), dst.Interface()); err != nil {
			return err
		}
		f.dstSlice.Set(reflect.Append(f.dstSlice, dst.Elem()))
		return nil
	}
	return errors.New(fmt.Sprintf("Set for structSliceFlag can only accept JSON objects or arrays, but type was %T", i))
}

func (f *structSliceFlag) UnderlyingSlice() interface{} {
	slice := reflect.MakeSlice(f.dstSlice.Type(), f.dstSlice.Len(), f.dstSlice.Len())
	reflect.Copy(slice, f.dstSlice)
	return slice.Interface()
}

func (f *structSliceFlag) SetTo(structSlice interface{}) {
	if reflect.TypeOf(structSlice).Kind() != reflect.Slice {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "SetTo accepts only slices of struct types, but was passed parameter of type %T", structSlice)
		return
	}
	if reflect.TypeOf(structSlice).Elem().Kind() != reflect.Struct {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "SetTo accepts only slices of struct types, but was passed parameter of type %T", structSlice)
		return
	}
	if reflect.TypeOf(structSlice) != f.dstSlice.Type() {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "SetTo was passed a slice of %T, which cannot be appended to a slice of type %T", structSlice, f.UnderlyingSlice())
		return
	}
	length := reflect.ValueOf(structSlice).Len()
	f.dstSlice.Set(reflect.MakeSlice(f.dstSlice.Type(), length, length))
	reflect.Copy(f.dstSlice, reflect.ValueOf(structSlice))
}

func newStructSliceFlag(structSlicePtr interface{}) *structSliceFlag {
	return &structSliceFlag{reflect.ValueOf(structSlicePtr).Elem()}
}
