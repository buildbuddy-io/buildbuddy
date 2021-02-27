package dynamic

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	digitsRegexp = regexp.MustCompile(`^\d+$`)
)

// FieldValues extracts values from an object given dot-separated field paths, returning
// a mapping from each field path to the string representation of the value at that path.
//
// It is mostly useful when dealing with deeply nested structs parsed from a third-party
// source.
//
// As paths are traversed, pointers are de-referenced (but only up to one level deep).
// If a nil pointer is encountered the access path, an error is returned.
//
// Number-valued field names refer to slice indexes.
func FieldValues(obj interface{}, fieldPaths ...string) (map[string]string, error) {
	values := map[string]string{}
	objVal := reflect.Indirect(reflect.ValueOf(obj))
	if !objVal.IsValid() {
		return nil, fmt.Errorf("call of fieldValues on nil value")
	}
	for _, path := range fieldPaths {
		fields := strings.Split(path, ".")
		cur := objVal
		curPath := []string{}
		for _, name := range fields {
			t := cur.Kind()

			if t == reflect.Struct {
				cur = cur.FieldByName(name)
				if !cur.IsValid() {
					return nil, fmt.Errorf("invalid field %q of %q", name, strings.Join(curPath, "."))
				}
				cur = reflect.Indirect(cur)
				if !cur.IsValid() {
					return nil, fmt.Errorf("nil value of %q", strings.Join(curPath, "."))
				}
			} else if t == reflect.Slice || t == reflect.Array {
				if !digitsRegexp.MatchString(name) {
					return nil, fmt.Errorf("invalid field access %q of list %q", name, strings.Join(curPath, "."))
				}
				index64, err := strconv.ParseInt(name, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("failed to parse %q as int: %s", name, err)
				}
				index := int(index64)
				cur = reflect.Indirect(cur)
				if !cur.IsValid() {
					return nil, fmt.Errorf("nil value at %q", strings.Join(curPath, "."))
				}
				if int(index) >= cur.Len() {
					return nil, fmt.Errorf("out of bounds: index %q of %q", name, strings.Join(curPath, "."))
				}
				cur = reflect.Indirect(cur.Index(index))
				if !cur.IsValid() {
					return nil, fmt.Errorf("nil value of %q", strings.Join(curPath, "."))
				}
			} else {
				return nil, fmt.Errorf("cannot access field %q of %T %q", name, cur.Interface(), strings.Join(curPath, "."))
			}
			curPath = append(curPath, name)
		}
		el := cur.Interface()
		str, ok := el.(string)
		if !ok {
			str = fmt.Sprintf("%v", el)
		}
		values[path] = str
	}
	return values, nil
}
