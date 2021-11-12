package flagutil

import "strings"

type StringSliceFlag []string

func (f *StringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *StringSliceFlag) Set(values string) error {
	for _, val := range strings.Split(values, ",") {
		*f = append(*f, val)
	}
	return nil
}
