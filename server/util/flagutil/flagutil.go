package flagutil

import (
	"flag"
	"strings"
)

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

func StringSlice(name string, usage string) *StringSliceFlag {
	f := &StringSliceFlag{}
	flag.Var(f, name, usage)
	return f
}
