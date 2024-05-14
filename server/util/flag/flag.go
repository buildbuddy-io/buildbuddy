package flag

import (
	"flag"
	"log"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags"

	flagtags "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags"
)

var Secret = flagtags.SecretTag
var Deprecated = flagtags.DeprecatedTag
var YAMLIgnore = flagtags.YAMLIgnoreTag

var NewFlagSet = flag.NewFlagSet
var ContinueOnError = flag.ContinueOnError
var ErrHelp = flag.ErrHelp
var CommandLine = flag.CommandLine
var Args = flag.Args

type Flag = flag.Flag
type FlagSet = flag.FlagSet

func New[T any](flagset *flag.FlagSet, name string, defaultValue T, usage string, tags ...flagtags.Taggable) *T {
	return autoflags.New[T](flagset, name, defaultValue, usage, tags...)
}

func Parse() {
	flag.Parse()
}

func String(name string, value string, usage string, tags ...flagtags.Taggable) *string {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Bool(name string, value bool, usage string, tags ...flagtags.Taggable) *bool {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Int(name string, value int, usage string, tags ...flagtags.Taggable) *int {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Int64(name string, value int64, usage string, tags ...flagtags.Taggable) *int64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func UInt(name string, value uint, usage string, tags ...flagtags.Taggable) *uint {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func UInt64(name string, value uint64, usage string, tags ...flagtags.Taggable) *uint64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Float64(name string, value float64, usage string, tags ...flagtags.Taggable) *float64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Duration(name string, value time.Duration, usage string, tags ...flagtags.Taggable) *time.Duration {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func URL(name string, value string, usage string, tags ...flagtags.Taggable) *url.URL {
	u, err := url.Parse(value)
	if err != nil {
		log.Fatalf("Error parsing default URL value '%s' for flag: %v", value, err)
		return nil
	}
	return autoflags.New(common.DefaultFlagSet, name, *u, usage, tags...)
}

func Slice[T any](name string, value []T, usage string, tags ...flagtags.Taggable) *[]T {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Struct[T any](name string, value T, usage string, tags ...flagtags.Taggable) *T {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}
