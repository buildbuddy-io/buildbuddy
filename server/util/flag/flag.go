package flag

import (
	"flag"
	"log"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags"
)

var Secret = autoflags.SecretTag
var Deprecated = autoflags.DeprecatedTag
var YAMLIgnore = autoflags.YAMLIgnoreTag

var NewFlagSet = flag.NewFlagSet
var ContinueOnError = flag.ContinueOnError
var CommandLine = flag.CommandLine
var Args = flag.Args

type Flag = flag.Flag
type FlagSet = flag.FlagSet

func Parse() {
	flag.Parse()
}

func String(name string, value string, usage string, tags ...autoflags.Taggable) *string {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Bool(name string, value bool, usage string, tags ...autoflags.Taggable) *bool {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Int(name string, value int, usage string, tags ...autoflags.Taggable) *int {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Int64(name string, value int64, usage string, tags ...autoflags.Taggable) *int64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func UInt(name string, value uint, usage string, tags ...autoflags.Taggable) *uint {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func UInt64(name string, value uint64, usage string, tags ...autoflags.Taggable) *uint64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Float64(name string, value float64, usage string, tags ...autoflags.Taggable) *float64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Duration(name string, value time.Duration, usage string, tags ...autoflags.Taggable) *time.Duration {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func URL(name string, value string, usage string, tags ...autoflags.Taggable) *url.URL {
	u, err := url.Parse(value)
	if err != nil {
		log.Fatalf("Error parsing default URL value '%s' for flag: %v", value, err)
		return nil
	}
	return autoflags.New(common.DefaultFlagSet, name, *u, usage, tags...)
}

func Slice[T any](name string, value []T, usage string, tags ...autoflags.Taggable) *[]T {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Struct[T any](name string, value T, usage string, tags ...autoflags.Taggable) *T {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}
