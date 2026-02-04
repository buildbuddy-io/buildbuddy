//go:build pure
// +build pure

package main

import (
	"github.com/bazelbuild/bazel-gazelle/language"
	"github.com/bazelbuild/bazel-gazelle/language/bazel/visibility"
	"github.com/bazelbuild/bazel-gazelle/language/proto"
	"github.com/buildbuddy-io/buildbuddy/cli/fix/golang"
)

var languages = []language.Language{
	proto.NewLanguage(),
	golang.NewLanguage(),
	visibility.NewLanguage(),
}

