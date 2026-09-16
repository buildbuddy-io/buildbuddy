load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "steps",
    srcs = ["steps.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/steps",
    deps = ["//proto:runner_go_proto"],
)
