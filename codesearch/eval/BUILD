load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "eval_lib",
    srcs = ["searcheval.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/eval",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "searcheval",
    embed = [":eval_lib"],
    visibility = ["//visibility:public"],
)
