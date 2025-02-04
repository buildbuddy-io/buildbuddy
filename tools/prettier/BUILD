load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "prettier_lib",
    srcs = ["prettier.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/prettier",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "prettier",
    embed = [":prettier_lib"],
    visibility = ["//visibility:public"],
)
