load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

sh_binary(
    name = "prettier",
    srcs = ["prettier.sh"],
    data = [
        "@npm//prettier/bin:prettier",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@bazel_tools//tools/bash/runfiles",
    ],
)

go_library(
    name = "go_prettier_lib",
    srcs = ["prettier.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/prettier",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "go_prettier",
    embed = [":go_prettier_lib"],
    visibility = ["//visibility:public"],
)
