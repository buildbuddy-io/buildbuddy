load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bazelrbe_lib",
    srcs = ["bazelrbe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/bazelrbe",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/bazel",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_binary(
    name = "bazelrbe",
    embed = [":bazelrbe_lib"],
    visibility = ["//visibility:public"],
)
