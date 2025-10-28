load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "poolviz_lib",
    srcs = ["poolviz.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/poolviz",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@org_golang_x_term//:term",
    ],
)

go_binary(
    name = "poolviz",
    embed = [":poolviz_lib"],
    visibility = ["//visibility:public"],
)
