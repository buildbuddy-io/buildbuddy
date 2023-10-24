load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "flag",
    srcs = ["flag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flag",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/types/autoflags",
    ],
)
