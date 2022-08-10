load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "flagutil",
    srcs = ["flagutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/types/autoflags",
    ],
)
