load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tags",
    srcs = ["tags.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/yaml",
        "//server/util/log",
    ],
)
