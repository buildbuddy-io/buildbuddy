load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "flagz",
    srcs = ["flagz.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagz",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil",
        "//server/util/flagutil/types",
        "//server/util/flagutil/yaml",
    ],
)
