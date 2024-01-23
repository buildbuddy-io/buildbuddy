load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "featureflag_cache",
    srcs = ["featureflag_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/featureflag/featureflag_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/lru",
    ],
)
