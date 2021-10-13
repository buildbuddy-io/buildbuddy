load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "approximatelru",
    srcs = ["approximatelru.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/approximatelru",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cespare_xxhash_v2//:xxhash",
    ],
)
