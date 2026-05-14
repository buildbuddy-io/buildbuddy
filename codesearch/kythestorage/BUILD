load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "kythestorage",
    srcs = ["kythestorage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/kythestorage",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/claims",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@io_kythe//kythe/go/storage/keyvalue",
    ],
)
