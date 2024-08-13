load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "server",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/kythe/server",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/yaml",
        "@io_kythe//kythe/go/services/filetree",
        "@io_kythe//kythe/go/services/graph",
        "@io_kythe//kythe/go/services/xrefs",
        "@io_kythe//kythe/go/serving/filetree",
        "@io_kythe//kythe/go/serving/graph",
        "@io_kythe//kythe/go/serving/identifiers",
        "@io_kythe//kythe/go/serving/xrefs",
        "@io_kythe//kythe/go/storage/keyvalue",
        "@io_kythe//kythe/go/storage/pebble",
        "@io_kythe//kythe/go/storage/table",
    ],
)
