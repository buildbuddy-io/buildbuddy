load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "atime_key_compression_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/atime_key_compression",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/util/compression",
        "//server/util/proto",
        "@com_github_cockroachdb_pebble//:pebble",
    ],
)

go_binary(
    name = "atime_key_compression",
    embed = [":atime_key_compression_lib"],
    # cgo build: pebble's zstd path uses DataDog/zstd, which requires the
    # go.mod replace pinning DataDog back to v1.4.5 (pebble v1.1.4's expected
    # version; v1.5.5's Decompress buffer handling trips pebble's
    # "decompressed into unexpected buffer" guard). Without that replace,
    # build with pure = "on" to fall back to klauspost's pure-Go zstd.
    visibility = ["//visibility:public"],
)
