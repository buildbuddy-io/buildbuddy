load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "tmp_runopt_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/tmp_runopt",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/posting",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_roaringbitmap_roaring//roaring64",
    ],
)

go_binary(
    name = "tmp_runopt",
    embed = [":tmp_runopt_lib"],
    visibility = ["//visibility:public"],
)
