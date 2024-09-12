load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "searcher",
    srcs = ["searcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/searcher",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/performance",
        "//codesearch/types",
        "//server/util/log",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "searcher_test",
    srcs = ["searcher_test.go"],
    deps = [
        ":searcher",
        "//codesearch/index",
        "//codesearch/types",
        "//server/testutil/testfs",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
