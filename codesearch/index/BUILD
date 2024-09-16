load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "index",
    srcs = ["index.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/index",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/performance",
        "//codesearch/posting",
        "//codesearch/token",
        "//codesearch/types",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_xiam_s_expr//ast",
        "@com_github_xiam_s_expr//parser",
        "@org_golang_x_exp//maps",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "index_test",
    srcs = ["index_test.go"],
    embed = [":index"],
    deps = [
        "//codesearch/posting",
        "//codesearch/types",
        "//server/testutil/testfs",
        "//server/util/log",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
