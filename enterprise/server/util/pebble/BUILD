load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "pebble",
    srcs = ["pebble.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
    ],
)

go_test(
    name = "pebble_test",
    srcs = ["pebble_test.go"],
    deps = [
        ":pebble",
        "//server/testutil/testfs",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
