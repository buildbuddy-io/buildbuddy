load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "xds",
    srcs = ["xds.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/xds",
    visibility = ["//visibility:public"],
    deps = [
        "//server/resources",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "xds_test",
    srcs = [
        "nodelabels_test.go",
        "xds_test.go",
    ],
    deps = [
        ":xds",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "incluster_test",
    srcs = ["incluster_test.go"],
    embed = [":xds"],
    deps = ["@com_github_stretchr_testify//require"],
)
