load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "approxlru",
    srcs = ["approxlru.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/approxlru",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "approxlru_test",
    srcs = ["approxlru_test.go"],
    deps = [
        ":approxlru",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
