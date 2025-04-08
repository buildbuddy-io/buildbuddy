load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "mockgcs",
    srcs = ["mockgcs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/v2/server/testutil/mockgcs",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/ioutil",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)
