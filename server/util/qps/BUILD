load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "qps",
    srcs = ["qps.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/qps",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "qps_test",
    size = "small",
    srcs = ["qps_test.go"],
    embed = [":qps"],
    deps = [
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
