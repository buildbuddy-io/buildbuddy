load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "analyze",
    srcs = ["analyze.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/analyze",
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/workspace",
        "//proto:bazel_query_go_proto",
        "//server/util/proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "analyze_test",
    srcs = ["analyze_test.go"],
    embed = [":analyze"],
    deps = ["//proto:bazel_query_go_proto"],
)
