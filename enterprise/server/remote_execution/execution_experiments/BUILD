load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_experiments",
    srcs = ["execution_experiments.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_experiments",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:experiments_go_proto",
        "//server/util/expflag",
    ],
)

go_test(
    name = "execution_experiments_test",
    size = "small",
    srcs = ["execution_experiments_test.go"],
    embed = [":execution_experiments"],
    embedsrcs = ["execution_experiments.go"],
    deps = ["@com_github_stretchr_testify//require"],
)
