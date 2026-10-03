load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_experiments",
    srcs = [
        "execution_experiments.go",
        "execution_experiments_linux.go",
        "execution_experiments_notlinux.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_experiments",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:experiments_go_proto",
        "//server/util/expflag",
    ],
)
