load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "copy_install",
    srcs = ["//cli:install.sh"],
    outs = ["install.sh"],
    cmd_bash = "cp $(SRCS) $@",
)

go_library(
    name = "update",
    srcs = ["update.go"],
    embedsrcs = ["install.sh"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/update",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/workspace",
    ],
)
