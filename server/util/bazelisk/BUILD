load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bazelisk-1.10.1_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin_x86_64": ["@io_bazel_bazelisk-1.10.1-darwin-amd64//file:downloaded"],
        "@bazel_tools//src/conditions:darwin_arm64": ["@io_bazel_bazelisk-1.10.1-darwin-arm64//file:downloaded"],
        "//conditions:default": ["@io_bazel_bazelisk-1.10.1-linux-amd64//file:downloaded"],
    }),
    outs = ["bazelisk-1.10.1"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

go_library(
    name = "bazelisk",
    srcs = ["bazelisk.go"],
    embedsrcs = [":bazelisk-1.10.1"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazelisk",
    visibility = ["//visibility:public"],
)
