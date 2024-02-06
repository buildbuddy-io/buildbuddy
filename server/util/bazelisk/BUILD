load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bazelisk-binary",
    srcs = select({
        "@bazel_tools//src/conditions:darwin_x86_64": ["@com_github_bazelbuild_bazelisk//:bazelisk-darwin-amd64"],
        "@bazel_tools//src/conditions:darwin_arm64": ["@com_github_bazelbuild_bazelisk//:bazelisk-darwin-arm64"],
        "//conditions:default": ["@com_github_bazelbuild_bazelisk//:bazelisk-linux-amd64"],
    }),
    outs = ["bazelisk-bin"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

go_library(
    name = "bazelisk",
    srcs = ["bazelisk.go"],
    embedsrcs = [":bazelisk-bin"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazelisk",
    visibility = ["//visibility:public"],
)
