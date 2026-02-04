load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bb-crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin_x86_64": ["//cli/cmd/bb:bb-darwin-amd64"],
        "@bazel_tools//src/conditions:darwin_arm64": ["//cli/cmd/bb:bb-darwin-arm64"],
        "@bazel_tools//src/conditions:linux_aarch64": ["//cli/cmd/bb:bb-linux-arm64"],
        "//conditions:default": ["//cli/cmd/bb:bb-linux-amd64"],
    }),
    outs = ["bb-bin"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)


go_library(
    name = "bb",
    srcs = ["bb.go"],
    embedsrcs = [
        "BUILD",
        "bb-bin",
        "bb.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bb",
    visibility = ["//visibility:public"],
)
