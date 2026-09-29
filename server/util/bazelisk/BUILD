load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@io_bazel_rules_go//go:def.bzl", "go_library")

copy_file(
    name = "bazelisk-crossplatform",
    src = select({
        "@bazel_tools//src/conditions:darwin_x86_64": "@com_github_bazelbuild_bazelisk-bazelisk-darwin-amd64//file:downloaded",
        "@bazel_tools//src/conditions:darwin_arm64": "@com_github_bazelbuild_bazelisk-bazelisk-darwin-arm64//file:downloaded",
        "@bazel_tools//src/conditions:linux_aarch64": "@com_github_bazelbuild_bazelisk-bazelisk-linux-arm64//file:downloaded",
        "@bazel_tools//src/conditions:windows_x64": "@com_github_bazelbuild_bazelisk-bazelisk-windows-amd64//file:downloaded",
        "//conditions:default": "@com_github_bazelbuild_bazelisk-bazelisk-linux-amd64//file:downloaded",
    }),
    out = "bazelisk-bin",
    allow_symlink = True,
    visibility = ["//visibility:public"],
)

go_library(
    name = "bazelisk",
    srcs = ["bazelisk.go"],
    embedsrcs = [":bazelisk-bin"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazelisk",
    visibility = ["//visibility:public"],
)
