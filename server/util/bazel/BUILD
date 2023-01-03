load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")
load(":defs.bzl", "extract_bazel_installation")

genrule(
    name = "bazel-3.7_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": ["@io_bazel_bazel-3.7-darwin-x86_64//file:downloaded"],
        "//conditions:default": ["@io_bazel_bazel-3.7-linux-x86_64//file:downloaded"],
    }),
    outs = ["bazel-3.7"],
    cmd_bash = "cp $(SRCS) $@",
    executable = True,
    visibility = ["//visibility:public"],
)

genrule(
    name = "bazel-4.1_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": ["@io_bazel_bazel-4.1-darwin-x86_64//file:downloaded"],
        "//conditions:default": ["@io_bazel_bazel-4.1-linux-x86_64//file:downloaded"],
    }),
    outs = ["bazel-4.1"],
    cmd_bash = "cp $(SRCS) $@",
    executable = True,
    visibility = ["//visibility:public"],
)

genrule(
    name = "bazel-5.3.2_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": ["@io_bazel_bazel-5.3.2-darwin-x86_64//file:downloaded"],
        "//conditions:default": ["@io_bazel_bazel-5.3.2-linux-x86_64//file:downloaded"],
    }),
    outs = ["bazel-5.3.2"],
    cmd_bash = "cp $(SRCS) $@",
    executable = True,
    visibility = ["//visibility:public"],
)

extract_bazel_installation(
    name = "bazel-5.3.2_extract_installation",
    bazel = ":bazel-5.3.2_crossplatform",
    out_dir = "bazel-5.3.2_install",
    visibility = ["//visibility:public"],
)

genrule(
    name = "bazel-6.0.0_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": ["@io_bazel_bazel-6.0.0-darwin-x86_64//file:downloaded"],
        "//conditions:default": ["@io_bazel_bazel-6.0.0-linux-x86_64//file:downloaded"],
    }),
    outs = ["bazel-6.0.0"],
    cmd_bash = "cp $(SRCS) $@",
    executable = True,
    visibility = ["//visibility:public"],
)

extract_bazel_installation(
    name = "bazel-6.0.0_extract_installation",
    bazel = ":bazel-6.0.0_crossplatform",
    out_dir = "bazel-6.0.0_install",
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "bazel_binaries_tar",
    srcs = [
        ":bazel-3.7_crossplatform",
        ":bazel-4.1_crossplatform",
        ":bazel-5.3.2_crossplatform",
        ":bazel-6.0.0_crossplatform",
    ],
    package_dir = "/bazel",
    visibility = ["//visibility:public"],
)

go_library(
    name = "bazel",
    srcs = ["bazel.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)
