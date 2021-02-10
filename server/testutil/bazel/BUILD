load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bazel-3.7.0_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin": [":bin/bazel-3.7.0-darwin-x86_64"],
        "//conditions:default": [":bin/bazel-3.7.0-linux-x86_64"],
    }),
    outs = ["bazel-3.7.0"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

go_library(
    name = "go_default_library",
    srcs = ["bazel.go"],
    data = [":bazel-3.7.0"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/bazel",
    visibility = ["//visibility:public"],
    deps = [
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
