load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "bb-crossplatform",
    srcs = select({
        "//platforms/configs:linux_x86_64": ["//cli/cmd/bb"],
        "//platforms/configs:linux_arm64": ["//cli/cmd/bb"],
        "@bazel_tools//src/conditions:darwin_x86_64": ["//cli/cmd/bb"],
        "@bazel_tools//src/conditions:darwin_arm64": ["//cli/cmd/bb"],
        "//conditions:default": [],
    }),
    outs = ["bb-bin"],
    cmd_bash = "if [ -n '$(SRCS)' ]; then cp $(SRCS) $@; else touch $@; fi",
    visibility = ["//visibility:public"],
)

go_library(
    name = "bb",
    srcs = ["bb.go"],
    embedsrcs = ["bb-bin"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bb",
    visibility = ["//visibility:public"],
)
