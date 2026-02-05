load("@io_bazel_rules_go//go:def.bzl", "go_library")

# Embeds the bb CLI binary for Linux and mac. On Windows, we create an empty
# file instead because the CLI depends on tree-sitter which requires CGo, and
# our Windows build environment doesn't have a C compiler available.
genrule(
    name = "bb-crossplatform",
    srcs = select({
        "//platforms/configs:linux_x86_64": ["//cli/cmd/bb"],
        "//platforms/configs:linux_arm64": ["//cli/cmd/bb"],
        "//platforms/configs:macos_x86_64": ["//cli/cmd/bb"],
        "//platforms/configs:macos_arm64": ["//cli/cmd/bb"],
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
