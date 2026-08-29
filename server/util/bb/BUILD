load("@io_bazel_rules_go//go:def.bzl", "go_library")

# Embeds the bb CLI binary for Linux and mac. //cli/cmd/bb is built with
# pure = "on" (no cgo), so on Linux it is already a static binary and no
# separate musl-linked build is needed. On Windows we still create an empty
# file; now that the CLI has no cgo dependency that could be revisited.
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
