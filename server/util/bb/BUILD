load("@io_bazel_rules_go//go:def.bzl", "go_library")

# Embeds the bb CLI binary for Linux and macOS. //cli/cmd/bb is pure Go, so
# the Linux output is already static and does not need a separate musl build.
# On Windows, keep producing an empty file until executor embedding there is
# deliberately enabled and tested.
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
