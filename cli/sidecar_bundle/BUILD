load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "sidecar_crossplatform",
    srcs = select({
        "@bazel_tools//src/conditions:darwin_x86_64": ["//cli/cmd/sidecar:sidecar-darwin-amd64"],
        "@bazel_tools//src/conditions:darwin_arm64": ["//cli/cmd/sidecar:sidecar-darwin-arm64"],
        "//conditions:default": ["//cli/cmd/sidecar:sidecar-linux-amd64"],
    }),
    outs = ["sidecar-binary"],
    cmd_bash = "cp $(SRCS) $@",
    visibility = ["//visibility:public"],
)

go_library(
    name = "sidecar_bundle",
    srcs = ["sidecar_bundle.go"],
    embedsrcs = [":sidecar-binary"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/sidecar_bundle",
    visibility = ["//visibility:public"],
)
