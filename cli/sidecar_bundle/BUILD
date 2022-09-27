load("@io_bazel_rules_go//go:def.bzl", "go_library")

genrule(
    name = "embedsrcs_compatible_sidecar",
    srcs = ["//cli/cmd/sidecar"],
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
