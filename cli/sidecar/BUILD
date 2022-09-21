load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "sidecar",
    srcs = ["sidecar.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/sidecar",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/download",
        "//cli/logging",
        "//cli/sidecar_bundle",
        "@org_golang_x_mod//semver",
    ],
)
