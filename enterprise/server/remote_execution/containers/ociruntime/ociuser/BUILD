load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "ociuser",
    srcs = ["ociuser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/ociuser",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/unixcred",
        "@com_github_opencontainers_runtime_spec//specs-go",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
