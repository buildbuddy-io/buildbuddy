load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "launcher",
    embed = [":launcher_lib"],
)

go_library(
    name = "launcher_lib",
    srcs = ["launcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/launcher",
    deps = [
        "//enterprise/server/remote_execution/containers/ociruntime/ociuser",
        "@com_github_opencontainers_runtime_spec//specs-go",
        "@org_golang_x_sys//unix",
    ],
)
