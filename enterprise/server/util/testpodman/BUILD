load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testpodman",
    testonly = 1,
    srcs = ["testpodman.go"],
    data = [
        "//enterprise/server/remote_execution/containers/podman:podman-static.tar.gz",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/testpodman",
    x_defs = {
        "podmanArchiveRlocationpath": "$(rlocationpath //enterprise/server/remote_execution/containers/podman:podman-static.tar.gz)",
    },
    deps = [
        "@io_bazel_rules_go//go/runfiles",
    ],
)
