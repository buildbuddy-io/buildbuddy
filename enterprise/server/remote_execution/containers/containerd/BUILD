load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "containerd",
    srcs = ["containerd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/containerd",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/background",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@com_github_containerd_containerd//:containerd",
        "@com_github_containerd_containerd//cio",
        "@com_github_containerd_containerd//errdefs",
        "@com_github_containerd_containerd//namespaces",
        "@com_github_containerd_containerd//oci",
        "@com_github_containerd_containerd//remotes",
        "@com_github_containerd_containerd//remotes/docker",
        "@com_github_containerd_containerd//remotes/docker/config",
        "@com_github_opencontainers_runtime_spec//specs-go",
    ],
)
