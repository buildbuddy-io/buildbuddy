load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sociartifactstore",
    srcs = ["sociartifactstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/sociartifactstore",
    target_compatible_with = [
        "@platforms//os:linux",
    ],
    deps = [
        "//enterprise/server/registry:registry_lib",
        "//enterprise/server/remote_execution/commandutil",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:soci_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_awslabs_soci_snapshotter//soci",
    ],
)
