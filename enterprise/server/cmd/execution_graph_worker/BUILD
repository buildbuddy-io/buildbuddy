load("@io_bazel_rules_docker//container:container.bzl", "container_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_graph_worker_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/execution_graph_worker",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/execution_graph_worker",
        "//server/backends/blobstore",
        "//server/config",
        "//server/real_environment",
        "//server/remote_cache/byte_stream_client",
        "//server/util/clickhouse",
        "//server/util/healthcheck",
        "//server/util/kuberesolver",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
    ],
)

go_binary(
    name = "execution_graph_worker",
    data = [
        "//enterprise:licenses",
        "//enterprise/config:config_files",
    ],
    embed = [":execution_graph_worker_lib"],
    visibility = ["//visibility:public"],
)

container_image(
    name = "base_image",
    base = "@buildbuddy_go_image_base//image",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

pkg_tar(
    name = "tar",
    srcs = [":execution_graph_worker"],
)

oci_image(
    name = "oci_image",
    base = "@buildbuddy_go_oci_image_base",
    entrypoint = ["/execution_graph_worker"],
    target_compatible_with = ["@platforms//os:linux"],
    tars = [":tar"],
    visibility = ["//visibility:public"],
)
