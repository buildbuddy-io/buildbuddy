load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "fcbench",
    embed = [":fcbench_lib"],
    data = [
        "//enterprise/vmsupport/bin:initrd.cpio",
        "//enterprise/vmsupport/bin:vmlinux",
        "//enterprise/vmsupport/bin:vmlinux_6.1",
    ],
)

go_library(
    name = "fcbench_lib",
    srcs = ["fcbench.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/fcbench",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/firecracker",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/vbd",
        "//enterprise/server/util/cpuset",
        "//enterprise/server/util/oci",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/backends/disk_cache",
        "//server/cache/dirtools",
        "//server/environment",
        "//server/interfaces",
        "//server/nullauth",
        "//server/real_environment",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/resources",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/networking",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_common//expfmt",
        "@io_opentelemetry_go_otel//:otel",
        "@io_opentelemetry_go_otel_sdk//trace",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)
