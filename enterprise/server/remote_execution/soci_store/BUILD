load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "soci_store",
    srcs = [
        "soci_store.go",
        "soci_store_darwin.go",
        "soci_store_linux.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/soci_store",
    deps = [
        "//enterprise/server/util/oci",
        "//server/environment",
    ] + select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//proto:registry_go_proto",
            "//proto:remote_execution_go_proto",
            "//proto:resource_go_proto",
            "//proto:soci_go_proto",
            "//server/interfaces",
            "//server/metrics",
            "//server/remote_cache/cachetools",
            "//server/remote_cache/digest",
            "//server/util/disk",
            "//server/util/grpc_client",
            "//server/util/log",
            "//server/util/prefix",
            "//server/util/status",
            "@com_github_awslabs_soci_snapshotter//proto",
            "@com_github_opencontainers_go_digest//:go-digest",
            "@com_github_prometheus_client_golang//prometheus",
        ],
        "//conditions:default": [],
    }),
)
