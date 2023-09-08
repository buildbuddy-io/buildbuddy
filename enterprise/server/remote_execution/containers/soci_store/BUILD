load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "soci_store",
    srcs = [
        "soci_store.go",
        "soci_store_darwin.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/soci_store",
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//enterprise/server/remote_execution/container",
            "//proto:soci_go_proto",
            "//server/environment",
            "//server/util/status",
            "@com_github_awslabs_soci_snapshotter//proto",
            "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/remote_execution/container",
            "//proto:remote_execution_go_proto",
            "//proto:resource_go_proto",
            "//proto:soci_go_proto",
            "//server/environment",
            "//server/metrics",
            "//server/remote_cache/cachetools",
            "//server/remote_cache/digest",
            "//server/util/disk",
            "//server/util/grpc_client",
            "//server/util/healthcheck",
            "//server/util/log",
            "//server/util/status",
            "@com_github_awslabs_soci_snapshotter//fs",
            "@com_github_awslabs_soci_snapshotter//fs/config",
            "@com_github_awslabs_soci_snapshotter//fs/layer",
            "@com_github_awslabs_soci_snapshotter//fs/source",
            "@com_github_awslabs_soci_snapshotter//metadata",
            "@com_github_awslabs_soci_snapshotter//proto",
            "@com_github_awslabs_soci_snapshotter//service/keychain/local_keychain",
            "@com_github_awslabs_soci_snapshotter//service/resolver",
            "@com_github_awslabs_soci_snapshotter//store",
            "@com_github_awslabs_soci_snapshotter//ztoc",
            "@com_github_coreos_go_systemd_v22//daemon",
            "@com_github_opencontainers_go_digest//:go-digest",
            "@io_etcd_go_bbolt//:bbolt",
            "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        ],
        "//conditions:default": [],
    }),
)
