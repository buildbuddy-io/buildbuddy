load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sociartifactstore",
    srcs = [
        "sociartifactstore.go",
        "sociartifactstore_darwin.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/sociartifactstore",
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//proto:soci_go_proto",
            "//server/environment",
            "//server/util/status",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//enterprise/server/util/dsingleflight",
            "//proto:registry_go_proto",
            "//proto:remote_execution_go_proto",
            "//proto:resource_go_proto",
            "//proto:soci_go_proto",
            "//server/environment",
            "//server/interfaces",
            "//server/util/log",
            "//server/util/prefix",
            "//server/util/status",
            "@com_github_awslabs_soci_snapshotter//compression",
            "@com_github_awslabs_soci_snapshotter//soci",
            "@com_github_awslabs_soci_snapshotter//ztoc",
            "@com_github_containerd_containerd//images",
            "@com_github_google_go_containerregistry//pkg/authn",
            "@com_github_google_go_containerregistry//pkg/name",
            "@com_github_google_go_containerregistry//pkg/v1:pkg",
            "@com_github_google_go_containerregistry//pkg/v1/match",
            "@com_github_google_go_containerregistry//pkg/v1/partial",
            "@com_github_google_go_containerregistry//pkg/v1/remote",
            "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
            "@com_github_google_go_containerregistry//pkg/v1/types",
            "@com_github_opencontainers_go_digest//:go-digest",
            "@com_github_opencontainers_image_spec//specs-go/v1:specs-go",
            "@org_golang_google_protobuf//proto",
            "@org_golang_x_sync//errgroup",
        ],
        "//conditions:default": [],
    }),
)
