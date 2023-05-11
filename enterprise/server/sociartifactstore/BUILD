load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
            "//server/util/random",
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

go_test(
    name = "sociartifactstore_test",
    srcs = ["sociartifactstore_test.go"],
    data = [":test_data"],
    embed = [":sociartifactstore"],
    deps = [
        "//enterprise/server/testutil/testredis",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:soci_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/crane",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/registry",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/empty",
        "@com_github_google_go_containerregistry//pkg/v1/mutate",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/stream",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)

filegroup(
    name = "test_data",
    srcs = [
        "test_data/layers/59fe65d9e601b6db7125a2259b7d64064db081ac6ed94ef24cc961f4438d1008.tar.gz",
        "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz",
        "test_data/layers/ddd3b7c66a0e0090df72a082ae683a96d8ea6d96a689d3bec894feec40880ec5.tar.gz",
        "test_data/soci_indexes/7579d04981896723ddd70ed633e9a801e869bd3d954251216adf3feef092c5ea.json",
        "test_data/ztocs/85e0877f6edf3eed5ea44c29b8c7adf7d2fa58a2d088b39593376c438dc311a2.ztoc",
        "test_data/ztocs/ffc7a206c8fc2f5239e3e7281e2d2c1f40af93d605c6a353a3146e577fb0e90c.ztoc",
    ],
)
