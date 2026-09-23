load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ext4",
    srcs = ["ext4.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4",
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/metrics",
            "//server/util/flag",
            "//server/util/log",
            "//server/util/status",
            "//server/util/tracing",
            "@com_github_prometheus_client_golang//prometheus",
            "@io_opentelemetry_go_otel//attribute",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "ext4_test",
    srcs = ["ext4_test.go"],
    exec_properties = {
        # Reproducible images need e2fsprogs 1.47.2 or later, which Ubuntu
        # 26.04 ships.
        "test.container-image": "docker://mirror.gcr.io/library/ubuntu@sha256:da6fc2be547864451aa253836dd926da33623312df4a9a243e35dc877c378a78",
    },
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            ":ext4",
            "//proto:remote_execution_go_proto",
            "//server/remote_cache/digest",
            "//server/testutil/testdigest",
            "//server/testutil/testfs",
            "//server/util/disk",
            "//server/util/testing/flags",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
