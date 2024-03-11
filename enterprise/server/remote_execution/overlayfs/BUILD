load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "overlayfs",
    srcs = [
        "overlayfs.go",
        "overlayfs_linux.go",
        "overlayfs_notlinux.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/overlayfs",
    visibility = ["//visibility:public"],
    deps = select({
        "@io_bazel_rules_go//go/platform:darwin": [
            "//server/util/status",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/util/disk",
            "//server/util/status",
            "//server/util/tracing",
        ],
        "@io_bazel_rules_go//go/platform:windows": [
            "//server/util/status",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "overlayfs_test",
    srcs = ["overlayfs_test.go"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        ":overlayfs",
        "//server/testutil/testfs",
        "//server/testutil/testmount",
        "@com_github_docker_go_units//:go-units",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "overlayfs_benchmark_test",
    srcs = ["overlayfs_test.go"],
    args = [
        "-test.bench=.",
        "-test.skip=^Test",
    ],
    tags = ["manual"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        ":overlayfs",
        "//server/testutil/testfs",
        "//server/testutil/testmount",
        "@com_github_docker_go_units//:go-units",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
