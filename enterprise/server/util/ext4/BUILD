load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ext4",
    srcs = ["ext4.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4",
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            "//server/util/log",
            "//server/util/status",
            "//server/util/tracing",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "ext4_test",
    srcs = ["ext4_test.go"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:linux": [
            ":ext4",
            "//proto:remote_execution_go_proto",
            "//server/remote_cache/digest",
            "//server/testutil/testdigest",
            "//server/testutil/testfs",
            "//server/util/disk",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
