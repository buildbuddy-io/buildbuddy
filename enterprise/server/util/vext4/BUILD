load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vext4",
    srcs = [
        "encode.go",
        "layout.go",
        "store.go",
        "tree.go",
        "vext4.go",
        "write.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vext4",
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:android": [
            "//proto:remote_execution_go_proto",
            "//server/cache/dirtools",
            "//server/remote_cache/digest",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            "//proto:remote_execution_go_proto",
            "//server/cache/dirtools",
            "//server/remote_cache/digest",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "vext4_test",
    srcs = ["vext4_test.go"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = select({
        "@io_bazel_rules_go//go/platform:android": [
            ":vext4",
            "//enterprise/server/util/ext4",
            "//proto:remote_execution_go_proto",
            "//server/remote_cache/digest",
            "//server/testutil/testfs",
            "@com_github_stretchr_testify//require",
        ],
        "@io_bazel_rules_go//go/platform:linux": [
            ":vext4",
            "//enterprise/server/util/ext4",
            "//proto:remote_execution_go_proto",
            "//server/remote_cache/digest",
            "//server/testutil/testfs",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
