load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gcsflagsync",
    srcs = ["gcsflagsync.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments/gcsflagsync",
    deps = [
        "//server/backends/blobstore/gcs",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_open_feature_flagd_core//pkg/sync",
    ],
)

go_test(
    name = "gcsflagsync_test",
    srcs = ["gcsflagsync_test.go"],
    embed = [":gcsflagsync"],
    deps = [
        "//server/interfaces",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_open_feature_flagd_core//pkg/sync",
        "@com_github_stretchr_testify//require",
    ],
)
