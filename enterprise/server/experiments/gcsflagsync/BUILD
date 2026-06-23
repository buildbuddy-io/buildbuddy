load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
