load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "watcher",
    srcs = ["watcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/dns/watcher",
    deps = [
        "//server/backends/blobstore/gcs",
        "//server/util/log",
    ],
)

go_test(
    name = "watcher_test",
    size = "small",
    srcs = ["watcher_test.go"],
    deps = [
        ":watcher",
        "//server/backends/blobstore/gcs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
