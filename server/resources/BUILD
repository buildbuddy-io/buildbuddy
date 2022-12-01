load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "resources",
    srcs = ["resources.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/resources",
    visibility = [
        "//enterprise:__subpackages__",
    ],
    deps = [
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
        "@com_github_elastic_gosigar//:gosigar",
        "@com_google_cloud_go_compute_metadata//:metadata",
    ],
)
