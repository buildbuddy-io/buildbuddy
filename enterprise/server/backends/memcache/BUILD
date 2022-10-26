load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "memcache",
    srcs = ["memcache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/memcache",
    deps = [
        "//enterprise/server/composable_cache",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/flagutil",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_bradfitz_gomemcache//memcache",
        "@org_golang_x_sync//errgroup",
    ],
)
