load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "pebbleutil",
    srcs = ["pebbleutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebbleutil",
    deps = [
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "pebbleutil_test",
    srcs = ["pebbleutil_test.go"],
    deps = [
        ":pebbleutil",
        "//server/testutil/testfs",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
