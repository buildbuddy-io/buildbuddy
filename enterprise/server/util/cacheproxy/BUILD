load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cacheproxy",
    srcs = ["cacheproxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cacheproxy",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/prefix",
        "//server/util/status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "cacheproxy_test",
    srcs = ["cacheproxy_test.go"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        ":cacheproxy",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/app",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/prefix",
        "//server/util/testing/flags",
    ],
)
