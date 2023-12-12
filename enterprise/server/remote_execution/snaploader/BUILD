load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "snaploader",
    srcs = ["snaploader.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/snaputil",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/authutil",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "snaploader_test",
    srcs = ["snaploader_test.go"],
    deps = [
        ":snaploader",
        "//enterprise/server/remote_execution/copy_on_write",
        "//enterprise/server/remote_execution/filecache",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/resources",
        "//server/testutil/testauth",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/random",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
