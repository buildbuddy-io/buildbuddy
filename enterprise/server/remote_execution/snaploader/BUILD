load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "snaploader",
    srcs = ["snaploader.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader",
    deps = [
        "//enterprise/server/remote_execution/blockio",
        "//enterprise/server/util/filecacheutil",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/remote_cache/digest",
        "//server/util/authutil",
        "//server/util/hash",
        "//server/util/perms",
        "//server/util/status",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "snaploader_test",
    srcs = ["snaploader_test.go"],
    deps = [
        ":snaploader",
        "//enterprise/server/remote_execution/blockio",
        "//enterprise/server/remote_execution/filecache",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/random",
        "@com_github_stretchr_testify//require",
    ],
)
