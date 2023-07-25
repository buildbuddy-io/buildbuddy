load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "nbdserver",
    srcs = ["nbdserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdserver",
    deps = [
        "//enterprise/server/remote_execution/blockio",
        "//proto:nbd_go_proto",
        "//server/environment",
        "//server/util/grpc_server",
        "//server/util/status",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//proto",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "nbdserver_test",
    srcs = ["nbdserver_test.go"],
    deps = [
        ":nbdserver",
        "//enterprise/server/remote_execution/blockio",
        "//proto:nbd_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
