load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "//tools/rbeperf:__pkg__",
])

go_library(
    name = "rbeclient",
    srcs = ["rbeclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbeclient",
    deps = [
        "//enterprise/server/remote_execution/dirtools",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
