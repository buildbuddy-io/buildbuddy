load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "upload_local_snapshot",
    embed = [":upload_local_snapshot_lib"],
    pure = "on",
    static = "on",
    target_compatible_with = ["@platforms//os:linux"],
)

go_library(
    name = "upload_local_snapshot_lib",
    srcs = ["upload_local_snapshot.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/upload_local_snapshot",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/filecache",
        "//enterprise/server/remote_execution/snaploader",
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/nullauth",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/claims",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_x_sync//errgroup",
    ],
)
