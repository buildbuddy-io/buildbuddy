load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "smash_lib",
    srcs = ["smash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/smash",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_bojand_ghz//printer",
        "@com_github_bojand_ghz//runner",
        "@com_github_jhump_protoreflect//desc",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "smash",
    embed = [":smash_lib"],
)
