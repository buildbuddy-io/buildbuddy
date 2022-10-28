load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "client",
    srcs = ["client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client",
    deps = [
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//statemachine",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//proto",
    ],
)
