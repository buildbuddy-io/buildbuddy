load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sender",
    srcs = ["sender.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender",
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rangecache",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/registry",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)
