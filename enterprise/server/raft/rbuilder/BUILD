load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rbuilder",
    srcs = ["rbuilder.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder",
    deps = [
        "//proto:raft_go_proto",
        "//server/util/status",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//encoding/prototext",
        "@org_golang_google_protobuf//proto",
    ],
)
