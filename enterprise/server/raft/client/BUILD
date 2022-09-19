load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "client",
    srcs = ["client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/filestore",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/environment",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_lni_dragonboat_v3//:dragonboat",
        "@com_github_lni_dragonboat_v3//statemachine",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)
