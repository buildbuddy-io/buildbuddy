load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "hit_tracker_client",
    srcs = ["hit_tracker_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_client",
    deps = [
        "//proto:hit_tracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

go_test(
    name = "hit_tracker_client_test",
    srcs = ["hit_tracker_client_test.go"],
    embed = [":hit_tracker_client"],
    deps = [
        "//proto:hit_tracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testenv",
        "//server/util/log",
        "@com_github_stretchr_testify//require",
    ],
)
