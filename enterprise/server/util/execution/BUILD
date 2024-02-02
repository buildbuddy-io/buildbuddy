load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "execution",
    srcs = ["execution.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
