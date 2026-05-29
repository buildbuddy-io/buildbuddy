load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "castop_lib",
    srcs = ["castop.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/castop",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:cache_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_go_proto",
        "//proto:pagination_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/paging",
        "@com_github_charmbracelet_bubbletea//:bubbletea",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/fieldmaskpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_term//:term",
    ],
)

go_binary(
    name = "castop",
    embed = [":castop_lib"],
    visibility = ["//visibility:public"],
)
