load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "invocation",
    srcs = ["invocation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/invocation",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

package(default_visibility = ["//experimental:__subpackages__"])
