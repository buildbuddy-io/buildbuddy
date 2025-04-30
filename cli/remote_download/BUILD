load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "remote_download",
    srcs = ["remote_download.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/remote_download",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/login",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
