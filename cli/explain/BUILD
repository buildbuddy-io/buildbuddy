load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "explain",
    srcs = ["explain.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain",
    deps = [
        "//cli/arg",
        "//cli/explain/compactgraph",
        "//cli/log",
        "//cli/login",
        "//cli/storage",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//proto:spawn_diff_go_proto",
        "//proto:spawn_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "@com_github_google_go_cmp//cmp",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)
