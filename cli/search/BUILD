load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "search",
    srcs = ["search.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/search",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//proto:buildbuddy_service_go_proto",
        "//proto:search_go_proto",
        "//server/util/grpc_client",
        "@com_github_logrusorgru_aurora//:aurora",
        "@org_golang_google_grpc//metadata",
    ],
)
