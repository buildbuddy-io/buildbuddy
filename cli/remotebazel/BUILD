load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "remotebazel",
    srcs = ["remotebazel.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/remotebazel",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/logging",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:runner_go_proto",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_go_git_go_git_v5//:go-git",
        "@com_github_go_git_go_git_v5//plumbing",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sys//unix",
    ],
)
