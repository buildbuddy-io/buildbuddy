load("@io_bazel_rules_docker//container:container.bzl", "container_push")
load("@io_bazel_rules_docker//go:image.bzl", "go_image")
load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "tput_lib",
    srcs = ["tput.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/tput",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/monitoring",
        "@com_github_docker_go_units//:go-units",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "tput",
    embed = [":tput_lib"],
    visibility = ["//visibility:public"],
)
