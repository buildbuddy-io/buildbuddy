load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "bes_sidecar_lib",
    srcs = ["bes_sidecar.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cmd/bes_sidecar",
    deps = [
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//proto:sidecar_go_proto",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials",
        "@org_golang_google_grpc//credentials/insecure",
        "@org_golang_google_grpc//keepalive",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)

go_binary(
    name = "bes_sidecar",
    embed = [":bes_sidecar_lib"],
)
