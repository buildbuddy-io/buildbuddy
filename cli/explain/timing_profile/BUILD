load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "timing_profile",
    srcs = ["timing_profile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain/timing_profile",
    deps = [
        "//cli/arg",
        "//cli/explain/timing_profile/ztracing",
        "//cli/log",
        "//cli/login",
        "//cli/terminal",
        "//cli/util/agent/claude",
        "//cli/util/download",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//server/util/grpc_client",
        "//server/util/uuid",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
