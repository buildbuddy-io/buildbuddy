load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "explain",
    srcs = [
        "explain.go",
        "timing_profile.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain",
    deps = [
        "//cli/arg",
        "//cli/explain/compactgraph",
        "//cli/flaghistory",
        "//cli/log",
        "//cli/login",
        "//cli/util/download",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//proto:spawn_diff_go_proto",
        "//proto:spawn_go_proto",
        "//server/util/grpc_client",
        "@com_github_google_go_cmp//cmp",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "timing_profile_test",
    srcs = ["timing_profile_test.go"],
    embed = [":explain"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
    ],
)
