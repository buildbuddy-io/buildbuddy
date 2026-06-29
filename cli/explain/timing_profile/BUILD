load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "timing_profile",
    srcs = [
        "timing_profile.go",
        "timing_profile_parser.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/explain/timing_profile",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/parser",
        "//cli/util/download",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//server/util/grpc_client",
        "//server/util/trace_events",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "timing_profile_test",
    srcs = ["timing_profile_parser_test.go"],
    embed = [":timing_profile"],
    deps = [
        "//server/util/trace_events",
        "@com_github_stretchr_testify//require",
    ],
)
