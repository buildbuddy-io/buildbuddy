load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "event_parser",
    srcs = ["event_parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "//server/util/git",
        "//server/util/timeutil",
    ],
)

go_test(
    name = "event_parser_test",
    size = "small",
    srcs = ["event_parser_test.go"],
    deps = [
        ":event_parser",
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_stretchr_testify//assert",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
