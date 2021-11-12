load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "build_event_protocol_test",
    srcs = ["build_event_protocol_test.go"],
    shard_count = 5,
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:invocation_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//jsonpb:go_default_library_gen",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//proto/wkt:empty_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
