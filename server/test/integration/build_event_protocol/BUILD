load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "build_event_protocol_test",
    srcs = ["build_event_protocol_test.go"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/testutil/app",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/util/status",
        "@com_github_golang_protobuf//jsonpb:go_default_library_gen",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_golang_protobuf//ptypes/empty:go_default_library",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//keepalive",
        "@org_golang_google_grpc//metadata",
    ],
)
