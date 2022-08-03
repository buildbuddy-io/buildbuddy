load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "build_logs_test",
    size = "small",
    srcs = ["build_logs_test.go"],
    deps = [
        "//enterprise/server/build_event_publisher",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
