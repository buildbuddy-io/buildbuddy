load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "target_tracker",
    srcs = ["target_tracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/build_event_protocol/accumulator",
        "//server/environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/timeutil",
        "//server/util/uuid",
        "@org_golang_google_protobuf//encoding/prototext",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "target_tracker_test",
    size = "small",
    srcs = ["target_tracker_test.go"],
    deps = [
        ":target_tracker",
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
