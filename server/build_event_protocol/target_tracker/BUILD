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
        "//server/util/background",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/timeutil",
        "//server/util/uuid",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "target_tracker_test",
    srcs = ["target_tracker_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04@sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        # We don't want different different db tests to be assigned to the samed
        # recycled runner, because we can't fit all db docker images with the
        # default disk limit.
        "test.runner-recycling-key": "clickhouse25.3",
    },
    tags = ["docker"],
    deps = [
        ":target_tracker",
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
