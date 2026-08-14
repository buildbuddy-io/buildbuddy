load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "error_tracking",
    srcs = ["error_tracking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/error_tracking",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:error_tracking_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_zeebo_blake3//:blake3",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)

go_test(
    name = "error_tracking_test",
    srcs = [
        "error_tracking_test.go",
        "test_fingerprint_contract_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":error_tracking"],
    embedsrcs = ["testdata/test_fingerprint_contract.json"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:error_tracking_go_proto",
        "//proto:failure_details_go_proto",
        "//server/util/clickhouse/schema",
        "@com_github_stretchr_testify//require",
    ],
)
