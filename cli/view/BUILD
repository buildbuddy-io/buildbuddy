load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "view",
    srcs = [
        "filtered_test_output.go",
        "view.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/view",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/util/download",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:target_go_proto",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "view_test",
    srcs = ["filtered_test_output_test.go"],
    deps = [
        ":view",
        "//cli/util/download/downloadtest",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:target_go_proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
    ],
)
