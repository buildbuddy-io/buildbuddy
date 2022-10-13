load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "filters",
    srcs = ["filters.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/rpc/filters",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/metrics",
        "//server/role_filter",
        "//server/util/bazel_request",
        "//server/util/log",
        "//server/util/quota",
        "//server/util/request_context",
        "//server/util/uuid",
        "@com_github_grpc_ecosystem_go_grpc_prometheus//:go-grpc-prometheus",
        "@io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc//:otelgrpc",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "filters_test",
    size = "small",
    srcs = ["filters_test.go"],
    deps = [
        "//proto:ping_service_go_proto",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/log",
        "//server/util/random",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
