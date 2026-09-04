load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "rpcutil",
    srcs = ["rpcutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/rpcutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "//server/util/flag",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/vtprotocodec",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel_exporters_prometheus//:prometheus",
        "@io_opentelemetry_go_otel_metric//:metric",
        "@io_opentelemetry_go_otel_metric//noop",
        "@io_opentelemetry_go_otel_sdk_metric//:metric",
        "@org_golang_google_grpc//experimental",
        "@org_golang_google_grpc//mem",
    ],
)

go_test(
    name = "rpcutil_test",
    srcs = ["rpcutil_test.go"],
    deps = [
        ":rpcutil",
        "//server/util/proto",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_model//io/prometheus/client:go",
        "@com_github_stretchr_testify//require",
        "@io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc//:otelgrpc",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
        "@org_golang_google_grpc//health",
        "@org_golang_google_grpc//health/grpc_health_v1",
        "@org_golang_google_grpc//test/bufconn",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_uber_go_goleak//:goleak",
    ],
)
