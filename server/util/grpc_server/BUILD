load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "grpc_server",
    srcs = ["grpc_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpc_server",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/rpc/interceptors",
        "//server/util/alert",
        "//server/util/clientip",
        "//server/util/grpc_forward",
        "//server/util/log",
        "//server/util/rpcutil",
        "//server/util/status",
        "@com_github_grpc_ecosystem_go_grpc_middleware_providers_prometheus//:prometheus",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc//:otelgrpc",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//encoding/gzip",
        "@org_golang_google_grpc//experimental",
        "@org_golang_google_grpc//health/grpc_health_v1",
        "@org_golang_google_grpc//keepalive",
        "@org_golang_google_grpc//mem",
        "@org_golang_google_grpc//reflection",
        "@org_golang_google_grpc//stats",
    ],
)

go_test(
    name = "grpc_server_test",
    srcs = ["grpc_server_test.go"],
    deps = [
        ":grpc_server",
        "//proto:ping_service_go_proto",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//credentials/insecure",
        "@org_golang_google_grpc//status",
        "@org_golang_x_exp//trace",
    ],
)
