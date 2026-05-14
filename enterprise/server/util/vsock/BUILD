load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "vsock",
    srcs = ["vsock.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock",
    deps = [
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_mdlayher_vsock//:vsock",
        "@io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc//:otelgrpc",
        "@io_opentelemetry_go_otel_metric//noop",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//backoff",
        "@org_golang_x_sys//unix",
    ],
)
