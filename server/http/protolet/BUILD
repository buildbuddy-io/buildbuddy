load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "protolet",
    srcs = ["protolet.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/http/protolet",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/proto",
        "//server/util/request_context",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)
