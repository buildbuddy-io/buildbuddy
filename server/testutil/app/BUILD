load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "app",
    testonly = 1,
    srcs = ["app.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/app",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/testutil/testserver",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
