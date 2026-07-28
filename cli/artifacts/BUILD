load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "artifacts",
    srcs = ["artifacts.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/artifacts",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/util/download",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:common_go_proto",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
