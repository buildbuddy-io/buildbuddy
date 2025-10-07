load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "record",
    srcs = ["record.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/record",
    deps = [
        "//cli/log",
        "//cli/login",
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:command_line_go_proto",
        "//server/build_event_publisher",
        "//server/util/status",
        "//server/util/uuid",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
