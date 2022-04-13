load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "webhooks",
    srcs = ["webhooks.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/webhooks",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:invocation_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/db",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_x_oauth2//:oauth2",
        "@org_golang_x_oauth2//google",
    ],
)
