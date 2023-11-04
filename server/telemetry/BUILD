load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "telemetry",
    srcs = [
        "telemetry_client.go",
        "telemetry_enterprise.go",
        "telemetry_foss.go",  #keep
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/telemetry",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:telemetry_go_proto",
        "//server/backends/blobstore",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/version",
        "@com_github_google_uuid//:uuid",
    ] + select({
        "//platforms/configs:buildbuddy_foss": [],
        "//conditions:default": [
            "//enterprise/server/remote_execution/config",
        ],
    }),  #keep
)
