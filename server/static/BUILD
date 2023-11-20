load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "static",
    srcs = ["static.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/static",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:config_go_proto",
        "//server/backends/github",
        "//server/build_event_protocol/target_tracker",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/invocation_stat_service/config",
        "//server/remote_cache/hit_tracker",
        "//server/remote_execution/config",
        "//server/scheduling/scheduler_server/config",
        "//server/util/flag",
        "//server/util/region",
        "//server/util/status",
        "//server/util/subdomain",
        "//server/version",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
