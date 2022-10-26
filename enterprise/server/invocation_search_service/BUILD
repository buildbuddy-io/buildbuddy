load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "invocation_search_service",
    srcs = ["invocation_search_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service",
    visibility = [
        "//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:invocation_go_proto",
        "//server/build_event_protocol/build_event_handler",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/alert",
        "//server/util/blocklist",
        "//server/util/db",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
    ],
)
