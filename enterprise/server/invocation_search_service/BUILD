load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["invocation_search_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//proto:invocation_go_proto",
        "//server/build_event_protocol/build_event_handler:go_default_library",
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/db:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/query_builder:go_default_library",
        "//server/util/status:go_default_library",
    ],
)
