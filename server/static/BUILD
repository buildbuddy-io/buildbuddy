load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "static",
    srcs = ["static.go"],
    data = [
        "//:VERSION",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/static",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:config_go_proto",
        "//server/backends/github",
        "//server/build_event_protocol/target_tracker",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/remote_cache/hit_tracker",
        "//server/util/status",
        "//server/version",
        "@com_github_golang_protobuf//jsonpb:go_default_library_gen",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
