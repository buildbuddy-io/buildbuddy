load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "mds_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/mds",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/raft/metadata",
        "//enterprise/server/remoteauth",
        "//proto:metadata_service_go_proto",
        "//server/config",
        "//server/gossip",
        "//server/http/interceptors",
        "//server/real_environment",
        "//server/ssl",
        "//server/util/grpc_server",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/tracing",
        "//server/version",
    ],
)

go_binary(
    name = "mds",
    data = [
        "//enterprise/config:config_files",
    ],
    embed = [":mds_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
