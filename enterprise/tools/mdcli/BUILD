load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "mdcli_lib",
    srcs = ["mdcli.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/mdcli",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/clientidentity",
        "//enterprise/server/filestore",
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//proto:storage_go_proto",
        "//server/config",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)

go_binary(
    name = "mdcli",
    embed = [":mdcli_lib"],
    visibility = ["//visibility:public"],
)
