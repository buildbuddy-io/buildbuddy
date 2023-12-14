load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "auditlog",
    srcs = ["auditlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:auditlog_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/clickhouse/schema",
        "//server/util/clientip",
        "//server/util/log",
        "//server/util/query_builder",
        "//server/util/random",
        "//server/util/role",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
