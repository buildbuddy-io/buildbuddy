load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__,//server/buildbuddy_server:__pkg__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "//server/buildbuddy_server:__pkg__",
])

go_library(
    name = "auditlog",
    srcs = ["auditlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog",
    deps = [
        "//proto:auditlog_go_proto",
        "//server/environment",
        "//server/interfaces",
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
