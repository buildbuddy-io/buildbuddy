load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
        "//server/util/db",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/query_builder",
        "//server/util/random",
        "//server/util/status",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "auditlog_test",
    srcs = ["auditlog_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse",
    },
    tags = ["docker"],
    deps = [
        ":auditlog",
        "//enterprise/server/testutil/enterprise_testenv",
        "//proto:api_key_go_proto",
        "//proto:auditlog_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
