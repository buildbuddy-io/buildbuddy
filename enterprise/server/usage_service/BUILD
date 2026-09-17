load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usage_service",
    srcs = ["usage_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service",
    deps = [
        "//enterprise/server/usage/config",
        "//proto:usage_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/usage/sku",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/log",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

# Tell gazelle to make each file a separate test target,
# so the ClickHouse-backed usage service test can keep separate runner properties.
# gazelle:go_test file

go_test(
    name = "usage_service_test",
    srcs = ["usage_service_test.go"],
    deps = [
        ":usage_service",
        "//proto:capability_go_proto",
        "//proto:context_go_proto",
        "//proto:usage_go_proto",
        "//proto:user_id_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "usage_service_olap_test",
    srcs = ["usage_service_olap_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04@sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
        "test.EstimatedComputeUnits": "4",
    },
    tags = ["docker"],
    deps = [
        ":usage_service",
        "//proto:context_go_proto",
        "//proto:usage_go_proto",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/usage/sku",
        "//server/util/clickhouse/schema",
        "//server/util/testing/flags",
        "@com_github_clickhouse_clickhouse_go_v2//lib/column/orderedmap",
        "@com_github_google_go_cmp//cmp",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
