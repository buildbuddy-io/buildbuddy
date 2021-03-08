load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["workflow.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/webhooks/bitbucket:go_default_library",
        "//enterprise/server/webhooks/github:go_default_library",
        "//enterprise/server/webhooks/webhook_data:go_default_library",
        "//proto:remote_execution_go_proto",
        "//proto:user_id_go_proto",
        "//proto:workflow_go_proto",
        "//server/environment:go_default_library",
        "//server/metrics:go_default_library",
        "//server/remote_cache/cachetools:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/query_builder:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_google_uuid//:go_default_library",
        "@com_github_prometheus_client_golang//prometheus:go_default_library",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
        "@io_gorm_gorm//:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["workflow_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:workflow_go_proto",
        "//server/buildbuddy_server:go_default_library",
        "//server/tables:go_default_library",
        "//server/testutil/auth:go_default_library",
        "//server/testutil/environment:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
        "@io_gorm_gorm//:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
    ],
)
