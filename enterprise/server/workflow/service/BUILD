load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "service",
    srcs = ["service.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/webhooks/bitbucket",
        "//enterprise/server/webhooks/github",
        "//enterprise/server/webhooks/webhook_data",
        "//proto:remote_execution_go_proto",
        "//proto:user_id_go_proto",
        "//proto:workflow_go_proto",
        "//server/environment",
        "//server/remote_cache/cachetools",
        "//server/tables",
        "//server/util/perms",
        "//server/util/prefix",
        "//server/util/query_builder",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
        "@io_gorm_gorm//:gorm",
    ],
)

go_test(
    name = "service_test",
    srcs = ["service_test.go"],
    deps = [
        ":service",
        "//proto:buildbuddy_service_go_proto",
        "//proto:workflow_go_proto",
        "//server/buildbuddy_server",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@io_gorm_gorm//:gorm",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
    ],
)
