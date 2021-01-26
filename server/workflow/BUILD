load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["workflow.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/workflow",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:target_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/query_builder:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_google_uuid//:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["workflow_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:target_go_proto",
        "//server/buildbuddy_server:go_default_library",
        "//server/testutil/environment:go_default_library",
        "//server/testutil/auth:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
    ],
)
