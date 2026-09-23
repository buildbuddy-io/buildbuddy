load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "permissions_test",
    srcs = [
        "anonymous_test.go",
        "invocations_test.go",
        "key_revocation_test.go",
        "permissions_test.go",
    ],
    deps = [
        "//enterprise/server/testutil/permissionstest",
        "//proto:acl_go_proto",
        "//proto:api_key_go_proto",
        "//proto:capability_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:invocation_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:user_go_proto",
        "//proto:user_id_go_proto",
        "//server/tables",
        "//server/util/grpc_client",
        "//server/util/perms",
        "//server/util/proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
    ],
)
