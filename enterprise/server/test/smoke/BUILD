load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "smoke_test",
    size = "medium",
    srcs = ["smoke_test.go"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testexecutor",
        "//proto:api_key_go_proto",
        "//proto:capability_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/testutil/testfs",
        "//server/util/grpc_client",
        "//server/util/rexec",
        "//server/util/uuid",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
