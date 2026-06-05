load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "operation",
    srcs = ["operation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/retry",
        "//server/util/status",
        "@com_google_cloud_go_longrunning//autogen/longrunningpb",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "operation_test",
    srcs = ["operation_test.go"],
    embed = [":operation"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@com_google_cloud_go_longrunning//autogen/longrunningpb",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
