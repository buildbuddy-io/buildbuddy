load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
