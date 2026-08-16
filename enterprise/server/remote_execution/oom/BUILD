load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oom",
    srcs = ["oom.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/oom",
    deps = [
        "@org_golang_google_genproto_googleapis_rpc//errdetails",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/anypb",
    ],
)

go_test(
    name = "oom_test",
    srcs = ["oom_test.go"],
    deps = [
        ":oom",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_rpc//errdetails",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/anypb",
    ],
)
