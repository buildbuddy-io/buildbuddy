load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["digest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/status:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_google_uuid//:go_default_library",
        "@go_googleapis//google/rpc:errdetails_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["digest_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/status:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)
