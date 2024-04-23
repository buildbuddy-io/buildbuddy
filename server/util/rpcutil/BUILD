load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "rpcutil",
    srcs = ["rpcutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/rpcutil",
    visibility = ["//visibility:public"],
    deps = ["//server/util/proto"],
)

go_test(
    name = "rpcutil_test",
    srcs = ["rpcutil_test.go"],
    deps = [
        ":rpcutil",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
