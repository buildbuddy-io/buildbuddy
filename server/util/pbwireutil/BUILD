load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "pbwireutil",
    srcs = ["pbwireutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/pbwireutil",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_protobuf//encoding/protowire",
    ],
)

go_test(
    name = "pbwireutil_test",
    srcs = ["pbwireutil_test.go"],
    deps = [
        ":pbwireutil",
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
