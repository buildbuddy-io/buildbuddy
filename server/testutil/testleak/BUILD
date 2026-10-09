load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "testleak",
    testonly = 1,
    srcs = ["testleak.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testleak",
    visibility = ["//visibility:public"],
    deps = ["@org_uber_go_goleak//:goleak"],
)

go_test(
    name = "testleak_test",
    size = "small",
    srcs = ["testleak_test.go"],
    embed = [":testleak"],
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sys//unix",
    ],
)
