load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "flagutil",
    srcs = ["flagutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "//server/util/log",
        "//server/util/status",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "flagutil_test",
    srcs = ["flagutil_test.go"],
    embed = [":flagutil"],
    deps = [
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
