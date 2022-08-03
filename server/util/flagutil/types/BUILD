load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "types",
    srcs = ["types.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "//server/util/flagutil/common",
        "//server/util/log",
        "//server/util/status",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "types_test",
    size = "small",
    srcs = ["types_test.go"],
    embed = [":types"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/yaml",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
