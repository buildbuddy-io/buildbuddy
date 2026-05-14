load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "xds",
    srcs = ["xds.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/xds",
    visibility = ["//visibility:public"],
    deps = [
        "//server/resources",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_client_go//kubernetes",
        "@io_k8s_client_go//rest",
    ],
)

go_test(
    name = "xds_test",
    srcs = ["xds_test.go"],
    deps = [
        ":xds",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@io_k8s_api//core/v1:core",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_client_go//kubernetes/fake",
    ],
)
