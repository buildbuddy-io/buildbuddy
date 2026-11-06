load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "k8singest",
    srcs = ["k8singest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/k8singest",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/atlas/server/summaries",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/apis/meta/v1/unstructured",
        "@io_k8s_client_go//tools/cache",
    ],
)

go_test(
    name = "k8singest_test",
    srcs = ["k8singest_test.go"],
    embed = [":k8singest"],
    deps = [
        "//enterprise/atlas/server/summaries",
        "@com_github_stretchr_testify//require",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/apis/meta/v1/unstructured",
        "@io_k8s_client_go//tools/cache",
    ],
)
