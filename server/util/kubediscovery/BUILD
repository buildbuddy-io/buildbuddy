load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "kubediscovery",
    srcs = ["kubediscovery.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/kubediscovery",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@io_k8s_api//core/v1:core",
        "@io_k8s_apimachinery//pkg/api/errors",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/watch",
        "@io_k8s_client_go//kubernetes",
        "@io_k8s_client_go//rest",
    ],
)

go_test(
    name = "kubediscovery_test",
    srcs = ["kubediscovery_test.go"],
    embed = [":kubediscovery"],
    deps = [
        "@com_github_stretchr_testify//require",
        "@io_k8s_api//apps/v1:apps",
        "@io_k8s_api//core/v1:core",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/watch",
        "@io_k8s_client_go//kubernetes/fake",
        "@io_k8s_client_go//testing",
    ],
)
