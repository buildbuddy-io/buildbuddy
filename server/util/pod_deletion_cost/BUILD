load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "pod_deletion_cost",
    srcs = ["pod_deletion_cost.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/pod_deletion_cost",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/types",
        "@io_k8s_client_go//kubernetes",
        "@io_k8s_client_go//rest",
    ],
)

go_test(
    name = "pod_deletion_cost_test",
    srcs = ["pod_deletion_cost_test.go"],
    embed = [":pod_deletion_cost"],
    deps = [
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@io_k8s_api//core/v1:core",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_client_go//kubernetes",
        "@io_k8s_client_go//kubernetes/fake",
        "@io_k8s_client_go//testing",
    ],
)
