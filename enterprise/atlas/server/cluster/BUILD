load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cluster",
    srcs = [
        "cluster.go",
        "logs.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/cluster",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/atlas/server/k8singest",
        "//enterprise/atlas/server/summaries",
        "//server/util/flag",
        "//server/util/log",
        "@io_k8s_apimachinery//pkg/api/errors",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/apis/meta/v1/unstructured",
        "@io_k8s_apimachinery//pkg/runtime",
        "@io_k8s_apimachinery//pkg/runtime/schema",
        "@io_k8s_client_go//discovery",
        "@io_k8s_client_go//dynamic",
        "@io_k8s_client_go//metadata",
        "@io_k8s_client_go//rest",
        "@io_k8s_client_go//tools/cache",
        "@io_k8s_client_go//tools/clientcmd",
    ],
)

go_test(
    name = "cluster_test",
    srcs = [
        "cluster_test.go",
        "logs_test.go",
    ],
    embed = [":cluster"],
    deps = [
        "//enterprise/atlas/server/summaries",
        "@com_github_stretchr_testify//require",
        "@io_k8s_apimachinery//pkg/api/errors",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:meta",
        "@io_k8s_apimachinery//pkg/apis/meta/v1/unstructured",
        "@io_k8s_apimachinery//pkg/fields",
        "@io_k8s_apimachinery//pkg/runtime",
        "@io_k8s_apimachinery//pkg/runtime/schema",
        "@io_k8s_client_go//discovery/fake",
        "@io_k8s_client_go//dynamic/fake",
        "@io_k8s_client_go//metadata/fake",
        "@io_k8s_client_go//rest",
        "@io_k8s_client_go//testing",
    ],
)
