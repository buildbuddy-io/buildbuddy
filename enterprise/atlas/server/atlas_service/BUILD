load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "atlas_service",
    srcs = ["atlas_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/atlas_service",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/atlas/server/cluster",
        "//enterprise/atlas/server/summaries",
        "//proto:atlas_go_proto",
        "//server/util/status",
        "@io_k8s_apimachinery//pkg/api/errors",
        "@io_k8s_sigs_yaml//:yaml",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "atlas_service_test",
    srcs = ["atlas_service_test.go"],
    embed = [":atlas_service"],
    deps = [
        "//enterprise/atlas/server/cluster",
        "//enterprise/atlas/server/k8singest",
        "//enterprise/atlas/server/summaries",
        "//proto:atlas_go_proto",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@io_k8s_apimachinery//pkg/apis/meta/v1/unstructured",
        "@io_k8s_apimachinery//pkg/runtime",
        "@io_k8s_apimachinery//pkg/runtime/schema",
        "@io_k8s_client_go//dynamic/fake",
        "@org_golang_google_grpc//:grpc",
    ],
)
