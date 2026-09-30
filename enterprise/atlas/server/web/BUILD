load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "web",
    srcs = ["web.go"],
    embedsrcs = ["index.html"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/web",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:atlas_go_proto",
        "//server/environment",
        "//server/http/csp",
        "//server/http/interceptors",
        "//server/http/protolet",
        "//server/util/status",
        "//server/version",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_test(
    name = "web_test",
    srcs = ["web_test.go"],
    embed = [":web"],
    deps = [
        "//enterprise/atlas/server/atlas_service",
        "//enterprise/atlas/server/cluster",
        "//enterprise/atlas/server/k8singest",
        "//enterprise/atlas/server/summaries",
        "//proto:atlas_go_proto",
        "//server/real_environment",
        "//server/util/healthcheck",
        "//server/util/proto",
        "@com_github_stretchr_testify//require",
        "@io_k8s_apimachinery//pkg/api/errors",
        "@io_k8s_apimachinery//pkg/apis/meta/v1/unstructured",
        "@io_k8s_apimachinery//pkg/runtime",
        "@io_k8s_apimachinery//pkg/runtime/schema",
        "@io_k8s_client_go//dynamic/fake",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
