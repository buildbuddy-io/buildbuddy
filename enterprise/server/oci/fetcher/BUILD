load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "fetcher",
    srcs = ["fetcher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/fetcher",
    deps = [
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//server/http/httpclient",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@org_golang_google_grpc//:grpc",
    ],
)
