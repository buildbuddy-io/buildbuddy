load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocifetch",
    srcs = [
        "image.go",
        "ocifetch.go",
        "registry.go",
        "remote.go",
        "store.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetch",
    deps = [
        "//enterprise/server/oci/ocicache",
        "//enterprise/server/util/ocimanifest",
        "//proto:oci_fetcher_go_proto",
        "//proto:ociregistry_go_proto",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/http/httpclient",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/bytebufferpool",
        "//server/util/flag",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/partial",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@com_github_google_go_containerregistry//pkg/v1/types",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_protobuf//proto",
    ],
)
