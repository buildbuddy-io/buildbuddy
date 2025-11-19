load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oci_fetch_server_proxy",
    srcs = ["oci_fetch_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/oci_fetch_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/ocirefactor/fetch",
        "//enterprise/server/util/oci",
        "//proto:ociregistry_go_proto",
        "//proto:registry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/status",
        "//third_party/singleflight",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "oci_fetch_server_proxy_test",
    size = "medium",
    srcs = ["oci_fetch_server_proxy_test.go"],
    embed = [":oci_fetch_server_proxy"],
    deps = [
        "//proto:ociregistry_go_proto",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
