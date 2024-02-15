load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gcplink",
    srcs = ["gcplink.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/gcplink",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/keystore",
        "//proto:context_go_proto",
        "//proto:gcp_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:secrets_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/real_environment",
        "//server/util/cookie",
        "//server/util/random",
        "//server/util/request_context",
        "//server/util/status",
        "@com_github_coreos_go_oidc//:go-oidc",
        "@org_golang_x_oauth2//:oauth2",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
