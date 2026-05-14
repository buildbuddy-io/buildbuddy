load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "selfauth",
    srcs = ["selfauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/selfauth",
    deps = [
        "//enterprise/server/oidcissuer",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/http/interceptors",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@org_golang_x_oauth2//:oauth2",
    ],
)
