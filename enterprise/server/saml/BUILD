load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "saml",
    srcs = ["saml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/saml",
    deps = [
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/cookie",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_crewjam_saml//:saml",
        "@com_github_crewjam_saml//samlsp",
        "@com_github_golang_jwt_jwt_v4//:jwt",
    ],
)
