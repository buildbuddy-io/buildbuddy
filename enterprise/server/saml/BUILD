load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "saml",
    srcs = ["saml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/saml",
    deps = [
        "//enterprise/server/auth",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/interfaces",
        "//server/nullauth",
        "//server/tables",
        "//server/util/log",
        "//server/util/status",
        "@com_github_crewjam_saml//samlsp",
    ],
)
