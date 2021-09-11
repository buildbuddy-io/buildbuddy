load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "saml",
    srcs = ["saml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/saml",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//server/environment",
        "//server/tables",
        "//server/util/log",
        "//server/util/status",
        "@com_github_crewjam_saml//samlsp",
    ],
)
